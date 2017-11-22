from . import kmeans
import numpy as np
import sys
from collections import defaultdict


class Dispatcher(object):
    def __init__(self, window_size, max_iter, num_transitions, event_map):
        self.WINDOW_SIZE = window_size
        self.KMEANS_MAX_ITERATIONS = max_iter
        self.NUM_STATE_TRANSITIONS = num_transitions
        self.machinesMap = dict()
        self.events = event_map
        # Keep track of prev cluster centroids/machine and outgoing value from current window
        self.prevClusteringResult = defaultdict(dict)
        #self.prevValuesForProp = defaultdict(dict) 

    def processEvent(self, machine_id, obs_group_id, metadata, force_run=False):
        if machine_id not in self.machinesMap:
            self.machinesMap[machine_id] = []
        if len(self.machinesMap[machine_id]) == self.WINDOW_SIZE or force_run:
            # Window size for machine is full. Onward to clustering
            print ("Running clustering")
            self.processMachineStream(machine_id, obs_group_id, metadata)
        self.machinesMap[machine_id].append(obs_group_id)

    def processMachineStream(self, machine_id, obs_group_id, metadata):
        data = []
        for oid in self.machinesMap[machine_id]:
            data.extend(self.events[oid].getObservations())
        adata = np.array(data)

        # Get unique observed properties
        statefulDims = np.unique(adata[:,1])
        for dim in statefulDims:
            # Extract values for a given observed property
            observationsForProp = adata[adata[:,1] == dim]
            observedValues = observationsForProp[:,2].astype(float)
            numClusters = metadata[dim].numClusters
            centroids, labels = self.clusterValues(machine_id, dim, observedValues, numClusters)
            trans_mat =self.buildTransitionProbabilityMatrix(labels, len(centroids))
            thresholdProb = metadata[dim].probThreshold
            print (f"Transition Matrix\n{trans_mat}")
            self.detectAnomalies(labels, trans_mat, thresholdProb)

    
    def clusterValues(self, machine_id, dim, values, maxClusters):
        compute_kmeans = True
        print (f"\nObserved Values - {values}")
        if dim not in self.prevClusteringResult[machine_id]:
            # Seed initial values with distinct observations observation values
            # NOTE: This could be inefficient if values is a large array
            unique_values, indices = np.unique(values, return_index=True)
            limit = maxClusters if maxClusters <= len(unique_values) else len(unique_values)
            # np.unique is unordered. Use indices to get ordered unique values
            seeds = unique_values[np.argsort(indices)][0:limit]
            print (f"Initializing centroids - {seeds}")
        else:
            # Use previously computed centroids and outgoing value
            prev_centroids, prev_outgoing_val = self.prevClusteringResult[machine_id][dim]
            incoming_val = values[-1]
            if prev_outgoing_val == incoming_val or incoming_val in prev_centroids:
                # OPTIMIZATION: Skip k-means computation if:
                # 1. prev outgoing value is same as new incoming, OR
                # 2. incoming value is same as cluster centroid
                compute_kmeans = False
                centroids = prev_centroids
                print (f"Reusing centroids - {centroids}")
            elif len(prev_centroids) < maxClusters:
                # Add newly streamed value to seed if unique
                seeds = np.append(prev_centroids, incoming_val)
                print (f"Added {incoming_val} to prev centroids - {seeds}")
            else:
                seeds = prev_centroids
                print (f"Using prev centroid - {seeds}")

        if compute_kmeans:
            centroids, ignored = kmeans.cluster(values, seeds, self.KMEANS_MAX_ITERATIONS)
            print (f"New centroids - {centroids}")

        labels = self.labelValues(values, centroids)
        print (f"Labels - {labels}")

        self.prevClusteringResult[machine_id][dim] = (centroids, values[0])
        return (centroids, labels)
    
    
    def labelValues(self, values, centroids):
        """
        Take values and return indices of the cluster they belong to
        """
        labels = []
        for v in values:
            distances = np.absolute(centroids - v)
            # Indices of minimum distances
            mindist_indices = np.where(distances == distances.min())
            # In case of more than one centroid with same distance, choose one with bigger value
            if len(mindist_indices) > 1:
                mindist_indices = np.where(centroids == centroids[mindist_indices].max())
            labels.append(mindist_indices)
        return np.array(labels).flatten()
        

    def buildTransitionProbabilityMatrix(self, labels, size):
        """
        State Transition Probability matrix for Markov Model from Cluster labels
        """
        if size == 1:
            trans_mat = np.mat([1])
        else:
            trans_mat = np.mat(np.zeros((size, size)))
            for i in range(1, len(labels)):
                start = labels[i-1]
                end = labels[i]
                trans_mat[start, end] += 1

            for i in range(0, size):
                sum = trans_mat[i].sum()
                if sum > 0:
                    trans_mat[i] = trans_mat[i] / sum

        return trans_mat

    def detectAnomalies(self, labels, trans_mat, threshold):
        N = self.NUM_STATE_TRANSITIONS
        # Max number of possible state transitions is dependent of window size
        max_possible_transitions = len(labels) - 1
        if N > max_possible_transitions:
            N = max_possible_transitions
        start = 1
        end = start + N
        prob_cur_chain = 1
        
        print (f"Threshold Prob - {threshold}")
        for i in range(start, end):
            # Compute initial state transition probability 
            cur_state = labels[i-1]
            next_state = labels[i]
            prob_cur_chain = prob_cur_chain * trans_mat[cur_state, next_state]

        print (f"Prob of Sequence - {prob_cur_chain}")
        if prob_cur_chain < threshold:
            print (f"Anomaly detected - {labels}\nThreshold: {threshold}\tComputed:{prob_cur_chain}")
            sys.exit(0)
            
        while end <= max_possible_transitions:
            # OPTIMIZATION: Instead of sequence of multiplications, 
            # P (next_chain) = P(cur_chain) / P(outgoing_transition) * P(next_transition)
            prob_prev_transition = trans_mat[labels[start-1], labels[start]]
            prob_next_transition = trans_mat[labels[end-1], labels[end]]
            prob_cur_chain = prob_cur_chain / prob_prev_transition * prob_next_transition
            start += 1
            end = start + N
            print (f"Prob of Sequence - {prob_cur_chain}")
            if prob_cur_chain < threshold:
                print (f"Anomaly detected - {labels}\nThreshold: {threshold}\tComputed:{prob_cur_chain}")
                sys.exit(0)
            

    
    
    def print_info(self):
        for m in self.machinesMap:
            groupQueue = self.machinesMap[m]
            while not len(groupQueue) == 0:
                gid = groupQueue.pop(0)
                print (f"Machine {m} - {gid}")
