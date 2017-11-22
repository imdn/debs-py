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

    def processEvent(self, machine_id, obs_group_id, metadata):
        if machine_id not in self.machinesMap:
            self.machinesMap[machine_id] = []
        if len(self.machinesMap[machine_id]) == self.WINDOW_SIZE:
            # Window size for machine is full. Onward to clustering
            print ("Running clustering")
            self.processMachineStream(machine_id, obs_group_id, metadata)
        self.machinesMap[machine_id].append(obs_group_id)

    def processMachineStream(self, machine_id, obs_group_id, metadata):
        data = []
        for oid in self.machinesMap[machine_id]:
            data.extend(self.events[oid].getObservations())
            adata = np.array(data)
            #np.save('data.npy', adata)
            # Get unique observed properties
            statefulDims = np.unique(adata[:,1])
            for dim in statefulDims:
                # Extract values for a given observed property
                observationsForProp = adata[adata[:,1] == dim]
                observedValues = observationsForProp[:,2].astype(float)
                numClusters = metadata[dim].numClusters
                centroids, labels = self.clusterValues(machine_id, dim, observedValues, numClusters)
                mat =self.buildTransitionProbabilityMatrix(labels)
                thresholdProb = metadata[dim].probThreshold
                self.detectAnomalies(mat, thresholdProb)

    
    def clusterValues(self, machine_id, dim, values, maxClusters):
        compute_kmeans = True
        if dim not in self.prevClusteringResult[machine_id]:
            # Seed initial values with distinct observations observation values
            # NOTE: This could be inefficient if values is a large array
            unique_values, indices = np.unique(values, return_index=True)
            limit = maxClusters if maxClusters <= len(unique_values) else len(unique_values)
            # np.unique is unordered. Use indices to get ordered unique values
            seeds = unique_values[np.argsort(indices)][0:limit]
        else:
            # Use previously computed centroids and outgoing value
            prev_centroids, prev_outgoing_val = self.prevClusteringResult[machine_id][dim]
            if len(prev_centroids) < maxClusters:
                # Add newly streamed value to seed if unique
                print ("Here", values[-1])
                seeds = np.append(prev_centroids, values[-1])
                print (seeds)
            else:
                seeds = prev_centroids

            # OPTIMIZATION: If prev outgoing value is same as new incoming
            # value cluster centroids are same. In this case skip kmeans.
            if prev_outgoing_val == values[-1]:
                compute_kmeans = False
                centroids = prev_centroids

        if compute_kmeans:
            print(values, seeds)
            centroids, ignored = kmeans.cluster(values, seeds, self.KMEANS_MAX_ITERATIONS)

        labels = self.assignValuesToCluster(values, centroids)
        print (f"\n{values}\nSeeds - {seeds}\nCentroids - {centroids}\nLabels - {labels}")

        self.prevClusteringResult[machine_id][dim] = (centroids, values[0])
        return (centroids, labels)
    
    def assignValuesToCluster(self, values, centroids):
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
        

    def buildTransitionProbabilityMatrix(self, labels):
        """
        State Transition Probability matrix for Markov Model from Cluster labels
        """
        size = len(labels)
        trans_mat = np.mat(np.zeros((size, size)))

        for i in range(1, size):
            start = labels[i-1]
            end = labels[i]
            trans_mat[start, end] += 1
        
        for i in range(0, size):
            sum = trans_mat[i].sum()
            if sum > 0:
                trans_mat[i] = trans_mat[i] / sum

        return trans_mat

    def detectAnomalies(self, mat, thresholds):
        computed_prob = mat[np.nonzero(mat)].prod()
        if computed_prob > thresholds:
            pass
                           
    
    def print_info(self):
        for m in self.machinesMap:
            groupQueue = self.machinesMap[m]
            while not len(groupQueue) == 0:
                gid = groupQueue.pop(0)
                print (f"Machine {m} - {gid}")
