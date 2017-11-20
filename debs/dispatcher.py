from . import kmeans
import numpy as np
import sys

class Dispatcher(object):
    def __init__(self, window_size, event_map):
        self.windowSize = window_size
        self.machinesMap = dict()
        self.events = event_map
        self.propClusters = dict()

    def processEvent(self, machine_id, obs_group_id, metadata):
        if machine_id not in self.machinesMap:
            self.machinesMap[machine_id] = []
        if len(self.machinesMap[machine_id]) == self.windowSize:
            # Window size for machine is full. Onward to clustering
            print ("Running clustering")
            self.runClustering(machine_id, obs_group_id, metadata)
        self.machinesMap[machine_id].append(obs_group_id)

    def runClustering(self, machine_id, obs_group_id, metadata):
        data = []
        for oid in self.machinesMap[machine_id]:
            data.extend(self.events[oid].getObservations())
        adata = np.array(data)
        np.save('data.npy', adata)

        # Get unique observed properties
        statefulDims = np.unique(adata[:,1])
        for dim in statefulDims:
            # Extract values for a given observed property
            observationsForProp = adata[adata[:,1] == dim]
            observedValues = observationsForProp[:,2].astype(float)
            numClusters = metadata[dim].numClusters
            if machine_id not in self.propClusters:
                self.propClusters[machine_id] = dict()
            if dim not in self.propClusters[machine_id]:
                # Seed initial values
                unique_values, indices = np.unique(observedValues, return_index=True)
                limit = numClusters if numClusters <= len(unique_values) else len(unique_values)
                # np.unique is unordered. Use indices to get ordered unique values
                seeds = unique_values[np.argsort(indices)][0:limit]
            else:
                # Use previously computed centroids
                # First check if 
                if len(self.propClusters[machine_id][dim]) < numClusters:
                    # Add newly streamed value to seed if unique
                    seeds = np.append(self.propClusters[machine_id][dim], observedValues[-1])
                else:
                    seeds = self.propClusters[machine_id][dim]
            centroids, ignored = kmeans.cluster(observedValues, seeds, 1000)
            labels = self.assignValuesToCluster(observedValues, centroids)
            print (f"\n{observedValues}\nSeeds - {seeds}\nCentroids - {centroids}\nLabels - {labels}")
            mat =self.buildTransitionProbabilityMatrix(labels)
            print (mat)
            self.propClusters[machine_id][dim] = centroids
        sys.exit(0)

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
                           
    
    def print_info(self):
        for m in self.machinesMap:
            groupQueue = self.machinesMap[m]
            while not len(groupQueue) == 0:
                gid = groupQueue.pop(0)
                print (f"Machine {m} - {gid}")
