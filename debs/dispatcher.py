import numpy as np
import sys
import logging
from . import globals
from . import kmeans
from collections import defaultdict

#log_level=logging.DEBUG
log_level=logging.WARNING
logging.basicConfig(filename='dispatch.log', filemode='w', level=log_level, format='Dispatcher.py: %(message)s')

# Print Info messages to the console
consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.INFO)
logging.getLogger().addHandler(consoleHandler)


class Dispatcher(object):
    """ Process stream data for an event """
    
    def __init__(self):
        self.machine_map = defaultdict(list)
        # Keep track of prev cluster centroids/machine and outgoing value from current window
        self.prev_clustering_result = defaultdict(dict)
        self.processed_events = defaultdict(list)

    def process_event(self, machine_id, obs_group_id, metadata, force_run=False):
        if len(self.machine_map[machine_id]) == globals.WINDOW_SIZE or force_run:
            # Window size for machine is full. Onward to clustering
            logging.info (f"Processing Machine with ID: {machine_id}; Observation Group: {obs_group_id}")
            self.process_machine_stream(machine_id, obs_group_id, metadata)
            self.processed_events[machine_id].append(obs_group_id)
            # After run is over, remove the first observation group for the particular machine
            logging.debug (f"Removing observation group from window: {self.machine_map[machine_id][0]}")
            del self.machine_map[machine_id][0]
            logging.info (f"Finished processing Machine with ID: {machine_id}; Observation Group: {obs_group_id}")
        self.machine_map[machine_id].append(obs_group_id)

    
    def process_machine_stream(self, machine_id, obs_group_id, metadata):
        """Run the computation pipeline for a given machine for each stateful dimension """
        
        data = []
        for oid in self.machine_map[machine_id]:
            data.extend(globals.event_map[oid].get_observations())
        adata = np.array(data)

        # Get unique observed properties
        stateful_dims = np.unique(adata[:,1])
        for dim in stateful_dims:
            if len(globals.properties_to_filter) > 0 and dim not in globals.properties_to_filter:
                continue
            # Extract values for a given observed property
            observations_for_prop = adata[adata[:,1] == dim]
            observedValues = observations_for_prop[:,2].astype(float)
            num_clusters = globals.get_machine_property(machine_id, dim, 'num_clusters')
            ts_id = globals.event_map[obs_group_id].timestamp_id
            logging.debug(f"\nNow clustering observations for property {dim} with Timestamp:{ts_id}...")
            centroids, labels = self.cluster_values(machine_id, dim, observedValues, num_clusters)
            logging.debug("Now building Markov model ...")
            trans_mat =self.build_transition_probability_matrix(labels, len(centroids))
            threshold_prob = metadata[dim].prob_threshold
            logging.debug (f"Transition Matrix\n{trans_mat}")
            logging.debug("Now detecting anomalies ...")
            has_anomalies, obs_probability = self.detect_anomalies(labels, trans_mat, threshold_prob)
            if has_anomalies:
                logging.warning(f"Anomalies observed in {machine_id:12}\tProperty: {dim:7}\tTimestamp: {ts_id:16}\tP(observed): {obs_probability:22} vs. P(threshold): {threshold_prob}")

    
    def cluster_values(self, machine_id, dim, values, max_clusters):
        """ Optimize computation of cluster centers using k-means """
        
        compute_kmeans = True
        logging.debug (f"\nObserved Values - {values}")
        logging.debug (f"Max clusters allowed - {max_clusters}")

        if dim not in self.prev_clustering_result[machine_id]:
            # Seed initial values with distinct observations observation values
            # NOTE: This could be inefficient if values is a large array
            unique_values, indices = np.unique(values, return_index=True)
            limit = max_clusters if max_clusters <= len(unique_values) else len(unique_values)
            # np.unique is unordered. Use indices to get ordered unique values
            seeds = unique_values[np.argsort(indices)][0:limit]
            logging.debug (f"Initializing centroids - {seeds}")
        else:
            # Use previously computed centroids and outgoing value
            prev_centroids, prev_outgoing_val = self.prev_clustering_result[machine_id][dim]
            incoming_val = values[-1]
            if prev_outgoing_val == incoming_val or incoming_val in prev_centroids:
                # OPTIMIZATION: Skip k-means computation if:
                # 1. prev outgoing value is same as new incoming, OR
                # 2. incoming value is same as cluster centroid
                compute_kmeans = False
                centroids = prev_centroids
                logging.debug (f"Reusing centroids - {centroids}")
            elif len(prev_centroids) < max_clusters:
                # Add newly streamed value to seed if unique
                seeds = np.append(prev_centroids, incoming_val)
                logging.debug (f"Added {incoming_val} to prev centroids - {seeds}")
            else:
                seeds = prev_centroids
                logging.debug (f"Using prev centroid - {seeds}")


        if compute_kmeans:
            centroids, ignored = kmeans.cluster(values, seeds, globals.KMEANS_MAX_ITERATIONS)
            logging.debug (f"New centroids - {centroids}")

        labels = self.label_values(values, centroids)
        logging.debug (f"Labels - {labels}")

        self.prev_clustering_result[machine_id][dim] = (centroids, values[0])
        return (centroids, labels)
    
    
    def label_values(self, values, centroids):
        """ Return indices of the cluster to which the values belong """

        labels = []
        for v in values:
            distances = np.absolute(centroids - v)
            # Indices of minimum distances
            mindist_indices = np.where(distances == distances.min())[0]
            # In case of more than one centroid with same distance, choose one with bigger value
            if mindist_indices.size > 1:
                mindist_indices = np.where(centroids == centroids[mindist_indices].max())
            labels.append(mindist_indices[0])
        return labels
        

    def build_transition_probability_matrix(self, labels, size):
        """ State Transition Probability matrix for Markov Model from Cluster labels """

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

    def detect_anomalies(self, labels, trans_mat, threshold):
        """ Check if probability of observed sequence of transitions is above threshold """
        
        N = globals.NUM_STATE_TRANSITIONS
        # Max number of possible state transitions is dependent of window size
        max_possible_transitions = len(labels) - 1
        if N > max_possible_transitions:
            N = max_possible_transitions
        start = 1
        end = start + N
        prob_cur_chain = 1
        
        logging.debug (f"Threshold Prob - {threshold}")
        for i in range(start, end):
            # Compute initial state transition probability 
            cur_state = labels[i-1]
            next_state = labels[i]
            prob_cur_chain = prob_cur_chain * trans_mat[cur_state, next_state]
            logging.debug(f"State: {cur_state}->{next_state} ; Probability: {trans_mat[cur_state, next_state]}")

        logging.debug(f"Observed probability for initial sequence: {prob_cur_chain}")
        if prob_cur_chain < threshold:
            return (True, prob_cur_chain)
            
        while end <= max_possible_transitions:
            # OPTIMIZATION: Instead of sequence of multiplications, 
            # P (next_chain) = P(cur_chain) / P(outgoing_transition) * P(next_transition)
            prob_prev_transition = trans_mat[labels[start-1], labels[start]]
            prob_next_transition = trans_mat[labels[end-1], labels[end]]
            logging.debug(f"P(previous[{labels[start-1]}->{labels[start]}]): {trans_mat[labels[start-1], labels[start]]}; P(next[{labels[end-1]}->{labels[end]}]): {trans_mat[labels[end-1], labels[end]]}")
            if prob_prev_transition != prob_next_transition: # Avoid unnecessary computation
                prob_cur_chain = prob_cur_chain / prob_prev_transition * prob_next_transition
            start += 1
            end = start + N
            logging.debug(f"Observed probability for next sequence: {prob_cur_chain}")
            if prob_cur_chain < threshold:
                logging.debug(f"Anomaly detected - {labels}\nThreshold: {threshold}\tComputed:{prob_cur_chain}")
                return (True, prob_cur_chain)

        return (False, prob_cur_chain)

