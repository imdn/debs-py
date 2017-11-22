"""
Global variables to share across files
"""

# Required parameters W, N and M
WINDOW_SIZE=10
NUM_STATE_TRANSITIONS = 5
KMEANS_MAX_ITERATIONS = 1000


# Machines in incoming stream
machines = dict()

# Machines Models in incoming stream
machine_models = dict()

# Keep track of events (observation groups) as they come in
event_map = dict()

# Filter list of properties to cluster. Useful for debugging
#properties_to_filter = None
properties_to_filter = ['_59_31']
