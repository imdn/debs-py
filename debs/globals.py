"""
Global variables to share across files
"""
import pika
from collections import OrderedDict
from . import dispatcher

# Required parameters W, N and M
WINDOW_SIZE=10
NUM_STATE_TRANSITIONS = 5
KMEANS_MAX_ITERATIONS = 1000

# Store Machine metadata from incoming stream
machines = dict()

# Store machine model metadata from incoming stream
machine_models = dict()

# Keep track of events (observation groups) as they are streamed in
event_map = OrderedDict()

# Dispatch observations to pipeline
dispatcher = dispatcher.Dispatcher()

# Filter list of properties to cluster. Useful for debugging
properties_to_filter = []
properties_to_filter = ['_59_5']

# RabbitMQ connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
observations_queue = 'debs_observations'
anomalies_queue = 'debs_anomalies'
#channel.queue_declare(observations_queue, auto_delete=True)
channel.queue_declare(anomalies_queue, auto_delete=True)
TERMINATION_MESSAGE="~~TERMINATION MESSAGE~~"


def get_machine_property(machine_id, prop_id, prop_name):
    model_name = machines[machine_id].model
    properties = machine_models[model_name].properties[prop_id]
    return getattr(properties, prop_name)

def exit_gracefully():
    """Shutdown all connections etc. before terminating
    """
    connection.close()
