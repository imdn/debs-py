"""
Global variables to share across files
"""
import pika
import os
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
#properties_to_filter = ['_59_5']

def get_machine_property(machine_id, prop_id, prop_name):
    model_name = machines[machine_id].model
    properties = machine_models[model_name].properties[prop_id]
    return getattr(properties, prop_name)

def init_connections():
    """Intialize connections"""
    global channel, connection, observations_queue, anomalies_queue, TERMINATION_MESSAGE

    # Hobbit and RabbitMQ specific envvars
    mq_hostname = os.environ['HOBBIT_RABBIT_HOST'] 
    TERMINATION_MESSAGE="~~TERMINATION MESSAGE~~"
    observations_queue = os.environ['DATA_GEN_2_SYSTEM_QUEUE_NAME']
    anomalies_queue = os.environ['SYSTEM_2_EVAL_STORAGE_QUEUE_NAME']
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(mq_hostname))
    channel = connection.channel()
    channel.queue_declare(observations_queue, auto_delete=True)
    channel.queue_declare(anomalies_queue, auto_delete=True)


def exit_gracefully():
    """Shutdown all connections etc. before terminating
    """
    connection.close()
