"""
Global variables to share across files
"""
import pika
import os
import struct
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

# HOBBIT Platform Specific
mq_hostname = os.environ['HOBBIT_RABBIT_HOST'] 
session_id = os.environ['HOBBIT_SESSION_ID']
TERMINATION_MESSAGE="~~Termination Message~~"
SYSTEM_READY_SIGNAL = b'\x01'
TASK_GENERATION_FINISHED = b'\x0F'
CMD_EXCHANGE = 'hobbit.command'

def construct_cmd_queue_msg(signal):
    """
    Command Structure :
    Big Endian
    Byte 0 - 4: Length of session id as 32-bit Integer
    Byte 5 - s: Session ID string of length s-4
    Byte s+1 - s+2: Command
    Byte s+3 : Data if any (ignored here)
    """
    session_id_bytes = session_id.encode('utf-8')
    length = len(session_id_bytes)
    format_string = f">i{length}sc"
    byte_buffer = struct.pack(format_string, length, session_id_bytes, signal)

    return byte_buffer

def unpack_cmd_queue_msg(msg):
    session_id_bytes = session_id.encode('utf-8')
    length = len(session_id_bytes)
    payload_length = length + 5 # SessionId bytes + 4 length bytes + 1 cmd byte
    trimmed_message = bytearray(msg)[0:payload_length]
    format_string = f">i{length}sc"
    rcv_len, rcv_sid, cmd = struct.unpack(format_string, trimmed_message)
    return (rcv_len, rcv_sid, cmd)

def send_to_cmd_queue(message):
    global cmd_channel
    cmd_channel.basic_publish(exchange=CMD_EXCHANGE,
                              routing_key='',
                              body=construct_cmd_queue_msg(message))

def init_connections():
    """Intialize connections"""
    global connection, input_channel, output_channel, cmd_channel, input_queue, output_queue

    input_queue = f"hobbit.datagen-system.{session_id}"
    output_queue = f"hobbit.system-evalstore.{session_id}"
    
    connection = pika.BlockingConnection(pika.ConnectionParameters(mq_hostname))
    input_channel = connection.channel()
    output_channel = connection.channel()
    cmd_channel = connection.channel()
    input_channel.basic_qos(prefetch_count=1)
    output_channel.basic_qos(prefetch_count=1)
    cmd_channel.exchange_declare(exchange=CMD_EXCHANGE,
                                 exchange_type='fanout',
                                 durable=False,
                                 auto_delete=True)
    input_channel.queue_declare(input_queue, auto_delete=True)
    output_channel.queue_declare(output_queue, auto_delete=True)


def exit_gracefully():
    """Shutdown all connections etc. before terminating
    """
    input_channel.close()
    output_channel.close()
    cmd_channel.close()
    connection.close()
