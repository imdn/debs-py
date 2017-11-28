"""
Global variables to share across files
"""
import rabbitpy
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
CMD_EXCHANGE_NAME = 'hobbit.command'

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
    body = construct_cmd_queue_msg(message)
    message_obj = rabbitpy.Message(cmd_channel, body)
    message_obj.publish(cmd_exchange)

def init_connections():
    """Intialize connections"""
    global connection, input_channel, output_channel, cmd_channel
    global cmd_exchange, input_queue, output_queue, cmd_queue

    mq_port = 5672
    input_queue_name = f"hobbit.datagen-system.{session_id}"
    output_queue_name = f"hobbit.system-evalstore.{session_id}"

    connection_str = f"amqp://guest:guest@{mq_hostname}:{mq_port}/"
    connection = rabbitpy.Connection(connection_str)
    cmd_channel = connection.channel()
    input_channel = connection.channel()
    output_channel = connection.channel()
    input_channel.prefetch_count(1)
    output_channel.prefetch_count(1)
    cmd_exchange = rabbitpy.Exchange(cmd_channel,
                                     CMD_EXCHANGE_NAME,
                                     exchange_type='fanout',
                                     durable=False,
                                     auto_delete=True)
    cmd_exchange.declare()

    cmd_queue = rabbitpy.Queue(cmd_channel, exclusive=True)
    input_queue = rabbitpy.Queue(input_channel,
                                 input_queue_name,
                                 auto_delete=True)
    output_queue = rabbitpy.Queue(output_channel,
                                  output_queue_name,
                                  auto_delete=True)
    cmd_queue.declare()
    cmd_queue.bind(cmd_exchange)
    input_queue.declare()
    output_queue.declare()

def exit_gracefully():
    """Shutdown all connections etc. before terminating
    """
    input_channel.close()
    output_channel.close()
    cmd_channel.close()
    connection.close()
