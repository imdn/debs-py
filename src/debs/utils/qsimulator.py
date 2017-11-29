"""Standalone file to Simulate streaming of triples from local file
"""
import struct
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

EXCHANGE_NAME = 'hobbit.command'
SYS_READY_SIGNAL = b'\x01'
TASK_GEN_FINISHED_SIGNAL = b'\x0F'
SESSION_ID = '1511785419368'
TERMINATION_MESSAGE="~~Termination Message~~"

channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', auto_delete=True)

observations_queue = 'hobbit.datagen-system.1511785419368'

channel.queue_declare(observations_queue, auto_delete=True)
channel.queue_purge(observations_queue)

observations_file = "../test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
#observations_file = "../test_data/18.04.2017.1molding_machine_308dp/mini.nt"


def stream_observations(observations_file):
    print ("Streaming observations")

    with open(observations_file,'r') as fp:
        for line in fp:
            channel.basic_publish(exchange='',
                                  routing_key=observations_queue,
                                  body=line)
            #sub, pred, obj = parser.parse_triple(line.strip('\n'))
            #process_observations(sub, pred, obj)

    channel.basic_publish(exchange='',
                          routing_key=observations_queue,
                          body=TERMINATION_MESSAGE)


def send_to_cmd_queue(message):
    cmd_channel.basic_publish(exchange=CMD_EXCHANGE,
                              routing_key='',
                              body=construct_cmd_queue_msg(message))


def construct_cmd_queue_msg(signal):
    """
    Command Structure :
    Big Endian
    Byte 0 - 4: Length of session id as 32-bit Integer
    Byte 5 - s: Session ID string of length s-4
    Byte s+1 - s+2: Command
    Byte s+3 : Data if any (ignored here)
    """
    length = len(SESSION_ID)
    session_id_bytes = SESSION_ID.encode('utf-8')
    format_string = f">i{length}sc"
    byte_buffer = struct.pack(format_string, length, session_id_bytes, signal)

    return byte_buffer

stream_observations(observations_file)

print(f"Sending TASK GEN FINISHED SIGNAL - {TASK_GEN_FINISHED_SIGNAL}")
channel.basic_publish(exchange=EXCHANGE_NAME,
                      routing_key='',
                      body=construct_cmd_queue_msg(TASK_GEN_FINISHED_SIGNAL))

channel.close()
connection.close()
