"""Standalone file to Simulate streaming of triples from local file
"""

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

observations_queue = 'debs_observations'
channel.queue_declare(observations_queue, auto_delete=True)
TERMINATION_MESSAGE="~~TERMINATION MESSAGE~~"

observations_file = "../test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"

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


stream_observations(observations_file)
