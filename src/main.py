#!/usr/bin/python3

import argparse
from datetime import datetime
import pickle
import debs.globals as global_vars
import debs.rdf.parse as parser
import urllib.request
from urllib.error import URLError, HTTPError
from datetime import datetime
from debs.dispatcher import Dispatcher

def remote_log(message):
    url=f"http://imdn.pythonanywhere.com/msg/{message}"
    return
    try:
        urllib.request.urlopen(url)
    except HTTPError as e:
        print('Error code: ', e.code)
    except URLError as e:
        print('Reason: ', e.reason)    

def load_metadata(parse=False, metadata_file=None):
    """Load serialized metadata
    """
    remote_log("Now parsing Metadata")
    if parse:
         print("Now parsing metadata file ...")
         parser.parse_metadata_file(metadata_file)
         print("Done parsing metadata!")
    else:
        print("Loading serialized metadata ...")
        machine_datafile = 'metadata/machines_final_meta.pickle'
        model_datafile = 'metadata/machine_models_final_meta.pickle'
        #machine_datafile = 'metadata/machine_59_meta.pickle'
        #model_datafile = 'metadata/machine_model_59_meta.pickle'
        
        with open(machine_datafile, 'rb') as fp:
            global_vars.machines = pickle.load(fp)
        with open(model_datafile, 'rb') as fp:
            global_vars.machine_models = pickle.load(fp)


def parse_message(ch, method, properties, body):
    message = body.decode()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if message == global_vars.TERMINATION_MESSAGE:
        parser.cleanup()
        global_vars.input_channel.stop_consuming()
        print ("Finished stream processing")
        remote_log(f"Input stream Terminated")
    else:
        triples_str = message.strip('\n')
        sub, pred, obj = parser.parse_triple(triples_str)
        parser.process_observations(sub, pred, obj)



def start_listening():
    input_channel = global_vars.input_channel
    input_queue = global_vars.input_queue
    print(f"Listening for messages on queue - {input_queue}")
    remote_log(f'Listening for messages on queue - {input_queue}')
    #global_vars.input_channel.basic_consume(parse_message,
    #                                        queue=input_queue)
    queue_state = input_channel.queue_declare(queue=input_queue, passive=True, auto_delete=True)
    print (f"Queue state = {queue_state}")
    remote_log(f"Queue state = {queue_state}")

    #global_vars.input_channel.start_consuming()

    message_count = 0
    # Iterate over messages
    for method_frame, properties, body in input_channel.consume(input_queue):
        message = body.decode()
        message_count += 1
        input_channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        if message_count < 3:
            remote_log(f"Message: {message}")
        if message == global_vars.TERMINATION_MESSAGE:
            parser.cleanup()
            input_channel.stop_consuming()
            print ("Finished stream processing")
            remote_log(f"Input stream Terminated")
        else:
            triples_str = message.strip('\n')
            sub, pred, obj = parser.parse_triple(triples_str)
            parser.process_observations(sub, pred, obj)

def test_run():
    """Load metadata locally and use date from file
    """
    metadata_file = "../test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.metadata.nt"
    observations_file = "..//test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
    #observations_file = "..//test_data/18.04.2017.1molding_machine_308dp/mini_obsgrp_25.nt"
    #metadata_file = "../test_data/10machines1000dp_static/10molding_machine_1000dp.metadata.nt"
    #observations_file = "../test_data/10machines1000dp_static/10molding_machine_1000dp.nt"
    #metadata_file = "../test_data/14.04.2017.1000molding_machine_final_metadata/1000molding_machine.metadata.nt"

    print (f"Metadata from - {metadata_file}\nObservations from - {observations_file}")

    load_metadata(parse=True, metadata_file=metadata_file)

    with open(observations_file,'r') as fp:
        for line in fp:
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            parser.process_observations(sub, pred, obj)

def run():
    """Load metadata from serialized file and listedn to messages from incoming queue
    """
    
    remote_log('Initializing connections - {}'.format(datetime.now()))
    remote_log(f'MQ_HostName - {global_vars.mq_hostname}; SESSION_ID - {global_vars.session_id}')
    load_metadata()

    # Consume the message queue
    start_listening()

if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('-t', '--testrun', action="store_true")
    args = cli_parser.parse_args()

    global_vars.init_connections()
    if args.testrun:
        test_run()
    else:
        run()

    global_vars.exit_gracefully()
