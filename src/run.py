#!/usr/bin/python3

import argparse
import pickle
import struct
import debs.globals as global_vars
import debs.rdf.parse as parser
import debs.output as output
import debs.utils.utils as utils
from debs.dispatcher import Dispatcher

def load_metadata(parse=False, metadata_file=None):
    """Load serialized metadata
    """
    if parse:
         print("Now parsing metadata file ...")
         parser.parse_metadata_file(metadata_file)
         print("Done parsing metadata!")
    else:
        print("Loading serialized metadata ...")
        #machine_datafile = 'metadata/machine_59_meta.pickle'
        #model_datafile = 'metadata/machine_model_59_meta.pickle'
        machine_datafile = 'metadata/machines_final_meta.pickle'
        model_datafile = 'metadata/machine_models_final_meta.pickle'
        
        with open(machine_datafile, 'rb') as fp:
            global_vars.machines = pickle.load(fp)
        with open(model_datafile, 'rb') as fp:
            global_vars.machine_models = pickle.load(fp)


def parse_message(ch, method, properties, body):
    message = body.decode()
    if message == global_vars.TERMINATION_MESSAGE:
        parser.cleanup()
        global_vars.input_channel.stop_consuming()
        print ("Finished stream processing")
    else:
        triples_str = message.strip('\n')
        sub, pred, obj = parser.parse_triple(triples_str)
        parser.process_observations(sub, pred, obj)

def start_listening():
    queue = global_vars.input_queue
    print(f"Listening for messages on queue - {queue}")
    global_vars.input_channel.queue_declare(queue, auto_delete=True)
    global_vars.input_channel.basic_consume(parse_message,
                                      queue=queue,
                                      no_ack=True)
    global_vars.input_channel.start_consuming()

def cmd_callback(ch, method, properties, body):
    print (f"Received body - {body}")
    len, sess_id, cmd = global_vars.unpack_cmd_queue_msg(body)
    if sess_id.decode('utf-8') == global_vars.session_id:
        if cmd == global_vars.TASK_GENERATION_FINISHED:
            global_vars.cmd_channel.stop_consuming()


    
def wait_for_task_generation():
    result = global_vars.cmd_channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    global_vars.cmd_channel.queue_bind(exchange = global_vars.CMD_EXCHANGE,
                                       queue=queue_name)

    global_vars.cmd_channel.basic_consume(cmd_callback,
                                          queue=queue_name,
                                          no_ack=True)
    global_vars.cmd_channel.start_consuming()


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
    
def close():
    """Send termination message to output stream and exit
    """
    print ("Sending TERMINATION_MESSAGE to output stream ...")
    output.send_to_output_stream(global_vars.TERMINATION_MESSAGE)
    global_vars.exit_gracefully()

def run():
    """Load metadata from serialized file and listen to messages from incoming queue
    """
    print (f"Sending SYSTEM_READY_SIGNAL ...")
    global_vars.send_to_cmd_queue(global_vars.SYSTEM_READY_SIGNAL)

    load_metadata()

    print ("Waiting for TASK_GENERATION_FINISHED ...")
    wait_for_task_generation()

    # Consume the message queue
    start_listening()

if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('-t', '--testrun', action="store_true")
    args = cli_parser.parse_args()

    print ("Initializing System Connetions ...")
    global_vars.init_connections()
    
    if args.testrun:
        test_run()
    else:
        run()
        
        close()
