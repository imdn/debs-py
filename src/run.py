#!/usr/bin/python3

import argparse
import pickle
import threading
import time
import urllib.request
from urllib.error import URLError, HTTPError
import debs.globals as global_vars
import debs.rdf.parse as parser
import debs.output as output
import debs.utils.utils as utils
from datetime import datetime

def remote_log(message):
    url=f"http://imdn.pythonanywhere.com/msg/{message}"
    try:
        urllib.request.urlopen(url)
    except HTTPError as e:
        print('Error code: ', e.code)
    except URLError as e:
        print('Reason: ', e.reason)
    finally:
        time.sleep(1)

def load_metadata(parse=False, metadata_file=None):
    """Load serialized metadata"""
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


def parse_message_body(content):
    """Process contents of input queue"""
    if content == global_vars.TERMINATION_MESSAGE:
        parser.cleanup()
        print (f"Input stream ended due to {content}")
        remote_log (f"Input stream ended due to {content}")
        return global_vars.TERMINATION_MESSAGE
    else:
        triples_str = content.strip('\n')
        sub, pred, obj = parser.parse_triple(triples_str)
        parser.process_observations(sub, pred, obj)
        return None

def input_queue_consumer():
    """Consume data from input queue on a separate thread"""
    # Consume the message queue
    start_execution_barrier.wait()
    queue_name = global_vars.INPUT_QUEUE_NAME
    print (f"Waiting for message on {queue_name}...")
    remote_log (f"Waiting for message on {queue_name}...")
    for message in global_vars.input_queue:
        message.ack()
        content = message.body.decode('utf-8')
        terminate = parse_message_body(content)
        if terminate == global_vars.TERMINATION_MESSAGE:
            termination_message_barrier.wait()
            return

def cmd_queue_consumer():
    """Consume data from command queue on a separate thread"""
    for message in global_vars.cmd_queue:
        print (f"Message received on command_channel: {message.body.hex()}")
        #message.pprint()
        len, sess_id, cmd = global_vars.unpack_cmd_queue_msg(message.body)
        if cmd == global_vars.TASK_GENERATION_FINISHED:
            print (f"TASK_GENERATION_FINISHED... ")
            remote_log("TASK_GENERATION_FINISHED...")
            # Countdown barrier to execution now task task generation's finished
            start_execution_barrier.wait() 
            return

def close():
    """Send termination message to output stream, close connections and exit"""
    print ("Sending TERMINATION_MESSAGE to output stream ...")
    remote_log('Sending TERMINATION_MESSAGE to output stream')
    output.send_to_output_stream(global_vars.TERMINATION_MESSAGE)
    global_vars.exit_gracefully()
    
        
# def test_run():
#     """Load metadata locally and use date from file"""
#     metadata_file = "../test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.metadata.nt"
#     observations_file = "..//test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
#     #observations_file = "..//test_data/18.04.2017.1molding_machine_308dp/mini_obsgrp_25.nt"
#     #metadata_file = "../test_data/10machines1000dp_static/10molding_machine_1000dp.metadata.nt"
#     #observations_file = "../test_data/10machines1000dp_static/10molding_machine_1000dp.nt"
#     #metadata_file = "../test_data/14.04.2017.1000molding_machine_final_metadata/1000molding_machine.metadata.nt"

#     print (f"Metadata from - {metadata_file}\nObservations from - {observations_file}")

#     load_metadata(parse=True, metadata_file=metadata_file)

#     with open(observations_file,'r') as fp:
#         for line in fp:
#             sub, pred, obj = parser.parse_triple(line.strip('\n'))
#             parser.process_observations(sub, pred, obj)
    
def run():
    """Start worker threads to read input and command queues. Load metadata"""
    cmd_worker = threading.Thread(target=cmd_queue_consumer, name='Command-Queue-Thread')
    cmd_worker.start()
    
    input_queue_worker = threading.Thread(target=input_queue_consumer, name='Input-Queue-Thread')
    input_queue_worker.start()
    
    print (f"Sending SYSTEM_READY_SIGNAL ...")
    remote_log('Sending SYSTEM_READY')
    
    global_vars.send_to_cmd_queue(global_vars.SYSTEM_READY_SIGNAL)

    remote_log('Waiting for TASK_GENERATION_FINISHED')
    print ("Waiting for TASK_GENERATION_FINISHED ...")

    kwargs = { 'connection' : global_vars.connection }


    remote_log('Loading METADATA')
    load_metadata()
    
if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument('-t', '--testrun', action="store_true")
    args = cli_parser.parse_args()

    start_execution_barrier = threading.Barrier(2)
    termination_message_barrier = threading.Barrier(2)

    print ("Initializing System Connections ...")
    remote_log('Initializing connections - {}'.format(datetime.now()))
    global_vars.init_connections()
    remote_log(f'MQ_HOSTNAME - {global_vars.MQ_HOSTNAME}; SESSION_ID - {global_vars.SESSION_ID}')

    if args.testrun:
        test_run()
    else:
        run()
        termination_message_barrier.wait()
        close()
