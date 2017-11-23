#!/usr/bin/python3

import pickle
import logging
import debs.globals as global_vars
import debs.rdf.parse as parser
from debs.dispatcher import Dispatcher

log_level=logging.WARNING
#log_level=logging.DEBUG
#log_level=logging.INFO
logging.basicConfig(filename='dispatch.log', filemode='w', level=log_level, format='%(filename)s: %(message)s')


metadata_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.metadata.nt"
observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
#observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/mini_obsgrp_25.nt"
#metadata_file = "/home/imad/workspace/agt-challenge/test_data/10machines1000dp_static/10molding_machine_1000dp.metadata.nt"
#observations_file = "/home/imad/workspace/agt-challenge/test_data/10machines1000dp_static/10molding_machine_1000dp.nt"
#metadata_file = "/home/imad/workspace/agt-challenge/test_data/14.04.2017.1000molding_machine_final_metadata/1000molding_machine.metadata.nt"

print (f"Metadata from - {metadata_file}\nObservations from - {observations_file}")

def load_metadata(parse=False):
    """Load serialized metadata
    """
    if parse:
         print("Now parsing metadata file ...")
         parser.parse_metadata_file(metadata_file)
         print("Done parsing metadata!")
    else:
        print("Loading serialized metadata")
        machine_datafile = 'machines_meta.pickle'
        model_datafile = 'machine_models_meta.pickle'
        
        with open(machine_datafile, 'rb') as fp:
            global_vars.machines = pickle.load(fp)
        with open(model_datafile, 'rb') as fp:
            global_vars.models = pickle.load(fp)


def parse_message(ch, method, properties, body):
    message = body.decode()
    if message == global_vars.TERMINATION_MESSAGE:
        parser.cleanup()
        global_vars.channel.stop_consuming()
        print ("Finished stream processing")
    else:
        triples_str = message.strip('\n')
        sub, pred, obj = parser.parse_triple(triples_str)
        parser.process_observations(sub, pred, obj)

def start_listening():
    print("Started listening")
    queue = global_vars.observations_queue
    global_vars.channel.queue_declare(queue, auto_delete=True)
    global_vars.channel.basic_consume(parse_message,
                                      queue=queue,
                                      no_ack=True)
    global_vars.channel.start_consuming()

def run():
    load_metadata(parse=True)
    
    #start_listening()
    
    with open(observations_file,'r') as fp:
         for line in fp:
             sub, pred, obj = parser.parse_triple(line.strip('\n'))
             parser.process_observations(sub, pred, obj)

    # Stream Ended call dispatcher again
    # myDispatcher.process_event(cur_machine, cur_obs_group, force_run=True)

#myDispatcher = Dispatcher()
run()

global_vars.exit_gracefully()
