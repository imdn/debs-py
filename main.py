#!/usr/bin/python3

import os
import sys
import re
import asyncio
import debs.globals as global_vars
import debs.rdf.parse as parser
import debs.machine as mc
import debs.observations as myobs
from debs.dispatcher import Dispatcher

metadata_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.metadata.nt"
observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
#observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/mini_obsgrp_25.nt"

# Variables to keep track of current values when parsing metadata
md_cur_model = None
md_cur_machine = None
md_cur_property = None
md_cur_prob_threshold = None

def process_metadata(a, b, c):
    global md_cur_model, md_cur_machine, md_cur_property, md_cur_prob_threshold
    ns_sub, sub = a
    ns_pred, pred = b
    ns_obj, obj = c

    #print ("Processing - ", sub, pred, obj)
    # Process triple based on predicate value
    if pred == "type":
        if obj == "MoldingMachine":
            # machine - type -> moldingMachine
            global_vars.machines[sub] = mc.Machine(sub)
            md_cur_machine = sub
        elif obj == "StatefulProperty":
            global_vars.machine_models[md_cur_model].properties[sub].stateful = True
    
    elif pred == "hasModel":
        # machine - hasModel -> machineModel
        global_vars.machines[sub].model = obj
        md_cur_model = obj
        if not obj in global_vars.machine_models:
            global_vars.machine_models[obj] = mc.MachineModel()

    elif pred == "hasProperty":
        # machineModel - hasProperty -> property
        global_vars.machine_models[sub].properties[obj] = mc.Property(obj)
        md_cur_property = obj
    
    elif pred == "hasNumberOfClusters":
        # prop - hasNumberOfClusters -> num_clusters
        global_vars.machine_models[md_cur_model].properties[sub].num_clusters = int(obj)
    
    elif pred == "valueLiteral":
        if sub.find('ProbabilityThreshold') >= 0:
            # ProbabilityThreshold_<id> - valueLiteral -> <value>
            md_cur_prob_threshold = float(obj)

    elif pred == "isThresholdForProperty":
        # ProbabilityThreshold_<id> - isThresholdForProperty -> <property_id>
        global_vars.machine_models[md_cur_model].properties[obj].prob_threshold = md_cur_prob_threshold


# Track latest values while parsing observations
cur_machine = None
cur_obs_group = None
cur_observation_id = None
cur_output_id = None
cur_value_id = None
skip_observation = False

def process_observations(a, b, c):
    global cur_machine, cur_obs_group, cur_observation_id, cur_output_id, cur_value_id, skip_observation
    ns_sub, sub = a
    ns_pred, pred = b
    ns_obj, obj = c

    #print (sub, pred, obj)

    if pred == 'type':
        if obj == 'MoldingMachineObservationGroup':
            # Signals start of a new event
            if cur_obs_group != None:
                # Add old event to dispatcher
                myDispatcher.process_event(cur_machine, cur_obs_group)
                pass
            global_vars.event_map[sub] = myobs.ObservationGroup(sub)
            # (Re)set outputs and values maps
            outputs = dict()
            values = dict()
            cur_obs_group = sub
        elif type == 'Cycle':
            pass
    
    elif pred == 'observationResultTime':
        global_vars.event_map[sub].timestamp_id = obj

    elif pred == 'machine':
        global_vars.event_map[sub].machine_id = obj
        cur_machine = obj
    
    elif pred == 'contains':
        cur_observation_id = obj
        # Create observation object
        global_vars.event_map[cur_obs_group].observations[obj] = myobs.Observation(obj)

    elif pred == 'hasValue':
        cur_value_id = obj

    elif pred == 'observationResult':
        cur_output_id = obj

    elif pred == 'observedCycle':
        global_vars.event_map[sub].cycle = obj
    
    elif pred == 'observedProperty':
        m_model = global_vars.machine_models[global_vars.machines[cur_machine].model]
        # Only track stateful properties, else omit observing
        if not m_model.is_stateful_property(obj):
            skip_observation = True
            del global_vars.event_map[cur_obs_group].observations[cur_observation_id]
        else:
            skip_observation = False
            global_vars.event_map[cur_obs_group].observations[cur_observation_id].observed_property = obj
    
    elif pred == 'valueLiteral':
        if sub.find('Timestamp') >= 0 and global_vars.event_map[cur_obs_group].timestamp_id == sub :
            global_vars.event_map[cur_obs_group].timestamp_value = obj
        elif sub.find('Value') >= 0 and not skip_observation:
            global_vars.event_map[cur_obs_group].observations[cur_observation_id].output_value = obj

def run():
    count = 0
    with open(metadata_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            process_metadata(sub, pred, obj)

    with open(observations_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            process_observations(sub, pred, obj)

        # Stream Ended call dispatcher again
        myDispatcher.process_event(cur_machine, cur_obs_group, force_run=True)


myDispatcher = Dispatcher()

print (f"Metadata from - {metadata_file}\nObservations from - {observations_file}")
run()


def csv_print():
    row_array = []
    headers = []
    header_set = False
    for group in global_vars.event_map:
        col_array = []
        og = global_vars.event_map[group]
        col_array.append(og.machine_id)
        col_array.append(og.timestamp_id)
        for ob in og.observations:
            obs = og.observations[ob]
            col_array.append(obs.output_value)
            if not header_set:
                headers.append(obs.observed_property)

        if not header_set:
            row_array.append(['', '', *headers])
            header_set = True
        
        row_array.append(col_array)

    import csv
    with open('test_new.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        #writer.writerow(headers)
        for row in row_array:
            writer.writerow(row)

#csv_print()
#myDispatcher.print_info()

