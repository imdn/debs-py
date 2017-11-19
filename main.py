import os
import re
import debs.rdf.parse as parser
import debs.machine as mc
import debs.observations as myobs

from scipy.cluster.vq import kmeans

metadata_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.metadata.nt"
observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/molding_machine_308dp.nt"
#observations_file = "/home/imad/workspace/agt-challenge/test_data/18.04.2017.1molding_machine_308dp/mini.nt"

#file = "/home/imad/workspace/agt-challenge/test_data/10machines1000dp_static/10molding_machine_1000dp.metadata.nt"
machines = dict()
models = dict()
thresholds = dict()
obs_groups = dict()

# Variables to keep track of current values
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
            machines[sub] = mc.Machine(sub)
            md_cur_machine = sub
        elif obj == "StatefulProperty":
            models[md_cur_model].properties[sub].stateful = True
    
    elif pred == "hasModel":
        # machine - hasModel -> machineModel
        machines[sub].model = obj
        md_cur_model = obj
        if not obj in models:
            models[obj] = mc.MachineModel()

    elif pred == "hasProperty":
        # machineModel - hasProperty -> property
        models[sub].properties[obj] = mc.Property(obj)
        md_cur_property = obj
    
    elif pred == "hasNumberOfClusters":
        # prop - hasNumberOfClusters -> numClusters
        models[md_cur_model].properties[sub].numClusters = obj
    
    elif pred == "valueLiteral":
        if sub.find('ProbabilityThreshold') >= 0:
            # ProbabilityThreshold_<id> - valueLiteral -> <value>
            md_cur_prob_threshold = obj

    elif pred == "isThresholdForProperty":
        # ProbabilityThreshold_<id> - isThresholdForProperty -> <property_id>
        models[md_cur_model].properties[obj].probThreshold = md_cur_prob_threshold
        
# Track latest values while parsing
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
            obs_groups[sub] = myobs.ObservationGroup(sub)
            # (Re)set outputs and values maps
            outputs = dict()
            values = dict()
            cur_obs_group = sub
        elif type == 'Cycle':
            pass
    
    elif pred == 'observationResultTime':
        obs_groups[sub].timeStampId = obj

    elif pred == 'machine':
        obs_groups[sub].machineId = obj
        cur_machine = obj
    
    elif pred == 'contains':
        cur_observation_id = obj
        # Create observation object
        obs_groups[cur_obs_group].observations[obj] = myobs.Observation(obj)

    elif pred == 'hasValue':
        cur_value_id = obj

    elif pred == 'observationResult':
        cur_output_id = obj

    elif pred == 'observedCycle':
        obs_groups[sub].cycle = obj
    
    elif pred == 'observedProperty':
        m_model = models[machines[cur_machine].model]
        # Only track stateful properties, else omit observing
        if not m_model.isStatefulProperty(obj):
            skip_observation = True
            del obs_groups[cur_obs_group].observations[cur_observation_id]
        else:
            skip_observation = False
            obs_groups[cur_obs_group].observations[cur_observation_id].observedProperty = obj
    
    elif pred == 'valueLiteral':
        if sub.find('Timestamp') >= 0 and obs_groups[cur_obs_group].timeStampId == sub :
            obs_groups[cur_obs_group].timeStampValue = obj
        elif sub.find('Value') >= 0 and not skip_observation:
            obs_groups[cur_obs_group].observations[cur_observation_id].outputValue = obj

def run():
    count = 0
    with open(metadata_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            process_metadata(sub, pred, obj)

            #if count > 10:
            #    break

    with open(observations_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            process_observations(sub, pred, obj)


run()

# Metadata values
for m in models:
    mod = models[m]
    mod.print_info()
    

#for group in obs_groups:
#    og = obs_groups[group]
#    og.print_info()

def csv_print():
    row_array = []
    for group in obs_groups:
        col_array = []
        og = obs_groups[group]
        col_array.append(og.machineId)
        col_array.append(og.timeStampValue)
        for ob in og.observations:
            obs = og.observations[ob]
            col_array.append(obs.outputValue)
        row_array.append(col_array)

    import csv
    with open('test.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for row in row_array:
            writer.writerow(row)

csv_print()
