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

# Mappings for metadata
map_prop2model = dict()
map_threshold2prop = dict()

def process_metadata(a, b, c):
    ns_sub, sub = a
    ns_pred, pred = b
    ns_obj, obj = c

    #print ("Processing - ", sub, pred, obj)
    # Process triple based on predicate value
    if pred == "type":
        if obj == "MoldingMachine":
            # machine - type -> moldingMachine
            machines[sub] = mc.Machine(sub)
    
    elif pred == "hasModel":
        # machine - hasModel -> machineModel
        machines[sub].model = obj
        if not obj in models:
            models[obj] = mc.MachineModel()

    elif pred == "hasProperty":
        # machineModel - hasProperty -> property
        propObj = mc.Property(obj)
        models[sub].properties[obj] = propObj
        map_prop2model[obj] = sub
    
    elif pred == "hasNumberOfClusters":
        # prop - hasNumberOfClusters -> numClusters
        model_name = map_prop2model[sub]
        prop = models[model_name].properties[sub]
        prop.numClusters = obj
        models[model_name].properties[sub] = prop
    
    elif pred == "valueLiteral":
        if sub.find('ProbabilityThreshold') >= 0:
            # ProbabilityThreshold_<id> - valueLiteral -> <value>
            thresholds[sub] = obj

    elif pred == "isThresholdForProperty":
        # ProbabilityThreshold_<id> - isThresholdForProperty -> <property_id>
        model_name = map_prop2model[obj]
        prop = models[model_name].properties[obj]
        prop.probThreshold = thresholds[sub]
        models[model_name].properties[obj] = prop
        
# Mappings for observations

map_obsId2outId = dict()
map_outId2valId = dict()
cur_obs_group = None
cur_observation_id = None
cur_output_id = None
cur_value_id = None

def process_observations(a, b, c):
    global cur_obs_group, cur_observation_id, cur_output_id, cur_value_id
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
        obs_groups[cur_obs_group].observations[cur_observation_id].observedProperty = obj
    
    elif pred == 'valueLiteral':
        if sub.find('Timestamp') >= 0 and obs_groups[cur_obs_group].timeStampId == sub :
            obs_groups[cur_obs_group].timeStampValue = obj
        elif sub.find('Value') >= 0:
            obs_groups[cur_obs_group].observations[cur_observation_id].outputValue = obj

def run():
    count = 0
    with open(metadata_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            #process_metadata(sub, pred, obj)

            #if count > 10:
            #    break

    with open(observations_file,'r') as fp:
        for line in fp:
            count +=1
            sub, pred, obj = parser.parse_triple(line.strip('\n'))
            process_observations(sub, pred, obj)


run()

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
