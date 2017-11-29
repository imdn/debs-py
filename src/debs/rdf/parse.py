import threading
from .. import machine as mc
from .. import observations as myobs
from .. import globals as global_vars

def parse_triple(triple_str):
    """Split incoming RDF-triples text into parts
    """
    parts = triple_str.split(' ')
    index = 0
    parsed = False
    subject = predicate = object = None
    isValue = False
    
    for part in parts:
        if part == "":
            # More than one space was encountered
            continue
        index += 1
        token = part.strip('<>').split('#')
        if len(token) == 2:
            ns = token[0].split('/')[-1]
            entity = token[1]
            if index == 1:
                subject = (ns,entity)
            elif index == 2:
                isValue = False
                predicate = (ns, entity)
                if entity == "valueLiteral" or entity == "hasNumberOfClusters":
                    # Set flag to interpret next token (object) as literal value
                    isValue = True
            elif index == 3:
                if isValue:
                    # Object is a literal value. Get corresponding object
                    ns = f'{ns}#{entity}' # The datatype of value literal
                    entity = part.split('"')[1]
                object = (ns, entity)
                parsed = True
            else:
                print (f"Something wrong - {line}")
            #print (f"Namespace - {ns}; Entity - '{entity}', {object}")
    #print()
    return (subject, predicate, object)

# Variables to keep track of current values when parsing metadata
md_cur_model = None
md_cur_machine = None
md_cur_property = None
md_cur_prob_threshold = None

def process_metadata(subject_info, predicate_info, object_info):
    """Read the incoming RDF triple and extract metadata information
    """
    global md_cur_model, md_cur_machine, md_cur_property, md_cur_prob_threshold
    ns_sub, sub = subject_info
    ns_pred, pred = predicate_info
    ns_obj, obj = object_info

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

def process_observations(subject_info, predicate_info, object_info):
    """Read incoming RDF triple and parse the observation
    """
    global cur_machine, cur_obs_group, cur_observation_id, cur_output_id, cur_value_id, skip_observation
    ns_sub, sub = subject_info
    ns_pred, pred = predicate_info
    ns_obj, obj = object_info

    #print (sub, pred, obj)

    if pred == 'type':
        if obj == 'MoldingMachineObservationGroup':
            # Signals start of a new event
            if cur_obs_group != None:
                # Add old event to dispatcher
                global_vars.dispatcher.process_event(cur_machine, cur_obs_group)
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

def parse_metadata_file(metadata_file):
    with open(metadata_file,'r') as fp:
        for line in fp:
            sub, pred, obj = parse_triple(line.strip('\n'))
            process_metadata(sub, pred, obj)

def cleanup():
    """Dispatch remaining events once stream has ended
    """
    print("Cleaning up the parsed observations")
    global_vars.dispatcher.process_event(cur_machine, cur_obs_group, force_run=True)
