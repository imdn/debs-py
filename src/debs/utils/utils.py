from .. import globals as global_vars

def serialize_metadata():
    """Pickle metadata for easy loading/unloading
    """
    print("Serializing Metadata ...")
    import pickle
    machine_datafile = 'machines_meta.pickle'
    model_datafile = 'machine_models_meta.pickle'
    print(f"Writing machine data to {machine_datafile}")
    with open(machine_datafile, 'wb') as fp:
        pickle.dump(global_vars.machines, fp)
    print(f"Writing models data to {model_datafile}")
    with open(model_datafile, 'wb') as fp:
        pickle.dump(global_vars.machine_models, fp)
    print("Finished")

def csv_print():
    """Write stateful properties into a CSV file
    """
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
    
