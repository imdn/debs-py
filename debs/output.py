import logging

outputHandler = logging.FileHandler('output.nt', mode='w')
outputHandler.setLevel(logging.ERROR)
logging.getLogger().addHandler(outputHandler)

sequence_num = 0

def create_triple(sub, pred, obj):
    return f"{sub} {pred} {obj} ."

def writer(data):
    global sequence_num
    print ("In writer")
    DEBS_URI = "<http://project-hobbit.eu/resources/debs2017#{}>"
    RDF_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    MACHINE_URI ="<http://www.agtinternational.com/ontologies/I4.0#machine>"
    IOT_URI = "<http://www.agtinternational.com/ontologies/IoTCore#{}>"
    RESULT_URI = "<http://www.agtinternational.com/ontologies/DEBSAnalyticResults#{}>"
    WMM_URI = "<http://www.agtinternational.com/ontologies/WeidmullerMetadata#{}>"
    XML_DOUBLE_URI = "<http://www.w3.org/2001/XMLSchema#double>"
    XML_DATETIME_URI = "<http://www.w3.org/2001/XMLSchema#dateTime>"
    output_triples = []

    machine = data['machine']
    property = data['dimension']
    ts_id = data['timestamp_id']
    ts_val = data['timestamp_val']
    probability = data['probability']

    anomaly_id = f"Anomaly_{sequence_num}"
    anomaly_uri = DEBS_URI.format(anomaly_id)
    anomaly_type = RESULT_URI.format("Anomaly")
    timestamp_uri = DEBS_URI.format(ts_id)

    output_triples.append(create_triple(anomaly_uri, RDF_URI, anomaly_type))
    output_triples.append(create_triple(anomaly_uri, MACHINE_URI, WMM_URI.format(machine)))
    output_triples.append(create_triple(anomaly_uri, RESULT_URI.format("inAbnormalDimension"), WMM_URI.format(property)))
    output_triples.append(create_triple(anomaly_uri, RESULT_URI.format("hasTimeStamp"), DEBS_URI.format(ts_id)))
    output_triples.append(create_triple(anomaly_uri, RESULT_URI.format("hasProbabilityOfObservedAbnormalSequence"), '"{}"^^{}'.format(probability, XML_DOUBLE_URI)))
    output_triples.append(create_triple(timestamp_uri, RDF_URI, IOT_URI.format("Timestamp")))
    output_triples.append(create_triple(timestamp_uri, IOT_URI.format("ValueLiteral"),'"{}"^^{}'.format(ts_val, XML_DATETIME_URI)))

    logging.error("\n".join(output_triples))

    sequence_num += 1
