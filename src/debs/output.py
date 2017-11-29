import logging
import os
import rabbitpy
import threading
from . import globals as global_vars

outputHandler = logging.FileHandler('output.nt', mode='w')
outputHandler.setLevel(logging.ERROR)
logging.getLogger(__name__).addHandler(outputHandler)

SYSTEM_URI = os.environ['SYSTEM_URI_KEY']
DEBS_URI = f"<{SYSTEM_URI}#{{}}>"
RDF_URI = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
MACHINE_URI ="<http://www.agtinternational.com/ontologies/I4.0#machine>"
IOT_URI = "<http://www.agtinternational.com/ontologies/IoTCore#{}>"
RESULT_URI = "<http://www.agtinternational.com/ontologies/DEBSAnalyticResults#{}>"
WMM_URI = "<http://www.agtinternational.com/ontologies/WeidmullerMetadata#{}>"
XML_DOUBLE_URI = "<http://www.w3.org/2001/XMLSchema#double>"
XML_DATETIME_URI = "<http://www.w3.org/2001/XMLSchema#dateTime>"

sequence_num = 0

def create_triple(sub, pred, obj):
    return f"{sub} {pred} {obj} ."

def send_to_output_stream(msg_body):
    """Dispatch message to queue"""
    properties = {
        'delivery_mode' : 2 # Make message persistent
    }
    message = rabbitpy.Message(global_vars.output_channel,
                               msg_body,
                               properties=properties)
    message.publish('', routing_key=global_vars.OUTPUT_QUEUE_NAME)
    return None


def writer(data):
    """Wraps the create_and_write def by sending it to a different thread"""
    output_worker = threading.Thread(target=create_and_write, args=[data], name='Output-Writer-Thread')
    output_worker.start()

def create_and_write(data):
    """Create output triples and send them to be delivered"""
    global sequence_num
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

    #output_str = "\n".join(output_triples)
    #logging.error(output_str)
    for triple in output_triples:
        send_to_output_stream(triple)
    sequence_num += 1
