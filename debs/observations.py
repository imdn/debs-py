class ObservationGroup(object):
    def __init__(self, id):
        self.groupId = id
        self.machineId = None
        self.timeStampId = None
        self.timeStampValue = None
        self.cycle = None
        self.observations = dict()

    def getObservations(self):
        """
        Return list of observations in the form of (TimestampId, ObservedProperty, Value)
        """
        ob_list = []
        for oid in self.observations:
            observation = self.observations[oid]
            ob_list.append((self.timeStampId, observation.observedProperty, observation.outputValue))
        return ob_list
    
    def print_info(self):
        print(f"ID: {self.groupId}; Machine: {self.machineId}; Time: {self.timeStampValue}; Cycle: {self.cycle}")
        for o in self.observations:
            self.observations[o].print_info()


class Observation(object):
    def __init__(self, id):
        self.observationId = id
        self.observedProperty = None
        self.outputId = None
        #self.outputValueId = None
        self.outputValue = None

    def print_info(self):
        print (f"ObservationID: {self.observationId}; Property: {self.observedProperty}; Value: {self.outputValue}")
