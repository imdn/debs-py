class ObservationGroup(object):
    def __init__(self, id):
        self.group_id = id
        self.machine_id = None
        self.timestamp_id = None
        self.timestamp_value = None
        self.cycle = None
        self.observations = dict()

    def get_observations(self):
        """ Return observations  in current observation group

        Observations are returned as a list of tuples.
        Each tuple contains (TimestampId, ObservedProperty, Value)
        """
        ob_list = []
        for oid in self.observations:
            observation = self.observations[oid]
            ob_list.append((self.timestamp_id, self.timestamp_value, observation.observed_property, observation.output_value))
        return ob_list
    
    def print_info(self):
        print(f"ID: {self.group_id}; Machine: {self.machine_id}; Time: {self.timestamp_value}; Cycle: {self.cycle}")
        for o in self.observations:
            self.observations[o].print_info()


class Observation(object):
    def __init__(self, id):
        self.observation_id = id
        self.observed_property = None
        self.output_id = None
        self.output_value = None

    def print_info(self):
        print (f"ObservationID: {self.observation_id}; Property: {self.observed_property}; Value: {self.output_value}")
