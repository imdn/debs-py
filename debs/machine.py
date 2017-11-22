# Machine model metadata
class MachineModel(object):
    def __init__(self):
        self.type = type
        self.properties = dict()

    def print_info(self):
        count = 1
        for prop in self.properties:
            p = self.properties[prop]
            print(f"Property {count} - ID: {p.propId}; pThreshold: {p.probThreshold}; #clusters - {p.numClusters}")

    def isStatefulProperty(self, propId):
        if propId in self.properties:
            return self.properties[propId].stateful
        return None

# Properties of a given Machine Model
class Property(object):
    def __init__(self, id):
        self.propId = id
        self.prob_threshold = None
        self.num_clusters = None # K
        self.stateful = False

# Machine
class Machine(object):
    def __init__(self, id):
        self.id = id
        self.type = None
        self.model = None


