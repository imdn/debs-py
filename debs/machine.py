class MachineModel(object):
    def __init__(self):
        self.type = type
        self.properties = dict()

    def print_info(self):
        count = 1
        for prop in self.properties:
            p = self.properties[prop]
            print(f"Property {count} - ID: {p.propId}; pThreshold: {p.probThreshold}; #clusters - {p.numClusters}")

    def hasProperty(self, propId):
        if propId in self.properties():
            return True
        return False

# Property of MachineModel
class Property(object):
    def __init__(self, id):
        self.propId = id
        self.probThreshold = None
        self.numClusters = None # K
        
class Machine(object):
    def __init__(self, id):
        self.id = id
        self.type = None
        self.model = None


