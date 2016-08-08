import json
from hederaController import HederaController


def saveflows():
    ob = HederaController()
    flowFile = 'reactiveFlows.json'

    with open(flowFile, 'w') as f:
        json.dump(ob.all_flows, f)

    print "Saved reactive flow paths in %s " % flowFile


if __name__ == "__main__":
    saveflows()
