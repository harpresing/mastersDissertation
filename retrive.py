import json
from pprint import pprint

with open('reactiveFlows.json') as data_file:
    data = json.load(data_file)

for i in xrange(len(data)):
    pprint(data[i]['dl_src'])
