import shelve

d = shelve.open('reactiveFlows.db')

for k, v in d.iteritems():
    print v["match"]