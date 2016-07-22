# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from threading import Thread
import time
import socket
from comm import REDUCELISTENER_PORT
import cPickle as pickle
import sys
import traceback

class MapTask(Thread):
    def __init__(self, task, control):
        Thread.__init__(self)
        self.task = task
        self.name = "Thread-%s" % task.name
        self.control = control

    def run(self):
        print "MapTask: starting (%s)" % self.task.name
        time.sleep(self.task.waitTime)
        self.control.start_map_task(self.task)
        
        # Sleep to simulate the tasks exectution time
        time.sleep(self.task.duration)
       
        self.control.end_map_task(self.task)

        # TODO: enviar para todos os reducers, salvar intention no log
        self.writeOutputData()

        if len(self.task.partitions) > 0:
            partitionLength = []
            self.task.partitions.sort(key=lambda x: x.reducer, reverse=False)
            for partition in self.task.partitions:
                partitionLength.append(partition.size)
                ok = False
                while not ok:
                    ok = self.sendPartition(partition)
                    if not ok:
                        time.sleep(0.1)
                print "MapTask: sent partition to %s" % partition.dstAddress
            self.control.logger.shuffle_intent(partition.mapper, partitionLength)

        print "MapTask: finished (%s)" % self.task.name

    def writeOutputData(self):
        print "salve mapoutput"
    
    def sendPartition(self, partition):
        try:
            s = socket.socket()
            host = partition.dstAddress
            port = REDUCELISTENER_PORT
            s.connect((host, port))
            data = pickle.dumps(partition, -1) 
            s.send(data)
            data = s.recv(1024)
            response = pickle.loads(data)
            print "Response received " + str(response)
            s.close()
            return True
        except:
            print traceback.print_exc(None, sys.stderr)
            return False
