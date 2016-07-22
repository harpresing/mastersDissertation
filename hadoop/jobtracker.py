# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from logger import HadoopLogger
import time
import socket
import sys
import traceback
from trace import TraceOutput
from comm import JOBTRACKER_PORT, TASKTRACKER_PORT

class JobTracker():
    def __init__(self, name, numMaps, numReduces, config, submitTime, trace):
        self.trace = trace
        self.config = config
        self.name = name
        self.numMaps = numMaps
        self.numReduces = numReduces
        self.logger = HadoopLogger(self.config.host, self.config.username, "jobtracker")
        self.logger.startup(self.config.host)
        self.submitTime = submitTime
 
    def startJob(self):
        self.startTime = time.time()
        self.logger.job_start(self.name, self.numMaps, self.numReduces)
        for host in self.trace.getHosts():
            ok = False
            while not ok:
                ok = self.startTaskTracker(host)
                if not ok:
                    time.sleep(0.1)

    def startTaskTracker(self, host):
        try:
            s = socket.socket()
            port = TASKTRACKER_PORT
            print "JobTracker: %s is sending start to %s:%d" % (self.config.host, host, port)
            s.connect((host, port))
            data = "start"
            s.send(data)
            data = s.recv(1024)
            print "JobTracker: %s received %s from %s:%d" % (self.config.host, str(data), host, port)
            s.close()
            return True
        except:
            traceback.print_exc(None, sys.stderr)
            return False
                    
    def finishJob(self):
        self.logger.job_finish(self.name)
        self.finishTime = time.time()
        output = TraceOutput(self.config.host)
        output.saveJobTracker(self.config, self.submitTime, self.startTime, self.finishTime, self.name, self.numMaps, self.numReduces)

    def waitTaskTrackers(self):
        #self.logger.info("Starting ShuffleServer (%s:%d)" % (self.host, SHUFFLE_PORT))
        s = socket.socket()
        host = self.config.host
        port = JOBTRACKER_PORT
        print "JobTracker: starting server in %s:%d" % (host, port)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)

        print self.trace.getHosts()
        numTaskTrackers = self.trace.getNumHosts()
        print "JobTracker: waiting for %d task trackers" % numTaskTrackers
        while numTaskTrackers > 0:
            c, addr = s.accept()
            data = c.recv(1024)
            print "JobTracker: received %s from %s:%d" % (data, addr[0], addr[1])
            data = "ok"
            c.send(data)
            c.close()
            numTaskTrackers -= 1
