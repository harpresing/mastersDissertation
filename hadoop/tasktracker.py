# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from threading import Thread, Condition, Lock
from logger import HadoopLogger
from comm import JOBTRACKER_PORT, TASKTRACKER_PORT, REDUCELISTENER_PORT, SHUFFLESERVER_PORT
from config import get_mininet_bw_ratio
from maptask import MapTask
from reducetask import ReduceTask
import time
import socket
import sys
import copy
from Queue import Queue
import cPickle as pickle
from subprocess import Popen
from trace import TraceOutput

TIMEOUT = 3600



class TaskTracker(Thread):
    def __init__(self, trace, config, maps, reduces, numTransfers):
        Thread.__init__(self)
        self.trace = trace
        self.config = config
        self.maps = maps
        self.reduces = reduces
        self.numTransfers = numTransfers
        self.logger = HadoopLogger(self.config.host, self.config.username, "tasktracker")
        self.control = Control(self.logger)

    def run(self):
        ok = False
        while not ok:
            ok = self.ready()
        self.waitForJobTracker()
        
        if len(self.reduces) > 0:
	        # Start Partition Listener
            partitionQueue = dict()
            for task in self.reduces:
                partitionQueue[task.name] = []
            reduceListener = ReduceListener(self.reduces, self.control, self.config, partitionQueue)
            reduceListener.start()
            
        # Schedule map tasks
        if len(self.maps) > 0:
            shuffleServer = ShuffleServer(self.config.host, self.numTransfers)
            shuffleServer.start()
            # Start Map Task Launcher
            initialDelay = self.trace.getInitialMapDelay(self.config.host)
            mapLauncher = MapTaskLauncher(self.maps, self.control, self.config, initialDelay)
            mapLauncher.start()

        # Schedule reduce tasks
        if len(self.reduces) > 0:
            # Start Reduce Task Launcher
            initialDelay = self.trace.getInitialReduceDelay(self.config.host)
            reduceLauncher = ReduceTaskLauncher(self.reduces, self.control, self.config, partitionQueue, initialDelay)
            reduceLauncher.start()
        
        if len(self.maps) > 0:
            mapLauncher.join()

        if len(self.reduces) > 0:
            reduceListener.join()

        if len(self.maps) > 0:
            shuffleServer.join()
        
        if len(self.reduces) > 0:
            reduceLauncher.join()

        self.finish()
        
        output = TraceOutput(self.config.host)
        output.saveTaskTracker(self.control.completedMaps, self.control.completedReduces, self.control.completedTransfers)
        

    def waitForJobTracker(self):
        #self.logger.info("Starting ShuffleServer (%s:%d)" % (self.host, SHUFFLE_PORT))
        s = socket.socket()
        host = self.config.host
        port = TASKTRACKER_PORT
        print "TaskTracker: starting server in %s:%d" % (host, port)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)
        print "TaskTracker: waiting for JobTracker start message"
        c, addr = s.accept()
        data = c.recv(1024)
        print "TaskTracker: received %s from %s:%d" % (data, addr[0], addr[1])
        data = "ok"
        c.send(data)
        c.close()

    def finish(self):
        s = socket.socket()
        host = self.config.jobTracker
        port = JOBTRACKER_PORT
        print "TaskTracker: %s is sending finish to %s:%d" % (self.config.host, host, port)
        s.connect((host, port))
        data = "finish"
        s.send(data)
        data = s.recv(1024)
        print "TaskTracker: %s received %s from %s:%d" % (self.config.host, str(data), host, port)
        s.close()

    def ready(self):
        try:
            s = socket.socket()
            host = self.config.jobTracker
            port = JOBTRACKER_PORT
            print "TaskTracker: %s is sending ready to %s:%d" % (self.config.host, host, port)
            s.connect((host, port))
            data = "ready"
            s.send(data)
            data = s.recv(1024)
            s.close()
            print "TaskTracker: %s received %s from %s:%d" % (self.config.host, str(data), host, port)
            return True
        except:
            return False

class MapTaskLauncher(Thread):
    def __init__(self, tasks, control, config, delay):
        Thread.__init__(self)
        self.tasks = copy.deepcopy(tasks)
        self.num_tasks = len(self.tasks)
        self.control = control
        self.config = config
        self.delay = delay
#        self.logger = SimulatorLogger(config.host).get()

    def run(self):
#        print "MapTaskLauncher: starting"
#        self.logger.info("Starting MapTaskLauncher")
        time.sleep(self.delay)
        
        threads = []
        self.control.cond.acquire()

        # Start first tasks to fill tasks slots 
        for i in xrange(self.config.mapTaskSlots):
            if len(self.tasks) <= 0:
                break
            task = self.tasks.pop(0)
            print "MapTaskLauncher: slot " + str(i) + " - starting task: " + str(task)            
            t = MapTask(task, self.control)
            t.start()
            threads.append(t)

        start_time = time.time()
        while self.control.finished_map_threads < self.num_tasks and (time.time() - start_time < TIMEOUT): 
            self.control.cond.wait()
            free_slots = self.config.mapTaskSlots - self.control.active_map_threads
#            print "active_map_threads=%d" % self.control.active_map_threads
#            print "finished_map_threads=%d" % self.control.finished_map_threads
#            print "remaining_tasks=%d" % len(self.tasks)            
#            print "free_slots=" + str(free_slots)
            for j in xrange(free_slots):
                if len(self.tasks) <= 0:
                    continue
                task = self.tasks.pop(0)
                t = MapTask(task, self.control)
                t.start()
                threads.append(t)
    
        self.control.cond.release()
        
        # Wait for all of them to finish
        [x.join() for x in threads]

class ReduceTaskLauncher(Thread):
    def __init__(self, tasks, control, config, partitionQueue, delay):
        Thread.__init__(self)
        self.tasks = copy.deepcopy(tasks)
        self.num_tasks = len(self.tasks)
        self.control = control
        self.config = config
        self.partitionQueue = partitionQueue
        self.delay = delay
#        self.logger = SimulatorLogger(config.host).get()

    def run(self):
#        self.logger.info("Starting ReduceTaskLauncher")
        time.sleep(self.delay)
        
        threads = []
        self.control.cond.acquire()
        
        # Start first tasks to fill tasks slots 
        for i in xrange(self.config.reduceTaskSlots):
            if len(self.tasks) <= 0:
                break
            task = self.tasks.pop(0)
            t = ReduceTask(task, self.control, self.config.maxParallelTransfer, self.partitionQueue[task.name])
            t.start()
            threads.append(t)

        #TODO: for now, we dont support multiple waves of reduce tasks
        #start_time = time.time()
        #while self.control.finished_reduce_threads < self.num_tasks and (time.time() - start_time < TIMEOUT): 
        #    self.control.cond.wait(3)
        
        self.control.cond.release()
        
        # Wait for all of them to finish
        [x.join() for x in threads]

class ReduceListener(Thread):
    def __init__(self, tasks, control, config, partitionQueue):
        Thread.__init__(self)
        self.tasks = copy.deepcopy(tasks)
        self.num_tasks = len(self.tasks)
        self.control = control
        self.config = config
        self.partitionQueue = partitionQueue

    def run(self):
        print "ReduceListener: starting"
        s = socket.socket()
        host = self.config.host
        port = REDUCELISTENER_PORT
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(1024)
        
        partition_received = 0
        # TODO: deve receber de todos os mappers que enviam para esse node
        l = 0
        for t in self.tasks:
            l += len(t.partitions)
        #l = len([x for x in self.tasks[0].partitions if x.srcAddress == self.config.host])
        #l = len(self.tasks[0].partitions)
        start_time = time.time()
        while l > partition_received and (time.time() - start_time < TIMEOUT):
            print "ReduceListener: waiting... "
            c, addr = s.accept()
#            if DEBUG is True:
            print 'ReduceListener: got connection from', addr
            data = c.recv(1024)
            obj = pickle.loads(data)
            print 'ReduceListener: received from ' + str(addr) + " - " + str(obj)
#            if DEBUG is True:
#                print obj
            # TODO: a chave de ver o nome da tarefa reducer
            self.control.cond.acquire()
            self.partitionQueue[obj.reducer].append(obj)
            self.control.cond.release()
            data = pickle.dumps(obj, -1)
            c.send(data)
            c.close()
            partition_received += 1
            print "RECEBI %d PARTICIOES DE %d NO TOTAL" % (partition_received ,l)

class ShuffleServer(Thread):
    def __init__(self, host, numTransfers):
        Thread.__init__(self)
        self.host = host
#        self.logger = SimulatorLogger(host).get()
        self.numTransfers = numTransfers

    def run(self):
        print "ShuffleServer: starting server at %s:%d" % (self.host, SHUFFLESERVER_PORT)
        s = socket.socket()
        host = self.host
        port = SHUFFLESERVER_PORT
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen(1024)
        
        threads = []
        remaining_transfers = self.numTransfers

        print "ShuffleServer: remaing %d transfers from %d in total" % (remaining_transfers, self.numTransfers)

        while remaining_transfers > 0:
#        while True:
            c, addr = s.accept()
            print "ShuffleServer got connection from " + str(addr)
            data = c.recv(1024)
            partition = pickle.loads(data)
            #print "ShuffleServer: received from " + str(addr) + ": " + str(partition)
            iperf = MapOutputServlet(partition)
            iperf.start()
            threads.append(iperf)
            c.sendall("ok")
            remaining_transfers -= 1
            c.close()
            print "ShuffleServer: remaing %d transfers from %d in total" % (remaining_transfers, self.numTransfers)

        # Wait for all of them to finish
        [x.join() for x in threads]

class MapOutputServlet(Thread):
    def __init__(self, partition):
        Thread.__init__(self)
        self.partition = partition

    def run(self):
        print "MapOutputServlet: starting iperf client"
        size = self.partition.size * get_mininet_bw_ratio()
        #size = 64
        cmd = "./trafficgen/iperf-client %s %s %d %d" % (self.partition.srcAddress, self.partition.dstAddress, self.partition.dstPort, size)
        print "MapOutputServlet: run %s" % cmd
        p = Popen(cmd, shell=True)
        p.wait()
        print "MapOutputServlet: finished %s:%d" % (self.partition.dstAddress, self.partition.dstPort)

class Control():
    def __init__(self, logger):
        self.finished_map_threads = 0
        self.active_map_threads = 0
        self.finished_reduce_threads = 0
        self.active_reduce_threads = 0
        self.logger = logger
        self.cond = Condition(Lock())
        self.completedMaps = []
        self.completedReduces = []
        self.completedTransfers = []

    def start_map_task(self, task):
        print "Starting Thread-" + task.name
        task.startTime = time.time()
        self.cond.acquire()
        self.logger.task_start(task.name)
        self.active_map_threads += 1
        self.cond.release()

    def end_map_task(self, task):
        print "Finishing Thread-" + task.name
        task.finishTime = time.time()
        self.cond.acquire()
        self.logger.task_finish(task.name)
        self.active_map_threads -= 1
        self.finished_map_threads += 1
        self.completedMaps.append(copy.deepcopy(task))
        self.cond.notify()
        self.cond.release()

    def start_reduce_task(self, task):
        print "Starting Thread-" + task.name
        task.startTime = time.time()
        self.cond.acquire()
        self.logger.task_start(task.name)
        self.active_reduce_threads += 1
        self.cond.release()

    def end_reduce_task(self, task):
        print "Finishing Thread-" + task.name
        task.finishTime = time.time()
        self.cond.acquire()
        self.logger.task_finish(task.name)
        self.active_reduce_threads -= 1
        self.finished_reduce_threads += 1
        self.completedReduces.append(copy.deepcopy(task))
        self.cond.notify()
        self.cond.release()
    
    def end_shuffle(self, partition):
        self.cond.acquire()
        self.logger.shuffle_finish(partition.srcAddress, partition.srcPort, 
        partition.dstAddress, partition.dstPort, partition.size, partition.mapper,
        int(partition.duration*1000*1000*1000), partition.reducer)
        self.completedTransfers.append(copy.deepcopy(partition))
        self.cond.release()
        
