# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

from comm import SHUFFLESERVER_PORT
from config import get_operation_mode, REPLAY_MODE, HADOOP_MODE
from threading import Thread, Condition, Lock
import time
import sys
import traceback
import socket
import cPickle as pickle
from subprocess import Popen
import random

TIMEOUT = 3600

IPERF_PORT = 7770

class ReduceTask(Thread):
    """
    A reduce task.
    """

    def __init__(self, task, control, maxParallelTransfer, queue):
        Thread.__init__(self)
        self.task = task
        self.name = "Thread-%s" % task.name
        self.control = control
        self.queue = queue
        self.copierControl = CopierControl(control)
        self.num_partitions = len(self.task.partitions)
        self.maxParallelTransfer = maxParallelTransfer
        self.maxInFlight = 4 * self.maxParallelTransfer
        self.numEventsFetched = 0

    def run(self):
        print "ReduceTask: starting (%s)" % self.task.name
        self.control.start_reduce_task(self.task)
    
        # Sleep to simulate the initial delay
        time.sleep(self.task.initialDelay)
        self.task.waitFinished = time.time()

        mode = get_operation_mode()
        if mode == REPLAY_MODE:
            # Replay execution from original traces
            self.scheduleTransfersFromTraces()
        else:
            # Hadoop Emulation
            self.fetchOutpus()

        time.sleep(self.task.mergingTime)
        self.task.shuffleFinished = time.time()

        # Sleep to simulate the tasks exectution time
        time.sleep(self.task.sortingTime)
        self.task.sortFinished = time.time()
        time.sleep(self.task.processingTime)

        self.control.end_reduce_task(self.task)

        print "ReduceTask: finished (%s)" % self.task.name

    def fetchOutpus(self):
        threads = []
        self.copierControl.cond.acquire()

        remaining_partitions = self.num_partitions
        free_slots = self.maxParallelTransfer
        start_time = time.time()
        numScheduled = 0
        mapList = []

        for x in range(0, self.maxParallelTransfer):
            copier = MapOutputCopier(self.num_partitions, self.copierControl)
            copier.start()
            threads.append(copier)

        numInFlight = 0
        self.mapLocations = []
        uniqueHosts = []
        numEventsAtStartOfScheduling = 0

        while remaining_partitions > 0 and (time.time() - start_time < TIMEOUT):

            numDups = 0
            numScheduled = 0

            self.numEventsFetched = self.copierControl.getMapCompletionEvents(self.queue, self.mapLocations, self.numEventsFetched)
            numEventsAtStartOfScheduling = self.numEventsFetched

            # get host list
            hostList = list(set([p.srcAddress for p in self.mapLocations]))

            # randomize host list
            # Hadoop does it to prevent all reduce-tasks swamping the same tasktracker.
            random.shuffle(hostList)

            for host in hostList:
                knownOutputsByLoc = [p for p in self.mapLocations if p.srcAddress == host]

                if not knownOutputsByLoc:
                    continue

                if host in uniqueHosts:
                    numDups += len(knownOutputsByLoc)
                    continue

                for loc in knownOutputsByLoc:
                    uniqueHosts.append(host);
                    self.copierControl.scheduledCopies.append(loc)
                    self.copierControl.numScheduled += 1
                    self.mapLocations.remove(loc);
                    numInFlight += 1
                    numScheduled += 1
                    remaining_partitions -= 1
                    break; # we have a map from this host

            self.copierControl.cond.notifyAll()
            if numScheduled > 0:
                #LOG.info(reduceTask.getTaskID() + " Scheduled " + numScheduled + " outputs (" + penaltyBox.size() + " slow hosts and" + numDups + " dup hosts)");
                print " Scheduled", numScheduled, "outputs ( 0 slow hosts and", numDups, "dup hosts)"

            while numInFlight > 0:
                cr = self.getCopyResult(numInFlight, numEventsAtStartOfScheduling)
                if not cr:
                    # we should go and get more mapcompletion events from the tasktracker
                    break
                uniqueHosts.remove(cr.srcAddress)
                numInFlight -= 1

        self.copierControl.cond.release()

        # Wait for all of them to finish
        [x.join() for x in threads]

    def getCopyResult(self, numInFlight, numEventsAtStartOfScheduling):
        waitedForNewEvents = False

        while not self.copierControl.copyResults:
            self.numEventsFetched = self.copierControl.getMapCompletionEvents(self.queue, self.mapLocations, self.numEventsFetched)
            if self.busyEnough(numInFlight):
                # All of the fetcher threads are busy. So, no sense trying
                # to schedule more until one finishes.
                self.copierControl.cond.wait(2)
            elif self.numEventsFetched == numEventsAtStartOfScheduling and not waitedForNewEvents:
                # no sense trying to schedule more, since there are no
                # new events to even try to schedule.
                waitedForNewEvents = True
                self.copierControl.cond.wait(2)
            else:
                return None

        return self.copierControl.copyResults.pop(0)

    def busyEnough(self, numInFlight):
        return numInFlight > self.maxInFlight

    def scheduleTransfersFromTraces(self):
        threads = []
        self.copierControl.cond.acquire()

        mapLocations = {}
        remaining_partitions = self.num_partitions
        free_slots = self.maxParallelTransfer
        start_time = time.time()
        numScheduled = 0
        numEventsFetched = 0
        mapList = []

        for x in range(0, self.maxParallelTransfer):
            copier = MapOutputCopier(self.num_partitions, self.copierControl)
            copier.start()
            threads.append(copier)

        while remaining_partitions > 0 and (time.time() - start_time < TIMEOUT):

            self.copierControl.cond.release()
            if numEventsFetched < self.num_partitions:
                numEventsFetched = self.copierControl.get_pending_partitions_trace(self.task, mapList, numEventsFetched, start_time)
            self.copierControl.cond.acquire()

            for partition in mapList:
                print "Recebendo " + str(partition)
                self.copierControl.scheduledCopies.append(partition)
                self.copierControl.numScheduled += 1
                remaining_partitions -= 1
                self.copierControl.cond.notifyAll()
                print "COPIEI %d PARTICOES DE %d NO TOTAL" % (self.num_partitions-remaining_partitions,self.num_partitions)
            mapList = []

        self.copierControl.cond.release()

        # Wait for all of them to finish
        [x.join() for x in threads]

class MapOutputCopier(Thread):
    """
    Copy a partition from the map output data of a given map task.
    """

    def __init__(self, numCopies, copierControl):
        Thread.__init__(self)
        self.numCopies = numCopies
        self.copierControl = copierControl

    def run(self):
        start_time = time.time()

        while time.time() - start_time < TIMEOUT:

            print "MapOutputCopier: waiting for new scheduled copies"
            self.copierControl.cond.acquire()
            try:
                partition = self.copierControl.scheduledCopies.pop(0)
            except:
                if self.copierControl.numScheduled >= self.numCopies:
                    self.copierControl.cond.release()
                    break
                self.copierControl.cond.wait()
                self.copierControl.cond.release()
                continue
            self.copierControl.cond.release()

            print "MapOutputCopier: calling copyOutput()"
            self.copyOutput(partition)

            self.copierControl.cond.acquire()
            self.copierControl.copyResults.append(partition)
            self.copierControl.cond.notifyAll()
            self.copierControl.cond.release()

    def copyOutput(self, partition):
        self.copierControl.start_copy()

        partition.startTime = time.time()
        print "MapOutputCopier: copying partition - %s" % str(partition)
        print "Sleeping for", partition.initTime, " before starting to send"
        mode = get_operation_mode()
        if mode == HADOOP_MODE:
            time.sleep(partition.initTime)

        self.copierControl.control.cond.acquire()
        global IPERF_PORT
        self.port = IPERF_PORT
        IPERF_PORT += 1
        self.copierControl.control.cond.release()
        partition.dstPort = self.port
                
        print "MapOutputCopier: starting iperf server at %s:%d" % (partition.dstAddress, partition.dstPort)
        cmd = "./trafficgen/iperf-server %s %d" % (partition.dstAddress, partition.dstPort)
        print "MapOutputCopier: run %s" % cmd
        p = Popen(cmd, shell=True)
        
        ok = False
        while not ok:
            ok = self.contactShuffleServer(partition)
            if not ok:
                time.sleep(0.01)

        # Wait for the shuffle transfer to finish
        p.wait()
        
        if mode == HADOOP_MODE:
            time.sleep(partition.postTime);

        partition.finishTime = time.time()
        partition.duration = partition.finishTime - partition.startTime
        self.copierControl.control.end_shuffle(partition)

        print "MapOutputCopier: finished %s:%d" % (partition.dstAddress, partition.dstPort)
        self.copierControl.end_copy()



    def contactShuffleServer(self, partition):
        """Contact the shuffle server informing Do some things.
        :param verbose: Be verbose (give additional messages).
        """
        try:
            s = socket.socket()
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            host = partition.srcAddress
            port = SHUFFLESERVER_PORT
            s.connect((host, port))
            #print "MapOutputCopier(" + name + ") got error connetion to " + host + ":" + str(port)
            data = pickle.dumps(partition, -1)
            s.sendall(data)
            #self.logger.info("MapOutputCopier(" + partition.reducer + ") send to " + host + ": " + str(partition))
            data = s.recv(1024)
            s.close()
            return True
        except:
            print traceback.print_exc(None, sys.stderr)
            return False

class CopierControl():
    def __init__(self, control):
        self.finished_copier_threads = 0
        self.active_copier_threads = 0
        #self.logger = logger
        self.cond = Condition(Lock())
        self.control = control
        self.numScheduled = 0
        self.scheduledCopies = []
        self.copyResults = []

    def start_copy(self):
        #print "Starting Thread-" + task.name
        self.cond.acquire()
        #self.logger.task_start(task.name)
        self.active_copier_threads += 1
        self.cond.notify()
        self.cond.release()

    def end_copy(self):
        #print "Finishing Thread-" + task.name
        self.cond.acquire()
        #self.logger.task_finish(task.name)
        self.active_copier_threads -= 1
        self.finished_copier_threads += 1
        self.cond.notify()
        self.cond.release()

    def get_pending_partitions_trace(self, task, mapList, numEventsFetched, start_time):
        elapsedTime = time.time()-start_time
        try:
            nextArrival = task.partitions[numEventsFetched].eventArrival
        except:
            return numEventsFetched

        if nextArrival > elapsedTime:
            diff = nextArrival - elapsedTime
            print "EVENT_SLEEP",diff,nextArrival,elapsedTime,numEventsFetched
            time.sleep(diff)
            elapsedTime = time.time()-start_time
        else:
            print "EVENT_NOSLEEP",nextArrival,elapsedTime,numEventsFetched

        for i in range(numEventsFetched, len(task.partitions)):
            p = task.partitions[i]
            if p.eventArrival > elapsedTime:
                break
            mapList.append(p)
            numEventsFetched += 1
            print "EVENT_ARRIVAL: ",p.mapper,p.eventArrival,elapsedTime
        return numEventsFetched

    def getMapCompletionEvents(self, queue, mapLocations, numEventsFetched):
        while queue:
            p = queue.pop(0)
            mapLocations.append(p)
            numEventsFetched += 1
        return numEventsFetched
