#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


from trace import Trace
from jobtracker import JobTracker
from tasktracker import TaskTracker
from config import set_mininet_bw_ratio, set_operation_mode, REPLAY_MODE, HADOOP_MODE

import json
import os
import sys
import time
import re

# Operation mode
mode = HADOOP_MODE

class Hadoop():
    def __init__(self, trace, conf, host):
        self.trace = trace
        self.conf = conf
        self.host = host
        ratio = float(conf["bandwidthMininet"])/float(conf["bandwidthTrace"])
        set_mininet_bw_ratio(ratio)

    def run(self):
        print "Hadoop: starting emulation"
        
        config = self.trace.getConfig(self.host)
        
        # start JobTracker
        if config.jobTracker == self.host:
            submitTime = time.time()
            jobTracker = JobTracker(self.trace.getJobName(), self.trace.getNumMaps(), self.trace.getNumReduces(), config, submitTime, self.trace)

        # start TaskTracker
        maps = self.trace.getMapTasksPerHost(self.host)
        reduces = self.trace.getReduceTasksPerHost(self.host)
        taskTracker = TaskTracker(self.trace, config, maps, reduces, self.trace.getNumTransfers(self.host))
        taskTracker.start()

        # start job
        if config.jobTracker == self.host:
            jobTracker.waitTaskTrackers()
            jobTracker.startJob()
                        
        # wait for the remote task Tracker to finish
        if config.jobTracker == self.host:
            jobTracker.waitTaskTrackers()

        # wait for the local task tracker to finish
        taskTracker.join()

        # finish job
        if config.jobTracker == self.host:
            jobTracker.finishJob()
            
            while True:
                if self.mergeFiles(self.conf["output"], self.trace.getJobName(), self.trace.getJobCompletionTime(), self.trace.getPostDelayJob()):
                    break
                time.sleep(0.5)

        print "Hadoop: emulation finished"


    def mergeFiles(self, jobOutputFile, name, traceTime, jobEndDelay):
        tmp_path= "./hadoop/tmp/"
        
        json_file = open(tmp_path + "job.json")
        output = json.load(json_file)
        json_file.close()

        output["tasks"] = []
        output["transfers"] = []
        for f in os.listdir(tmp_path):
            if re.match('.*.json', f) and f != "job.json":
                json_file = open(tmp_path + f)
                try:
                    data = json.load(json_file)
                except ValueError:
                    json_file.close()
                    return False
                json_file.close()
                output["tasks"] += data["tasks"]
                output["transfers"] += data["transfers"]

        lastFinishTime = sorted([x["finishTime"] for x in output["tasks"]], reverse=True)[0]
        output["finishTime"] = lastFinishTime + jobEndDelay

        outfile = open(jobOutputFile, 'w')
        json.dump(output, outfile,indent=4, sort_keys=True)
        outfile.close()

        result = {"name": name,
            "timeTrace": traceTime,
            "timeMininet": output["finishTime"] - output["startTime"]
        }
        outfile = open("./output/done.json", 'w')
        json.dump(result, outfile, indent=4, sort_keys=True)
        outfile.close()

        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "Usage: %s host\n" % (sys.argv[0])
        exit()

    host = sys.argv[1]
    # load emulation config file
    json_file = open("config.json")
    conf = json.load(json_file)
    json_file.close()
    print conf
    
    set_operation_mode(mode)

    # load trace file
    trace = Trace(conf["trace"], conf["mapping"], host)

    # start Hadoop emulation
    hadoop = Hadoop(trace, conf, host)
    hadoop.run()
