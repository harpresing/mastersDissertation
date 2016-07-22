#!/usr/bin/python

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

# Mininet bandwidht ratio
mininet_bw = 1

def set_mininet_bw_ratio(ratio):
    global mininet_bw
    mininet_bw = ratio

def get_mininet_bw_ratio():
    global mininet_bw
    return mininet_bw

# Operation mode: Replay or Hadoop emulation 
REPLAY_MODE = 0
HADOOP_MODE = 1
operation_mode = HADOOP_MODE

def set_operation_mode(mode):
    global operation_mode
    operation_mode = mode

def get_operation_mode():
    global operation_mode
    print "get_operation_mode",operation_mode
    return operation_mode

