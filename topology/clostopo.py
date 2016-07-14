#!/usr/bin/python
# -*- coding: utf-8 -*-

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.node import RemoteController, Controller, OVSController, OVSKernelSwitch

import argparse
import sys
import time

class ClosTopo(Topo):

    "Single switch connected to n hosts."
    def build(self, fanout, cores):
        
        aggr = fanout * cores
        edge = aggr * fanout
        host = edge * fanout
        info('*** Adding switches\n') 
        
        core_switches = []
        print "Core ",
        for i in range(cores):  # Add core switches
            switch = self.addSwitch('c' + str(i + 1), cls=OVSKernelSwitch)
            core_switches.append(switch)
            print 'c' + str(i + 1),
        print 
        
        aggr_switches = []
        print "Aggregation ",
        for i in range(aggr):  # Add aggregation switches
            switch = self.addSwitch('a' + str(i + 1), cls=OVSKernelSwitch)
            aggr_switches.append(switch)
            print 'a' + str(i + 1),
        print 
        
        edge_switches = []
        print "Edge ", 
        for i in range(edge):  # Add edge switches
            switch = self.addSwitch('e' + str(i + 1), cls=OVSKernelSwitch)
            edge_switches.append(switch)
            print 'e' + str(i + 1),
        print
        
        for c in core_switches:  # Wiring up links between switches
            for a in aggr_switches:
                self.addLink(c, a)
        for a in aggr_switches:        
            for e in edge_switches:
                    self.addLink(a, e)
        
        "Set up Host level, Connection Edge - Host level "
        hosts = []
        h = 0
        for e in edge_switches:
            for i in range(2):
                host = self.addHost('h' + str(h + 1), ip=("10.0.0.%s" % str(h)))
                self.addLink(e, host)
                hosts.append(host)
                h += 1

def setup_clos_topo(fanout=2, cores=2):
    "Create and test a simple clos network"
    assert(fanout > 0)
    assert(cores > 0)
     
    net = Mininet(topo=ClosTopo(fanout, cores),
            autoSetMacs=True, link=TCLink, controller=None)
    
    info('*** Adding controller\n')
    c0 = net.addController(name='c0',
                      controller=RemoteController,
                      ip='127.0.0.1',
                      protocol='tcp',
                      port=6633)
    info( '*** Starting controller bro\n')
    for controller in net.controllers:
        controller.start()
    info('***Starting them switches bitch\n')
    for switch in net.switches:
        info( switch.name + ' ')
        switch.start([net.controllers[0]])
#     print "Dumping host connections"
    # dumpNodeConnections(net.hosts)
    # print "Testing network connectivity"
    # time.sleep(10)
    # net.pingAll()
    CLI(net)
#     # net.stop()

def main(argv):
    parser = argparse.ArgumentParser(description="Parse input information for mininet Clos network")
    parser.add_argument('--num_of_core_switches', '-c', dest='cores', type=int, help='number of core switches')
    parser.add_argument('--fanout', '-f', dest='fanout', type=int, help='network fanout')
    args = parser.parse_args(argv)
    setLogLevel('info')
    #setup_clos_topo(args.fanout, args.cores)
    setup_clos_topo(args.fanout, args.cores)

if __name__ == '__main__':
    # Tell mininet to print useful information
    main(sys.argv[1:])
