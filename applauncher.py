from subprocess import Popen, PIPE
from argparse import ArgumentParser
import multiprocessing
from time import sleep
from monitor.monitor import monitor_devs_ng, monitor_cpu
import os
import sys

def HadoopTest(hosts):
	basedir = "./hadoop"
	confdir = basedir + "/conf"
	done = "./output/done.json"
	emulator = basedir + "/emulator.py"

	if os.path.isfile(done):
		os.remove(done)

	f = open(confdir + "/slaves", 'w')
	for h in hosts:
		f.write(h.IP() + "\n")
	f.close()

	f = open(confdir + "/masters", 'w')
	f.write(hosts[0].IP() + "\n")
	f.close()

	print "Starting monitor ..."
	output_dir="output"
	monitors = []

	# Start network usage monitor
	monitor = multiprocessing.Process(target = monitor_devs_ng, args =
                ('%s/rate.txt' % output_dir, 0.01))
	monitor.start()
	monitors.append(monitor)

	# Start CPU usage monitor
	monitor = multiprocessing.Process(target=monitor_cpu, args=('%s/cpu.txt' % output_dir,))
	monitor.start()
	monitors.append(monitor)

	print "Running Hadoop simulation ..."
	for h in hosts:
		h.popen('%s %s > ./output/output-%s.txt 2> ./output/error-%s.txt' % (emulator, h.IP(), h.IP(), h.IP()), shell = True)
	while True:
		if os.path.isfile(done):
			break
		sleep(1)

	print "Stopping monitor ..."
	for monitor in monitors:
		monitor.terminate()
	os.system("killall -9 bwm-ng")
	os.system("killall -9 top")

	sleep(5)
	print "Done."

def IperfTest(hosts):
	basedir = "./benchmarks"
	done = "./output/done.json"
	emulator = basedir + "/emulator.py"

	if os.path.isfile(done):
		os.remove(done)

	print "Starting monitor ..."
	output_dir="output"
	monitor = multiprocessing.Process(target = monitor_devs_ng, args =
                ('%s/rate.txt' % output_dir, 0.1))
	monitor.start()

	print "Running tests ..."
	for h in hosts:
		h.popen('%s %s > ./output/output-%s.txt 2> ./output/error-%s.txt' % (emulator, h.IP(), h.IP(), h.IP()), shell = True)
	while True:
		if os.path.isfile(done):
			break
		sleep(1)

	print "Stopping monitor ..."
	monitor.terminate()
        os.system("killall -9 bwm-ng")

	sleep(5)
	print "Done."

