#!/bin/bash

sudo mn -c

kill_them_all()
{
	pids=$*
	#echo $pids
	if [ ! -z "$pids" ]
	then
		sudo kill -9 $pids
	fi
}

x=`ps auxf | grep emulator.py | grep mnexec | awk '{print $2}'`
kill_them_all $x

x=`ps auxf | grep emulator.py | grep python | awk '{print $2}'`
kill_them_all $x

x=`ps axuf | grep iperf | grep "/bin/sh" | awk '{print $2}'`
kill_them_all $x

x=`ps auxf | grep iperf | sed '/grep/d' | sed '/geany/d' | awk '{print $2}'`
kill_them_all $x

x=`ps axuf | grep bwm-ng | grep "/bin/sh" | awk '{print $2}'`
kill_them_all $x

x=`ps auxf | grep bwm-ng | sed '/grep/d' | awk '{print $2}'`
kill_them_all $x

x=`ps axuf | grep sshd | grep banner | sed '/grep/d' | awk '{print $2}'`
kill_them_all $x

x=`ps axuf | grep com.company.Main | sed '/grep/d' | awk '{print $2}'`
kill_them_all $x

rm ./hadoop/logs/hadoop-*.log -f
rm ./hadoop/logs/simulator-*.log -f
rm ./hadoop/tmp/* -f
rm -rf /tmp/iperf
mkdir -p /tmp/iperf
rm -rf ./output/*
