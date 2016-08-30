# Masters Dissertation

Topology - Flat tree

Based on [riplPOX](https://github.com/brandonheller/riplpox) and hedera implementation as described [here](https://reproducingnetworkresearch.wordpress.com/2015/05/31/cs244-15-hedera-flow-scheduling-draft/)
This is an implementation of the Hedera controller supporting Global First Fit from http://bnrg.cs.berkeley.edu/~randy/Courses/CS294.S13/7.3.pdf. 
This Hedera Controller is used to route realistic map reduce traffic using a Hadoop Emulator based on this [repository](https://github.com/mvneves/mremu).
It is built on top of Brandon Heller's Ripl library and POX controller with minor changes to both to support version consistency and Hedera functionality.

Use a CS 244 Mininet VM to run the code (either from the class website or an Amazon EC2 instance).

1. Switch to the CS 244 version of Mininet

    `$ cd ~/mininet`

    `$ git checkout -b cs244 origin/class/cs244`

2. Fix the module dependencies for this version

    `$ vim ~/mininet/mininet/moduledeps.py`

    (^change this line: "-OVS_KMOD = 'openvswitch_mod'"
                    to: "OVS_KMOD = 'openvswitch'")

3. Install the correct version

    `$ cd ~/mininet`

    `$ sudo make install`

4. Switch to the 'dart' branch of POX

    `$ cd ~/pox`

    `$ git checkout dart`

    `$ git pull origin dart`

5. Clone our project repo

    `$ cd ~`

    `$ git clone https://github.com/iwalsh/244proj.git`

    `cd 244proj/`

    `$ sudo python setup.py install`

6. Run it!

    `$ cd ~/244proj`

    `$ sudo ./run.sh`

7. Plot the results

    `$ cd ~/244proj`

    `$ python plot_results myAwesomePlot.png`

BONUS:

If you don't want to run the full measurement suite, you can run one measurement
at a time, like so:

`$ cd ~/244proj`

Terminal #1 - start the remote controller using ECMP flow scheduling

`$ ~/pox/pox.py controllers.riplpox --topo=ft,4 --routing=hashed --mode=reactive`

Terminal #2 - run our measurement script on a sample traffic pattern

`$ sudo python LaunchExperiment.py ecmp traffic/stride2.json`

Alternate Terminal #1 - start the Hedera controller using Global First-Fit flow scheduling

`~/pox/pox.py controllers.hederaController --topo=ft,4`

#### Troubleshooting Errors:

##### POX

*  Process already exists:

```$ sudo netstat -lpn |grep :6633```

```$ sudo kill <process-ID>```

#### Dependencies

```
sudo apt-get install bwm-ng iperf python-pip matplotlib networkx
sudo pip install IPy
```

### Make scripts executable

+ `$ chmod +x myscript.py`

#### Traces that were used for the experiment from mremu [repo](https://github.com/mvneves/mremu)

Changing the names of the traces for readibility

|Trace  |New Name|       |       
|---    |---     |---    |      
|job_201312301708_0016_trace|job1        |       
|job_201307220134_0002_trace|job2        |       
|job_201312301708_0002_trace|job3        |             
|job_201312301708_0010_trace|job4        |


#### Trace types

+ 8 Nodes = job_201312012205_0002_trace (2nd file), job_201312012250_0002_trace.json (3rd file), job_201312012307_0002_trace.json (4th file)

##### Traces with errors

+ job_201312301610_0002_trace.json (5th file), Mode: GFF + ECMP, Error `TypeError: 'NoneType' object is not iterable 
ERROR:core:Exception while handling OpenFlowNexus!PacketIn...`
+ job_201312301749_0010_trace (9th file) = no node `10.0.0.3` GFF mode - `  File "/usr/lib/python2.7/socket.py", line 224, in meth
    return getattr(self._sock,name)(*args)
error: [Errno 110] Connection timed out` 
+ job_201312301808_0016_trace (10th file) = runs indefinitely