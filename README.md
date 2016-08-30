# Proactive Configuration of Data centre Networks for Big Data Processing

Topology - Flat tree

The SDN controller routes Hadoop traffic in three ways
+ ECMP - Based on [riplPOX](https://github.com/brandonheller/riplpox) 
+ Global First Fit - Based on flow scheduling algorim described [here](http://bnrg.cs.berkeley.edu/~randy/Courses/CS294.S13/7.3.pdf) 
+ Proactive controller as described in the dissertation

### Running the experiment

Use a CS 244 Mininet [VM](http://web.stanford.edu/class/cs244/vbsetup.htm) to run the code.

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

5. Install the controllers by chanding directory into this folder and run

    `$ sudo python setup.py install`
    
    `$ sudo ./run_ecmp.sh`

6. After ecmp is finished, run gff 
    
    `$ sudo ./cleanup.sh`
    
    `$ sudo ./run_gff.sh`

7. The flow rules are stored it `reactiveFlows.json`. After gff finishes executing, edit the `reactiveFlows.json` file by adding `[` in the beginning of the first line and remove `,` in the end of the last line, subsequenty add `]` instead, so that the file becomes valid JSON

8. Finally, run the proactive scheduler
    
    `$ sudo ./cleanup.sh`
    
    `$ sudo ./run_proactive.sh`


#### Dependencies

```
sudo apt-get install bwm-ng iperf python-pip matplotlib networkx
sudo pip install IPy
```

### Make scripts executable

+ `$ chmod +x myscript.sh`


### Quirks while running the experiment

We ran the experiment on an 8 core i7 processor with 16 GB or RAM. Still the Hadoop emulation would not finish on time and would keep running indefinately, due to IO bottlenecks. If this happens, restart your system.

#### Traces that were used for the experiment from mremu [repo](https://github.com/mvneves/mremu)

Changing the names of the traces for readibility

|Trace  |New Name|       |       
|---    |---     |---    |      
|job_201312301708_0016_trace|job1_trace        |       
|job_201307220134_0002_trace|job2_trace        |       
|job_201312301708_0002_trace|job3_trace        |             
|job_201312301708_0010_trace|job4_trace        |

### Extras
`gff_flow_rules` folder contains the flows obtained from gff routing, that were used by the proactive scheduler to install proactive flows, the readings from which, we evaluated its performance. 

