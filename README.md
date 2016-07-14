# Masters Dissertation

Topology - Flat tree

Use POX with the following commands:

```
$ sudo ~/pox/pox.py forwarding.l2_learning \
  openflow.spanning_tree --no-flood --hold-down \
  log.level --DEBUG samples.pretty_log \
  openflow.discovery host_tracker \
  info.packet_dump 
```

Detailed description can be found [here](http://www.brianlinkletter.com/using-pox-components-to-create-a-software-defined-networking-application/#fn2-3488) 

Clos Topo - The number of core switches (tree roots) and the fanout (number of child switches per parent
switch) should be treated as configurable parameters