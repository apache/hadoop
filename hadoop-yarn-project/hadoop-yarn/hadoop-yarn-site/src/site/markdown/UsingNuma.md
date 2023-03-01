<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# NUMA

Non-uniform memory access (NUMA) is a computer memory design used in multiprocessing,
where the memory access time depends on the memory location relative to the processor.
Under NUMA, a processor can access its own local memory faster than non-local memory
(memory local to another processor or memory shared between processors).
Yarn Containers can make benefit of this NUMA design to get better performance by binding to a
specific NUMA node and all subsequent memory allocations will be served by the same node,
reducing remote memory accesses. NUMA support for YARN Container has to be enabled only if worker
node machines has NUMA support.

# Enabling NUMA

### Prerequisites

- As of now, NUMA awareness works only with `LinuxContainerExecutor` (LCE)
- To use the feature of NUMA awareness in the cluster,It must be enabled with
  LinuxContainerExecutor (LCE)
- Steps to enable SecureContainer (LCE) for cluster is documented [here](SecureContainer.md)

## Configurations

**1) Enable/Disable the NUMA awareness**

This property enables the NUMA awareness feature in the Node Manager
for the containers. By default, the value of this property is false which means it is disabled.
If this property is `true` then only the below configurations will be applicable otherwise they
will be ignored.

In `yarn-site.xml` add

```
  <property>
     <name>yarn.nodemanager.numa-awareness.enabled</name>
     <value>true</value>
  </property>
```

**2) NUMA topology**

This property decides whether to read the NUMA topology from the system or from the
configurations. If this property value is true then the topology will be read from the system using
`numactl --hardware` command in UNIX systems and similar way in windows.
If this property is false then the topology will be read using the below configurations.
Default value of this configuration is false which means NodeManager will read the NUMA topology
from the below configurations.

In `yarn-site.xml` add

```
    <property>
        <name>yarn.nodemanager.numa-awareness.read-topology</name>
        <value>false</value>
    </property>
```

**3) Numa command**

This property is passed when `yarn.nodemanager.numa-awareness.read-topology` is set to true.
It is recommended to verify the installation of `numactl` command in the Linux OS of every node.

Use `/usr/bin/numactl --hardware` to verify.
Sample output of `/usr/bin/numactl --hardware`

```
available: 2 nodes (0-1)
node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71
node 0 size: 191297 MB
node 0 free: 186539 MB
node 1 cpus: 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95
node 1 size: 191383 MB
node 1 free: 185914 MB
node distances:
node   0   1
  0:  10  21
  1:  21  10
```

In `yarn-site.xml` add

```
    <property>
         <name>yarn.nodemanager.numa-awareness.numactl.cmd</name>
         <value>/usr/bin/numactl</value>
    </property>
```

**4) NUMA nodes id’s**

This property is used to provide the NUMA node ids as comma separated values.It will be read only
when the `yarn.nodemanager.numa-awareness.read-topology` is false.

In ```yarn-site.xml``` add

```
    <property>
        <name>yarn.nodemanager.numa-awareness.node-ids</name>
        <value>0,1</value>
    </property>
```

**5) NUMA Node memory**

This property will be used to read the memory(in MB) configured for each NUMA node specified in
`yarn.nodemanager.numa-awareness.node-ids` by substituting the node id in the place of
`<NODE_ID>`.It will be read only when the `yarn.nodemanager.numa-awareness.read-topology`
is false.

In `yarn-site.xml` add

```
    <property>
        <name>yarn.nodemanager.numa-awareness.<NODE_ID>.memory</name>
        <value>191297</value>
    </property>
```

The value passed is the per node memory available , from the above sample output of
`numactl --hardware` the value passed for the property is the memory available i.e `191297`

**6) NUMA Node CPUs**

This property will be used to read the number of CPUs configured for each node specified in
`yarn.nodemanager.numa-awareness.node-ids` by substituting the node id in the place of
`<NODE_ID>`.It will be read only when the `yarn.nodemanager.numa-awareness.read-topology` is false.

In ```yarn-site.xml``` add

```
    <property>
        <name>yarn.nodemanager.numa-awareness.<NODE_ID>.cpus</name>
        <value>48</value>
    </property>
```

referring to the `numactl --hardware` output , number of cpu's in a node is `48`.

**7) Passing java_opts for map/reduce**

Every container has to be aware of NUMA and the JVM can be notified via passing NUMA flag.
Spark, Tez and other YARN Applications also need to set the container JVM Opts to leverage
NUMA Support.

In ```mapred-site.xml``` add

```
    <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-XX:+UseNUMA</value>
    </property>
    <property>
        <name>mapreduce.map.java.opts</name>
        <value>-XX:+UseNUMA</value>
    </property>
```

# Default configuration

| Property | Default value |
| --- |-----|
|yarn.nodemanager.numa-awareness.enabled|false|
|yarn.nodemanager.numa-awareness.read-topology|false|

# Enable numa balancing at OS Level (Optional)

In linux, by default numa balancing is by default off. For more performance improvement,
NumaBalancing can be turned on for all the nodes in cluster

```
echo 1 | sudo tee /proc/sys/kernel/numa_balancing
```

# Verify

**1) NodeManager log**

In any of the NodeManager, grep log file using below command

`grep "NUMA resources allocation is enabled," *`

Sample log with `LinuxContainerExecutor` enabled message

```
<nodemanager_ip>.log.2022-06-24-19.gz:2022-06-24 19:16:40,178 INFO org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.numa.NumaResourceHandlerImpl (main): NUMA resources allocation is enabled, initializing NUMA resources allocator.
```

**2) Container Log**

Grep the NodeManager log using below grep command to check if a container is assigned with NUMA node
resources.

`grep "NUMA node" | grep <container_id>`