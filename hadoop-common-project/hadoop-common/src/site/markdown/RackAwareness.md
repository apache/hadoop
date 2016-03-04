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

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Rack Awareness
==============

Hadoop components are rack-aware. For example, HDFS block placement
will use rack awareness for fault tolerance by placing one block
replica on a different rack. This provides data availability in the
event of a network switch failure or partition within the cluster.

Hadoop master daemons obtain the rack id of the cluster slaves by
invoking either an external script or java class as specified by
configuration files. Using either the java class or external script
for topology, output must adhere to the java
**org.apache.hadoop.net.DNSToSwitchMapping** interface. The interface
expects a one-to-one correspondence to be maintained and the topology
information in the format of '/myrack/myhost', where '/' is the
topology delimiter, 'myrack' is the rack identifier, and 'myhost' is
the individual host. Assuming a single /24 subnet per rack, one could
use the format of '/192.168.100.0/192.168.100.5' as a unique rack-host
topology mapping.

To use the java class for topology mapping, the class name is
specified by the **net.topology.node.switch.mapping.impl** parameter
in the configuration file. An example, NetworkTopology.java, is
included with the hadoop distribution and can be customized by the
Hadoop administrator. Using a Java class instead of an external script
has a performance benefit in that Hadoop doesn't need to fork an
external process when a new slave node registers itself.

If implementing an external script, it will be specified with the
**net.topology.script.file.name** parameter in the configuration
files. Unlike the java class, the external topology script is not
included with the Hadoop distribution and is provided by the
administrator. Hadoop will send multiple IP addresses to ARGV when
forking the topology script. The number of IP addresses sent to the
topology script is controlled with **net.topology.script.number.args**
and defaults to 100. If **net.topology.script.number.args** was
changed to 1, a topology script would get forked for each IP submitted
by DataNodes and/or NodeManagers.

If **net.topology.script.file.name** or
**net.topology.node.switch.mapping.impl** is not set, the rack id
'/default-rack' is returned for any passed IP address. While this
behavior appears desirable, it can cause issues with HDFS block
replication as default behavior is to write one replicated block off
rack and is unable to do so as there is only a single rack named
'/default-rack'.

python Example
--------------
```python
#!/usr/bin/python
# this script makes assumptions about the physical environment.
#  1) each rack is its own layer 3 network with a /24 subnet, which
# could be typical where each rack has its own
#     switch with uplinks to a central core router.
#
#             +-----------+
#             |core router|
#             +-----------+
#            /             \
#   +-----------+        +-----------+
#   |rack switch|        |rack switch|
#   +-----------+        +-----------+
#   | data node |        | data node |
#   +-----------+        +-----------+
#   | data node |        | data node |
#   +-----------+        +-----------+
#
# 2) topology script gets list of IP's as input, calculates network address, and prints '/network_address/ip'.

import netaddr
import sys
sys.argv.pop(0)                                                  # discard name of topology script from argv list as we just want IP addresses

netmask = '255.255.255.0'                                        # set netmask to what's being used in your environment.  The example uses a /24

for ip in sys.argv:                                              # loop over list of datanode IP's
address = '{0}/{1}'.format(ip, netmask)                      # format address string so it looks like 'ip/netmask' to make netaddr work
try:
   network_address = netaddr.IPNetwork(address).network     # calculate and print network address
   print "/{0}".format(network_address)
except:
   print "/rack-unknown"                                    # print catch-all value if unable to calculate network address
```

bash Example
------------

```bash
#!/usr/bin/env bash
# Here's a bash example to show just how simple these scripts can be
# Assuming we have flat network with everything on a single switch, we can fake a rack topology.
# This could occur in a lab environment where we have limited nodes,like 2-8 physical machines on a unmanaged switch.
# This may also apply to multiple virtual machines running on the same physical hardware.
# The number of machines isn't important, but that we are trying to fake a network topology when there isn't one.
#
#       +----------+    +--------+
#       |jobtracker|    |datanode|
#       +----------+    +--------+
#              \        /
#  +--------+  +--------+  +--------+
#  |datanode|--| switch |--|datanode|
#  +--------+  +--------+  +--------+
#              /        \
#       +--------+    +--------+
#       |datanode|    |namenode|
#       +--------+    +--------+
#
# With this network topology, we are treating each host as a rack.  This is being done by taking the last octet
# in the datanode's IP and prepending it with the word '/rack-'.  The advantage for doing this is so HDFS
# can create its 'off-rack' block copy.
# 1) 'echo $@' will echo all ARGV values to xargs.
# 2) 'xargs' will enforce that we print a single argv value per line
# 3) 'awk' will split fields on dots and append the last field to the string '/rack-'. If awk
#    fails to split on four dots, it will still print '/rack-' last field value

echo $@ | xargs -n 1 | awk -F '.' '{print "/rack-"$NF}'
```

