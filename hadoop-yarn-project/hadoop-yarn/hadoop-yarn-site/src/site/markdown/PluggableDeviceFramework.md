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

# YARN Pluggable Device Framework

<!-- MACRO{toc|fromDepth=0|toDepth=2} -->

## Introduction

At present, YARN supports GPU/FPGA device through a native, coupling way.
But it's difficult for a vendor to implement such a device plugin
because the developer needs to understand various integration points with
YARN and also a deeper understanding YARN internals related to NodeManager.

### Pain Points Of Current Device Plugin

Some of the pain points for current device plugin development and integration
 are listed below:


* At least 6 classes to be implemented (If you wanna support
Docker, you’ll implement one more “DockerCommandPlugin”).
* When implementing the “ResourceHandler” interface,
the developer must understand the YARN NM internal concepts like container
launch mechanism, cgroups operations, docker runtime operations.
* If one wants isolation, the native container-executor also need a new module
written in C language.


This brings burdens to the community to maintain both YARN
core and vendor-specific code. For more details, check YARN-8851 design document.


Based on the above reasons and in order for YARN and vendor-specific plugin to
evolve independently, we developed a new pluggable device framework to ease
vendor device plugin development and provide a more flexible way to integrate with YARN.

## Quick Start

This pluggable device framework not only simplifies the plugin development but
also the number of configurations in YARN which are needed for plugin integration.
Before we go through how to implement
your own device plugin, let's first see how to use an existing plugin.


As an example, the new framework includes a sample implementation of Nvidia
GPU plugin supporting detecting Nvidia GPUs, the custom scheduler and isolating
containers run with both YARN cgroups and Nvidia Docker runtime v2.

### Prerequisites
1. The pluggable device framework depends on LinuxContainerExecutor to handle
resource isolation and Docker stuff. So LCE and Docker enabled on YARN is a
must.
See [Using CGroups with YARN](./NodeManagerCgroups.html) and [Docker on YARN](./DockerContainers.html)

2. The sample plugin `NvidiaGPUPluginForRuntimeV2` requires Nvidia GPU drivers
and Nvidia Docker runtime v2 installed in the nodes. See Nvidia official
documents for this.

3. If you use YARN capacity scheduler, below
`DominantResourceCalculator` configuration is needed (In `capacity-scheduler.xml`):
```
<property>
  <name>yarn.scheduler.capacity.resource-calculator</name>
  <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
</property>
```

### Enable Device Plugin Framework
Two properties to enable the pluggable framework support. First one is
in `yarn-site.xml`:
```
<property>
  <name>yarn.nodemanager.pluggable-device-framework.enabled</name>
  <value>true</value>
</property>
```
And then enable the isolation native module in `container-executor.cfg`:
```
# The configs below deal with settings for resource handled by pluggable device plugin framework
[devices]
  module.enabled=true
#  devices.denied-numbers=## Blacklisted devices not permitted to use. The format is comma separated "majorNumber:minorNumber". For instance, "195:1,195:2". Leave it empty means default devices reported by device plugin are all allowed.
```

### Configure Sample Nvidia GPU Plugin
The pluggable device framework loads one plugin and talks to it to know
which resource name the plugin is handling. And the resource name should be
pre-defined in `resource-types.xml`. Here we already know the resource name is
`nvidia.com/gpu` from the plugin implementation.
```
<property>
  <name>yarn.resource-types</name>
  <value>nvidia.com/gpu</value>
</property>
```
After define the resource name handled by the plugin. We can configure the
plugin name in `yarn-site.xml now:
```
<property>
  <name>yarn.nodemanager.pluggable-device-framework.device-classes</name>
  <value>org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.com.nvidia.NvidiaGPUPluginForRuntimeV2</value>
</property>
```
Note that the property value must be a full class name of the plugin.

### Restart YARN And Run Job
After restarting YARN, you should see the `nvidia.com/gpu` resource count displayed
 while accessing YARN UI2 Overview and NodeManages page or issuing command:
```
yarn node -list -showDetails
```

Then you can run job requesting several `nvidia.com/gpu` as usual:
```
yarn jar <path/to/hadoop-yarn-applications-distributedshell.jar> \
       -jar <path/to/hadoop-yarn-applications-distributedshell.jar> \
       -shell_env YARN_CONTAINER_RUNTIME_TYPE=docker \
       -shell_env YARN_CONTAINER_RUNTIME_DOCKER_IMAGE=<docker-image-name> \
       -shell_command nvidia-smi \
       -container_resources memory-mb=3072,vcores=1,nvidia.com/gpu=2 \
       -num_containers 2
```

### NM API To Query Resource Allocation
When a job run with resource like `nvidia.com/gpu`, you can query a NM node's
resource allocation through below RESTful API. Note that the resource name
should be URL encoded format (in this case, "nvidia.com%2Fgpu").
```
node:port/ws/v1/node/resources/nvidia.com%2Fgpu
```
For instance, use below command to get the JSON format resource allocation:
```
curl localhost:8042/ws/v1/node/resources/nvidia.com%2Fgpu | jq .
```

## Develop Your Own Plugin

Configure an existing plugin is easy. But how about implementing my own one?
It's easy too! See [Develop Device Plugin](./DevelopYourOwnDevicePlugin.html)