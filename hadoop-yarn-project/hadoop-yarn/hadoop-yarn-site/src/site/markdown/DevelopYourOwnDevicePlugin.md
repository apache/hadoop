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

# Develop Your Own Plugin

A device plugin is loaded into the framework when
starting NM. Your plugin class only needs to consider two interfaces provided
by the framework. The `DevicePlugin` is a must to implement and the
`DevicePluginScheduler` is optional.

## DevicePlugin Interface

```
/**
 * A must interface for vendor plugin to implement.
 * */
public interface DevicePlugin {
  /**
   * Called first when device plugin framework wants to register.
   * @return DeviceRegisterRequest {@link DeviceRegisterRequest}
   * @throws Exception
   * */
  DeviceRegisterRequest getRegisterRequestInfo()
      throws Exception;

  /**
   * Called when update node resource.
   * @return a set of {@link Device}, {@link java.util.TreeSet} recommended
   * @throws Exception
   * */
  Set<Device> getDevices() throws Exception;

  /**
   * Asking how these devices should be prepared/used
   * before/when container launch. A plugin can do some tasks in its own or
   * define it in DeviceRuntimeSpec to let the framework do it.
   * For instance, define {@code VolumeSpec} to let the
   * framework to create volume before running container.
   *
   * @param allocatedDevices A set of allocated {@link Device}.
   * @param yarnRuntime Indicate which runtime YARN will use
   *        Could be {@code RUNTIME_DEFAULT} or {@code RUNTIME_DOCKER}
   *        in {@link DeviceRuntimeSpec} constants. The default means YARN's
   *        non-docker container runtime is used. The docker means YARN's
   *        docker container runtime is used.
   * @return a {@link DeviceRuntimeSpec} description about environment,
   * {@link         VolumeSpec}, {@link MountVolumeSpec}. etc
   * @throws Exception
   * */
  DeviceRuntimeSpec onDevicesAllocated(Set<Device>; allocatedDevices,
      YarnRuntimeType yarnRuntime) throws Exception;

  /**
   * Called after device released.
   * @param releasedDevices A set of released devices
   * @throws Exception
   * */
  void onDevicesReleased(Set<Device> releasedDevices)
      throws Exception;
}

```
The above code shows the `DevicePlugin` interface you need to implement.
Let’s go through the methods that a your plugin should implement.


* getRegisterRequestInfo(): DeviceRegisterRequest
* getDevices: Set&lt;Device&gt;
* onDevicesAllocated(Set&lt;Device&gt;, YarnRuntimeType yarnRuntime): DeviceRuntimeSpec
* onDeviceReleased(Set&lt;Device&gt;): void


The getRegisterRequestInfo interface is used for the plugin to advertise a
new resource type name and then the ResourceManager. The “DeviceRegisterRequest”
returned by the method consists a plugin version and a resource type name
like “nvidia.com/gpu”.


The getDevices interface is used to get latest vendor device list in this NM
node.
The resource count pre-defined in node-resources.xml will be overridden.
And it’s recommended that the vendor plugin manages allowed devices reported
to YARN in its own configuration. YARN can only have a blacklist
configuration `devices.denied-numbers` in `container-executor.cfg`.
In this method, you may invoke shell command or invoke RESTful/RPC to remote
service to get the devices at your convenience.


Please note that the `Device` object can describe a fake device. If the major
device number, minor device number and device path is left unset, the
framework won't do isolation for it. This provide feasibility for user to
define a fake device without real hardware.

The onDevicesAllocated interface is invoked to tell the framework how to use these devices.
The NM invoke this interface to let the plugin do some preparation work like create volume before container launch
and give hints on how to expose the devices to container when launch it. The
`DeviceRuntimeSpec` is the structure of the hints. For instance,
`DeviceRuntimeSpec` can describes the container launch requirements like
environment variables, device and volume mounts, Docker runtime type.etc.


The onDeviceReleased  interface is used for the plugin to do some cleanup work
after container finish.

## Optional DevicePluginScheduler Interface

```
/**
 * An optional interface to implement if custom device scheduling is needed.
 * If this is not implemented, the device framework will do scheduling.
 * */
public interface DevicePluginScheduler {
  /**
   * Called when allocating devices. The framework will do all device book
   * keeping and fail recovery. So this hook could be stateless and only do
   * scheduling based on available devices passed in. It could be
   * invoked multiple times by the framework. The hint in environment variables
   * passed in could be potentially used in making better scheduling decision.
   * For instance, GPU scheduling might support different kind of policy. The
   * container can set it through environment variables.
   * @param availableDevices Devices allowed to be chosen from.
   * @param count Number of device to be allocated.
   * @param env Environment variables of the container.
   * @return A set of {@link Device} allocated
   * */
  Set<Device> allocateDevices(Set<Device> availableDevices, int count,
      Map<String, String> env);
}
```
The above code shows the `DevicePluginScheduler` interface that you might
needed if you want to arm the plugin with a more efficient scheduler.
This `allocateDevices` method is invoked by YARN each time when asking the
plugin's recommendation devices for one container.
This interface is optional because YARN will provide a very basic scheduler.

You can refer to `NvidiaGPUPluginForRuntimeV2` plugin for a plugin customized
scheduler. Its scheduler is targeting for Nvidia GPU topology aware
scheduling and can get considerable performance boost for the container.

## Dependency in Plugin Project

When developing the plugin, you need to add below dependency property into
your projects's `pom.xml`. For instance,
```
<dependencies>
  <dependency>
    <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-yarn-server-nodemanager</artifactId>
      <version>3.3.0</version>
      <scope>provided</scope>
  </dependency>
</dependencies>
```

And after this, you can implement the above interfaces based on classes
provided in `org.apache.hadoop.yarn.server.nodemanager.api.deviceplugin`.
Please note that the plugin project is coupled with the Hadoop YARN NM version.

## Test And Use Your Own Plugin
Once you build your project and package a jar which contains your plugin
class and want to give it a try in your Hadoop cluster.


Firstly, put the jar file under a directory in Hadooop classpath.
(recommend $HADOOP_COMMOND_HOME/share/hadoop/yarn). Secondly,
follow the configurations described in [Pluggable Device Framework](./PluggableDeviceFramework.html) and restart YARN.