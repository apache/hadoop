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

Hadoop: Writing YARN Applications
=================================

* [Purpose](#Purpose)
* [Concepts and Flow](#Concepts_and_Flow)
* [Interfaces](#Interfaces)
* [Writing a Simple Yarn Application](#Writing_a_Simple_Yarn_Application)
    * [Writing a simple Client](#Writing_a_simple_Client)
    * [Writing an ApplicationMaster (AM)](#Writing_an_ApplicationMaster_AM)
* [FAQ](#FAQ)
    * [How can I distribute my application's jars to all of the nodes in the YARN cluster that need it?](#How_can_I_distribute_my_applications_jars_to_all_of_the_nodes_in_the_YARN_cluster_that_need_it)
    * [How do I get the ApplicationMaster's ApplicationAttemptId?](#How_do_I_get_the_ApplicationMasters_ApplicationAttemptId)
    * [Why my container is killed by the NodeManager?](#Why_my_container_is_killed_by_the_NodeManager)
    * [How do I include native libraries?](#How_do_I_include_native_libraries)
* [Useful Links](#Useful_Links)
* [Sample Code](#Sample_Code)

Purpose
-------

This document describes, at a high-level, the way to implement new Applications for YARN.

Concepts and Flow
-----------------

The general concept is that an *application submission client* submits an *application* to the YARN *ResourceManager* (RM). This can be done through setting up a `YarnClient` object. After `YarnClient` is started, the client can then set up application context, prepare the very first container of the application that contains the *ApplicationMaster* (AM), and then submit the application. You need to provide information such as the details about the local files/jars that need to be available for your application to run, the actual command that needs to be executed (with the necessary command line arguments), any OS environment settings (optional), etc. Effectively, you need to describe the Unix process(es) that needs to be launched for your ApplicationMaster.

The YARN ResourceManager will then launch the ApplicationMaster (as specified) on an allocated container. The ApplicationMaster communicates with YARN cluster, and handles application execution. It performs operations in an asynchronous fashion. During application launch time, the main tasks of the ApplicationMaster are: a) communicating with the ResourceManager to negotiate and allocate resources for future containers, and b) after container allocation, communicating YARN *NodeManager*s (NMs) to launch application containers on them. Task a) can be performed asynchronously through an `AMRMClientAsync` object, with event handling methods specified in a `AMRMClientAsync.CallbackHandler` type of event handler. The event handler needs to be set to the client explicitly. Task b) can be performed by launching a runnable object that then launches containers when there are containers allocated. As part of launching this container, the AM has to specify the `ContainerLaunchContext` that has the launch information such as command line specification, environment, etc.

During the execution of an application, the ApplicationMaster communicates NodeManagers through `NMClientAsync` object. All container events are handled by `NMClientAsync.CallbackHandler`, associated with `NMClientAsync`. A typical callback handler handles client start, stop, status update and error. ApplicationMaster also reports execution progress to ResourceManager by handling the `getProgress()` method of `AMRMClientAsync.CallbackHandler`.

Other than asynchronous clients, there are synchronous versions for certain workflows (`AMRMClient` and `NMClient`). The asynchronous clients are recommended because of (subjectively) simpler usages, and this article will mainly cover the asynchronous clients. Please refer to `AMRMClient` and `NMClient` for more information on synchronous clients.

Interfaces
----------

Following are the important interfaces:

* **Client**\<-\->**ResourceManager** 
    
    By using `YarnClient` objects.

* **ApplicationMaster**\<-\->**ResourceManager**

    By using `AMRMClientAsync` objects, handling events asynchronously by `AMRMClientAsync.CallbackHandler`

* **ApplicationMaster**\<-\->**NodeManager**

    Launch containers. Communicate with NodeManagers by using `NMClientAsync` objects, handling container events by `NMClientAsync.CallbackHandler`

**Note**

* The three main protocols for YARN application (ApplicationClientProtocol, ApplicationMasterProtocol and ContainerManagementProtocol) are still preserved. The 3 clients wrap these 3 protocols to provide simpler programming model for YARN applications.

* Under very rare circumstances, programmer may want to directly use the 3 protocols to implement an application. However, note that *such behaviors are no longer encouraged for general use cases*.

Writing a Simple Yarn Application
---------------------------------

### Writing a simple Client

* The first step that a client needs to do is to initialize and start a YarnClient.

          YarnClient yarnClient = YarnClient.createYarnClient();
          yarnClient.init(conf);
          yarnClient.start();

* Once a client is set up, the client needs to create an application, and get its application id.

          YarnClientApplication app = yarnClient.createApplication();
          GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

* The response from the `YarnClientApplication` for a new application also contains information about the cluster such as the minimum/maximum resource capabilities of the cluster. This is required so that to ensure that you can correctly set the specifications of the container in which the ApplicationMaster would be launched. Please refer to `GetNewApplicationResponse` for more details.

* The main crux of a client is to setup the `ApplicationSubmissionContext` which defines all the information needed by the RM to launch the AM. A client needs to set the following into the context:

  * Application info: id, name

  * Queue, priority info: Queue to which the application will be submitted, the priority to be assigned for the application.

  * User: The user submitting the application

  * `ContainerLaunchContext`: The information defining the container in which the AM will be launched and run. The `ContainerLaunchContext`, as mentioned previously, defines all the required information needed to run the application such as the local **R**esources (binaries, jars, files etc.), **E**nvironment settings (CLASSPATH etc.), the **C**ommand to be executed and security **T**okens (*RECT*).

```java
// set the application submission context
ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
ApplicationId appId = appContext.getApplicationId();

appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
appContext.setApplicationName(appName);

// set local resources for the application master
// local files or archives as needed
// In this scenario, the jar file for the application master is part of the local resources
Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

LOG.info("Copy App Master jar from local filesystem and add to local environment");
// Copy the application master jar to the filesystem
// Create a local resource to point to the destination jar path
FileSystem fs = FileSystem.get(conf);
addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(),
    localResources, null);

// Set the log4j properties if needed
if (!log4jPropFile.isEmpty()) {
  addToLocalResources(fs, log4jPropFile, log4jPath, appId.toString(),
      localResources, null);
}

// The shell script has to be made available on the final container(s)
// where it will be executed.
// To do this, we need to first copy into the filesystem that is visible
// to the yarn framework.
// We do not need to set this as a local resource for the application
// master as the application master does not need it.
String hdfsShellScriptLocation = "";
long hdfsShellScriptLen = 0;
long hdfsShellScriptTimestamp = 0;
if (!shellScriptPath.isEmpty()) {
  Path shellSrc = new Path(shellScriptPath);
  String shellPathSuffix =
      appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
  Path shellDst =
      new Path(fs.getHomeDirectory(), shellPathSuffix);
  fs.copyFromLocalFile(false, true, shellSrc, shellDst);
  hdfsShellScriptLocation = shellDst.toUri().toString();
  FileStatus shellFileStatus = fs.getFileStatus(shellDst);
  hdfsShellScriptLen = shellFileStatus.getLen();
  hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
}

if (!shellCommand.isEmpty()) {
  addToLocalResources(fs, null, shellCommandPath, appId.toString(),
      localResources, shellCommand);
}

if (shellArgs.length > 0) {
  addToLocalResources(fs, null, shellArgsPath, appId.toString(),
      localResources, StringUtils.join(shellArgs, " "));
}

// Set the env variables to be setup in the env where the application master will be run
LOG.info("Set the environment for the application master");
Map<String, String> env = new HashMap<String, String>();

// put location of shell script into env
// using the env info, the application master will create the correct local resource for the
// eventual containers that will be launched to execute the shell scripts
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION, hdfsShellScriptLocation);
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
env.put(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN, Long.toString(hdfsShellScriptLen));

// Add AppMaster.jar location to classpath
// At some point we should not be required to add
// the hadoop specific classpaths to the env.
// It should be provided out of the box.
// For now setting all required classpaths including
// the classpath to "." for the application jar
StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
  .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
for (String c : conf.getStrings(
    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
  classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
  classPathEnv.append(c.trim());
}
classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append(
  "./log4j.properties");

// Set the necessary command to execute the application master
Vector<CharSequence> vargs = new Vector<CharSequence>(30);

// Set java executable command
LOG.info("Setting up app master command");
vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
// Set Xmx based on am memory size
vargs.add("-Xmx" + amMemory + "m");
// Set class name
vargs.add(appMasterMainClass);
// Set params for Application Master
vargs.add("--container_memory " + String.valueOf(containerMemory));
vargs.add("--container_vcores " + String.valueOf(containerVirtualCores));
vargs.add("--num_containers " + String.valueOf(numContainers));
vargs.add("--priority " + String.valueOf(shellCmdPriority));

for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
  vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
}
if (debugFlag) {
  vargs.add("--debug");
}

vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

// Get final command
StringBuilder command = new StringBuilder();
for (CharSequence str : vargs) {
  command.append(str).append(" ");
}

LOG.info("Completed setting up app master command " + command.toString());
List<String> commands = new ArrayList<String>();
commands.add(command.toString());

// Set up the container launch context for the application master
ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(
  localResources, env, commands, null, null, null);

// Set up resource type requirements
// For now, both memory and vcores are supported, so we set memory and
// vcores requirements
Resource capability = Resource.newInstance(amMemory, amVCores);
appContext.setResource(capability);

// Service data is a binary blob that can be passed to the application
// Not needed in this scenario
// amContainer.setServiceData(serviceData);

// Setup security tokens
if (UserGroupInformation.isSecurityEnabled()) {
  // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
  Credentials credentials = new Credentials();
  String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
  if (tokenRenewer == null | | tokenRenewer.length() == 0) {
    throw new IOException(
      "Can't get Master Kerberos principal for the RM to use as renewer");
  }

  // For now, only getting tokens for the default file-system.
  final Token<?> tokens[] =
      fs.addDelegationTokens(tokenRenewer, credentials);
  if (tokens != null) {
    for (Token<?> token : tokens) {
      LOG.info("Got dt for " + fs.getUri() + "; " + token);
    }
  }
  DataOutputBuffer dob = new DataOutputBuffer();
  credentials.writeTokenStorageToStream(dob);
  ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  amContainer.setTokens(fsTokens);
}

appContext.setAMContainerSpec(amContainer);
```

* After the setup process is complete, the client is ready to submit the application with specified priority and queue.

```java
// Set the priority for the application master
Priority pri = Priority.newInstance(amPriority);
appContext.setPriority(pri);

// Set the queue to which this application is to be submitted in the RM
appContext.setQueue(amQueue);

// Submit the application to the applications manager
// SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);

yarnClient.submitApplication(appContext);
```

* At this point, the RM will have accepted the application and in the background, will go through the process of allocating a container with the required specifications and then eventually setting up and launching the AM on the allocated container.

* There are multiple ways a client can track progress of the actual task.
   
> * It can communicate with the RM and request for a report of the application via the `getApplicationReport()` method of `YarnClient`.

```java
// Get application report for the appId we are interested in
ApplicationReport report = yarnClient.getApplicationReport(appId);
```
  
> The ApplicationReport received from the RM consists of the following:

>> * *General application information*: Application id, queue to which the application was submitted, user who submitted the application and the start time for the application.

>> * *ApplicationMaster details*: the host on which the AM is running, the rpc port (if any) on which it is listening for requests from clients and a token that the client needs to communicate with the AM.

>> * *Application tracking information*: If the application supports some form of progress tracking, it can set a tracking url which is available via `ApplicationReport`'s `getTrackingUrl()` method that a client can look at to monitor progress.

>> * *Application status*: The state of the application as seen by the ResourceManager is available via `ApplicationReport#getYarnApplicationState`. If the `YarnApplicationState` is set to `FINISHED`, the client should refer to `ApplicationReport#getFinalApplicationStatus` to check for the actual success/failure of the application task itself. In case of failures, `ApplicationReport#getDiagnostics` may be useful to shed some more light on the the failure.

> * If the ApplicationMaster supports it, a client can directly query the AM itself for progress updates via the host:rpcport information obtained from the application report. It can also use the tracking url obtained from the report if available.

* In certain situations, if the application is taking too long or due to other factors, the client may wish to kill the application. `YarnClient` supports the `killApplication` call that allows a client to send a kill signal to the AM via the ResourceManager. An ApplicationMaster if so designed may also support an abort call via its rpc layer that a client may be able to leverage.

          yarnClient.killApplication(appId);

### Writing an ApplicationMaster (AM)

* The AM is the actual owner of the job. It will be launched by the RM and via the client will be provided all the necessary information and resources about the job that it has been tasked with to oversee and complete.

* As the AM is launched within a container that may (likely will) be sharing a physical host with other containers, given the multi-tenancy nature, amongst other issues, it cannot make any assumptions of things like pre-configured ports that it can listen on.

* When the AM starts up, several parameters are made available to it via the environment. These include the `ContainerId` for the AM container, the application submission time and details about the NM (NodeManager) host running the ApplicationMaster. Ref `ApplicationConstants` for parameter names.

* All interactions with the RM require an `ApplicationAttemptId` (there can be multiple attempts per application in case of failures). The `ApplicationAttemptId` can be obtained from the AM's container id. There are helper APIs to convert the value obtained from the environment into objects.

```java
Map<String, String> envs = System.getenv();
String containerIdString =
    envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV);
if (containerIdString == null) {
  // container id should always be set in the env by the framework
  throw new IllegalArgumentException(
      "ContainerId not set in the environment");
}
ContainerId containerId = ConverterUtils.toContainerId(containerIdString);
ApplicationAttemptId appAttemptID = containerId.getApplicationAttemptId();
```

* After an AM has initialized itself completely, we can start the two clients: one to ResourceManager, and one to NodeManagers. We set them up with our customized event handler, and we will talk about those event handlers in detail later in this article.

```java
  AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
  amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
  amRMClient.init(conf);
  amRMClient.start();

  containerListener = createNMCallbackHandler();
  nmClientAsync = new NMClientAsyncImpl(containerListener);
  nmClientAsync.init(conf);
  nmClientAsync.start();
```

* The AM has to emit heartbeats to the RM to keep it informed that the AM is alive and still running. The timeout expiry interval at the RM is defined by a config setting accessible via `YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS` with the default being defined by `YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS`. The ApplicationMaster needs to register itself with the ResourceManager to start heartbeating.

```java
// Register self with ResourceManager
// This will start heartbeating to the RM
appMasterHostname = NetUtils.getHostname();
RegisterApplicationMasterResponse response = amRMClient
    .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
        appMasterTrackingUrl);
```

* In the response of the registration, maximum resource capability if included. You may want to use this to check the application's request.

```java
// Dump out information about cluster capability as seen by the
// resource manager
int maxMem = response.getMaximumResourceCapability().getMemory();
LOG.info("Max mem capability of resources in this cluster " + maxMem);

int maxVCores = response.getMaximumResourceCapability().getVirtualCores();
LOG.info("Max vcores capability of resources in this cluster " + maxVCores);

// A resource ask cannot exceed the max.
if (containerMemory > maxMem) {
  LOG.info("Container memory specified above max threshold of cluster."
      + " Using max value." + ", specified=" + containerMemory + ", max="
      + maxMem);
  containerMemory = maxMem;
}

if (containerVirtualCores > maxVCores) {
  LOG.info("Container virtual cores specified above max threshold of  cluster."
    + " Using max value." + ", specified=" + containerVirtualCores + ", max="
    + maxVCores);
  containerVirtualCores = maxVCores;
}
List<Container> previousAMRunningContainers =
    response.getContainersFromPreviousAttempts();
LOG.info("Received " + previousAMRunningContainers.size()
        + " previous AM's running containers on AM registration.");
```

* Based on the task requirements, the AM can ask for a set of containers to run its tasks on. We can now calculate how many containers we need, and request those many containers.

```java
List<Container> previousAMRunningContainers =
    response.getContainersFromPreviousAttempts();
LOG.info("Received " + previousAMRunningContainers.size()
    + " previous AM's running containers on AM registration.");

int numTotalContainersToRequest =
    numTotalContainers - previousAMRunningContainers.size();
// Setup ask for containers from RM
// Send request for containers to RM
// Until we get our fully allocated quota, we keep on polling RM for
// containers
// Keep looping until all the containers are launched and shell script
// executed on them ( regardless of success/failure).
for (int i = 0; i < numTotalContainersToRequest; ++i) {
  ContainerRequest containerAsk = setupContainerAskForRM();
  amRMClient.addContainerRequest(containerAsk);
}
```

* In `setupContainerAskForRM()`, the follow two things need some set up:

> * Resource capability: Currently, YARN supports memory based resource requirements so the request should define how much memory is needed. The value is defined in MB and has to less than the max capability of the cluster and an exact multiple of the min capability. Memory resources correspond to physical memory limits imposed on the task containers. It will also support computation based resource (vCore), as shown in the code.

> * Priority: When asking for sets of containers, an AM may define different priorities to each set. For example, the Map-Reduce AM may assign a higher priority to containers needed for the Map tasks and a lower priority for the Reduce tasks' containers.

```java
private ContainerRequest setupContainerAskForRM() {
  // setup requirements for hosts
  // using * as any host will do for the distributed shell app
  // set the priority for the request
  Priority pri = Priority.newInstance(requestPriority);

  // Set up resource type requirements
  // For now, memory and CPU are supported so we set memory and cpu requirements
  Resource capability = Resource.newInstance(containerMemory,
    containerVirtualCores);

  ContainerRequest request = new ContainerRequest(capability, null, null,
      pri);
  LOG.info("Requested container ask: " + request.toString());
  return request;
}
```

* After container allocation requests have been sent by the application manager, contailers will be launched asynchronously, by the event handler of the `AMRMClientAsync` client. The handler should implement `AMRMClientAsync.CallbackHandler` interface.

> * When there are containers allocated, the handler sets up a thread that runs the code to launch containers. Here we use the name `LaunchContainerRunnable` to demonstrate. We will talk about the `LaunchContainerRunnable` class in the following part of this article.

```java
@Override
public void onContainersAllocated(List<Container> allocatedContainers) {
  LOG.info("Got response from RM for container ask, allocatedCnt="
      + allocatedContainers.size());
  numAllocatedContainers.addAndGet(allocatedContainers.size());
  for (Container allocatedContainer : allocatedContainers) {
    LaunchContainerRunnable runnableLaunchContainer =
        new LaunchContainerRunnable(allocatedContainer, containerListener);
    Thread launchThread = new Thread(runnableLaunchContainer);

    // launch and start the container on a separate thread to keep
    // the main thread unblocked
    // as all containers may not be allocated at one go.
    launchThreads.add(launchThread);
    launchThread.start();
  }
}
```

> * On heart beat, the event handler reports the progress of the application.

```java
@Override
public float getProgress() {
  // set progress to deliver to RM on next heartbeat
  float progress = (float) numCompletedContainers.get()
      / numTotalContainers;
  return progress;
}
```

* The container launch thread actually launches the containers on NMs. After a container has been allocated to the AM, it needs to follow a similar process that the client followed in setting up the `ContainerLaunchContext` for the eventual task that is going to be running on the allocated Container. Once the `ContainerLaunchContext` is defined, the AM can start it through the `NMClientAsync`.

```java
// Set the necessary command to execute on the allocated container
Vector<CharSequence> vargs = new Vector<CharSequence>(5);

// Set executable command
vargs.add(shellCommand);
// Set shell script path
if (!scriptPath.isEmpty()) {
  vargs.add(Shell.WINDOWS ? ExecBatScripStringtPath
    : ExecShellStringPath);
}

// Set args for the shell command if any
vargs.add(shellArgs);
// Add log redirect params
vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

// Get final command
StringBuilder command = new StringBuilder();
for (CharSequence str : vargs) {
  command.append(str).append(" ");
}

List<String> commands = new ArrayList<String>();
commands.add(command.toString());

// Set up ContainerLaunchContext, setting local resource, environment,
// command and token for constructor.

// Note for tokens: Set up tokens for the container too. Today, for normal
// shell commands, the container in distribute-shell doesn't need any
// tokens. We are populating them mainly for NodeManagers to be able to
// download anyfiles in the distributed file-system. The tokens are
// otherwise also useful in cases, for e.g., when one is running a
// "hadoop dfs" command inside the distributed shell.
ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
  localResources, shellEnv, commands, null, allTokens.duplicate(), null);
containerListener.addContainer(container.getId(), container);
nmClientAsync.startContainerAsync(container, ctx);
```

* The `NMClientAsync` object, together with its event handler, handles container events. Including container start, stop, status update, and occurs an error.

* After the ApplicationMaster determines the work is done, it needs to unregister itself through the AM-RM client, and then stops the client.

```java
try {
  amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
} catch (YarnException ex) {
  LOG.error("Failed to unregister application", ex);
} catch (IOException e) {
  LOG.error("Failed to unregister application", e);
}

amRMClient.stop();
```

FAQ
---

### How can I distribute my application's jars to all of the nodes in the YARN cluster that need it?

You can use the LocalResource to add resources to your application request. This will cause YARN to distribute the resource to the ApplicationMaster node. If the resource is a tgz, zip, or jar - you can have YARN unzip it. Then, all you need to do is add the unzipped folder to your classpath. For example, when creating your application request:

```java
File packageFile = new File(packagePath);
URL packageUrl = ConverterUtils.getYarnUrlFromPath(
    FileContext.getFileContext().makeQualified(new Path(packagePath)));

packageResource.setResource(packageUrl);
packageResource.setSize(packageFile.length());
packageResource.setTimestamp(packageFile.lastModified());
packageResource.setType(LocalResourceType.ARCHIVE);
packageResource.setVisibility(LocalResourceVisibility.APPLICATION);

resource.setMemory(memory);
containerCtx.setResource(resource);
containerCtx.setCommands(ImmutableList.of(
    "java -cp './package/*' some.class.to.Run "
    + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
    + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
containerCtx.setLocalResources(
    Collections.singletonMap("package", packageResource));
appCtx.setApplicationId(appId);
appCtx.setUser(user.getShortUserName);
appCtx.setAMContainerSpec(containerCtx);
yarnClient.submitApplication(appCtx);
```

  As you can see, the `setLocalResources` command takes a map of names to resources. The name becomes a sym link in your application's cwd, so you can just refer to the artifacts inside by using ./package/\*.

  **Note**: Java's classpath (cp) argument is VERY sensitive. Make sure you get the syntax EXACTLY correct.

  Once your package is distributed to your AM, you'll need to follow the same process whenever your AM starts a new container (assuming you want the resources to be sent to your container). The code for this is the same. You just need to make sure that you give your AM the package path (either HDFS, or local), so that it can send the resource URL along with the container ctx.

### How do I get the ApplicationMaster's `ApplicationAttemptId`?

The `ApplicationAttemptId` will be passed to the AM via the environment and the value from the environment can be converted into an `ApplicationAttemptId` object via the ConverterUtils helper function.

### Why my container is killed by the NodeManager?

This is likely due to high memory usage exceeding your requested container memory size. There are a number of reasons that can cause this. First, look at the process tree that the NodeManager dumps when it kills your container. The two things you're interested in are physical memory and virtual memory. If you have exceeded physical memory limits your app is using too much physical memory. If you're running a Java app, you can use -hprof to look at what is taking up space in the heap. If you have exceeded virtual memory, you may need to increase the value of the the cluster-wide configuration variable `yarn.nodemanager.vmem-pmem-ratio`.

### How do I include native libraries?

Setting `-Djava.library.path` on the command line while launching a container can cause native libraries used by Hadoop to not be loaded correctly and can result in errors. It is cleaner to use `LD_LIBRARY_PATH` instead.

Useful Links
------------

* [YARN Architecture](./YARN.html)

* [YARN Capacity Scheduler](./CapacityScheduler.html)

* [YARN Fair Scheduler](./FairScheduler.html)

Sample Code
-----------

Yarn distributed shell: in `hadoop-yarn-applications-distributedshell` project after you set up your development environment.
