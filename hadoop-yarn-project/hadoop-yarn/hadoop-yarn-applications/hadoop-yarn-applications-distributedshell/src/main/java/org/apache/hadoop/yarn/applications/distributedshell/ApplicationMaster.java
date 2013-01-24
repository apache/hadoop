/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.distributedshell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;

import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.AMRMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 * 
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 * 
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link AMRMProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 * 
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManager} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link AMRMProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManager} by querying for the status of the allocated
 * container's {@link ContainerId}.
 *
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  // Configuration
  private Configuration conf;
  // YARN RPC to communicate with the Resource Manager or Node Manager
  private YarnRPC rpc;

  // Handle to communicate with the Resource Manager
  private AMRMClient resourceManager;

  // Application Attempt Id ( combination of attemptId and fail count )
  private ApplicationAttemptId appAttemptID;

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = 0;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  // App Master configuration
  // No. of containers to run shell command on
  private int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private int containerMemory = 10;
  // Priority of the request
  private int requestPriority;

  // Simple flag to denote whether all works is done
  private boolean appDone = false;
  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  private AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  private AtomicInteger numRequestedContainers = new AtomicInteger();

  // Shell command to be executed
  private String shellCommand = "";
  // Args to be passed to the shell command
  private String shellArgs = "";
  // Env variables to be setup for the shell command
  private Map<String, String> shellEnv = new HashMap<String, String>();

  // Location of shell script ( obtained from info set in env )
  // Shell script path in fs
  private String shellScriptPath = "";
  // Timestamp needed for creating a local resource
  private long shellScriptPathTimestamp = 0;
  // File length needed for local resource
  private long shellScriptPathLen = 0;

  // Hardcoded path to shell script in launch container's local env
  private final String ExecShellStringPath = "ExecShellScript.sh";

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  /**
   * @param args Command line args
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      ApplicationMaster appMaster = new ApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  /**
   * Dump out contents of $CWD and the environment to stdout for debugging
   */
  private void dumpOutDebugInfo() {

    LOG.info("Dump debug output");
    Map<String, String> envs = System.getenv();
    for (Map.Entry<String, String> env : envs.entrySet()) {
      LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
      System.out.println("System env: key=" + env.getKey() + ", val="
          + env.getValue());
    }

    String cmd = "ls -al";
    Runtime run = Runtime.getRuntime();
    Process pr = null;
    try {
      pr = run.exec(cmd);
      pr.waitFor();

      BufferedReader buf = new BufferedReader(new InputStreamReader(
          pr.getInputStream()));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
      buf.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public ApplicationMaster() throws Exception {
    // Set up the configuration and RPC
    conf = new YarnConfiguration();
    rpc = YarnRPC.create(conf);
  }

  /**
   * Parse command line options
   *
   * @param args Command line args
   * @return Whether init successful and run should be invoked
   * @throws ParseException
   * @throws IOException
   */
  public boolean init(String[] args) throws ParseException, IOException {

    Options opts = new Options();
    opts.addOption("app_attempt_id", true,
        "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption("shell_command", true,
        "Shell command to be executed by the Application Master");
    opts.addOption("shell_script", true,
        "Location of the shell script to be executed");
    opts.addOption("shell_args", true, "Command line args for the shell script");
    opts.addOption("shell_env", true,
        "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("num_containers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("debug", false, "Dump out debug information");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    if (envs.containsKey(ApplicationConstants.AM_APP_ATTEMPT_ID_ENV)) {
      appAttemptID = ConverterUtils.toApplicationAttemptId(envs
          .get(ApplicationConstants.AM_APP_ATTEMPT_ID_ENV));
    } else if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ConverterUtils.toContainerId(envs
          .get(ApplicationConstants.AM_CONTAINER_ID_ENV));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    if (!cliParser.hasOption("shell_command")) {
      throw new IllegalArgumentException(
          "No shell command specified to be executed by application master");
    }
    shellCommand = cliParser.getOptionValue("shell_command");

    if (cliParser.hasOption("shell_args")) {
      shellArgs = cliParser.getOptionValue("shell_args");
    }
    if (cliParser.hasOption("shell_env")) {
      String shellEnvs[] = cliParser.getOptionValues("shell_env");
      for (String env : shellEnvs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        shellEnv.put(key, val);
      }
    }

    if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION)) {
      shellScriptPath = envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
        shellScriptPathTimestamp = Long.valueOf(envs
            .get(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
      }
      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
        shellScriptPathLen = Long.valueOf(envs
            .get(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN));
      }

      if (!shellScriptPath.isEmpty()
          && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
        LOG.error("Illegal values in env for shell script path" + ", path="
            + shellScriptPath + ", len=" + shellScriptPathLen + ", timestamp="
            + shellScriptPathTimestamp);
        throw new IllegalArgumentException(
            "Illegal values in env for shell script path");
      }
    }

    containerMemory = Integer.parseInt(cliParser.getOptionValue(
        "container_memory", "10"));
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
        "num_containers", "1"));
    requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));

    return true;
  }

  /**
   * Helper function to print usage
   *
   * @param opts Parsed command line options
   */
  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("ApplicationMaster", opts);
  }

  /**
   * Main run function for the application master
   *
   * @throws YarnRemoteException
   */
  public boolean run() throws YarnRemoteException {
    LOG.info("Starting ApplicationMaster");

    // Connect to ResourceManager
    resourceManager = new AMRMClientImpl(appAttemptID);
    resourceManager.init(conf);
    resourceManager.start();

    try {
      // Setup local RPC Server to accept status requests directly from clients
      // TODO need to setup a protocol for client to be able to communicate to
      // the RPC server
      // TODO use the rpc port info to register with the RM for the client to
      // send requests to this app master

      // Register self with ResourceManager
      RegisterApplicationMasterResponse response = resourceManager
          .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
              appMasterTrackingUrl);
      // Dump out information about cluster capability as seen by the
      // resource manager
      int minMem = response.getMinimumResourceCapability().getMemory();
      int maxMem = response.getMaximumResourceCapability().getMemory();
      LOG.info("Min mem capabililty of resources in this cluster " + minMem);
      LOG.info("Max mem capabililty of resources in this cluster " + maxMem);

      // A resource ask has to be atleast the minimum of the capability of the
      // cluster, the value has to be a multiple of the min value and cannot
      // exceed the max.
      // If it is not an exact multiple of min, the RM will allocate to the
      // nearest multiple of min
      if (containerMemory < minMem) {
        LOG.info("Container memory specified below min threshold of cluster."
            + " Using min value." + ", specified=" + containerMemory + ", min="
            + minMem);
        containerMemory = minMem;
      } else if (containerMemory > maxMem) {
        LOG.info("Container memory specified above max threshold of cluster."
            + " Using max value." + ", specified=" + containerMemory + ", max="
            + maxMem);
        containerMemory = maxMem;
      }

      // Setup heartbeat emitter
      // TODO poll RM every now and then with an empty request to let RM know
      // that we are alive
      // The heartbeat interval after which an AM is timed out by the RM is
      // defined by a config setting:
      // RM_AM_EXPIRY_INTERVAL_MS with default defined by
      // DEFAULT_RM_AM_EXPIRY_INTERVAL_MS
      // The allocate calls to the RM count as heartbeats so, for now,
      // this additional heartbeat emitter is not required.

      // Setup ask for containers from RM
      // Send request for containers to RM
      // Until we get our fully allocated quota, we keep on polling RM for
      // containers
      // Keep looping until all the containers are launched and shell script
      // executed on them ( regardless of success/failure).

      int loopCounter = -1;

      while (numCompletedContainers.get() < numTotalContainers && !appDone) {
        loopCounter++;

        // log current state
        LOG.info("Current application state: loop=" + loopCounter
            + ", appDone=" + appDone + ", total=" + numTotalContainers
            + ", requested=" + numRequestedContainers + ", completed="
            + numCompletedContainers + ", failed=" + numFailedContainers
            + ", currentAllocated=" + numAllocatedContainers);

        // Sleep before each loop when asking RM for containers
        // to avoid flooding RM with spurious requests when it
        // need not have any available containers
        // Sleeping for 1000 ms.
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted " + e.getMessage());
        }

        // No. of containers to request
        // For the first loop, askCount will be equal to total containers needed
        // From that point on, askCount will always be 0 as current
        // implementation does not change its ask on container failures.
        int askCount = numTotalContainers - numRequestedContainers.get();
        numRequestedContainers.addAndGet(askCount);

        if (askCount > 0) {
          ContainerRequest containerAsk = setupContainerAskForRM(askCount);
          resourceManager.addContainerRequest(containerAsk);
        }

        // Send the request to RM
        LOG.info("Asking RM for containers" + ", askCount=" + askCount);
        AMResponse amResp = sendContainerAskToRM();

        // Retrieve list of allocated containers from the response
        List<Container> allocatedContainers = amResp.getAllocatedContainers();
        LOG.info("Got response from RM for container ask, allocatedCnt="
            + allocatedContainers.size());
        numAllocatedContainers.addAndGet(allocatedContainers.size());
        for (Container allocatedContainer : allocatedContainers) {
          LOG.info("Launching shell command on a new container."
              + ", containerId=" + allocatedContainer.getId()
              + ", containerNode=" + allocatedContainer.getNodeId().getHost()
              + ":" + allocatedContainer.getNodeId().getPort()
              + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
              + ", containerState" + allocatedContainer.getState()
              + ", containerResourceMemory"
              + allocatedContainer.getResource().getMemory());
          // + ", containerToken"
          // +allocatedContainer.getContainerToken().getIdentifier().toString());

          LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(
              allocatedContainer);
          Thread launchThread = new Thread(runnableLaunchContainer);

          // launch and start the container on a separate thread to keep
          // the main thread unblocked
          // as all containers may not be allocated at one go.
          launchThreads.add(launchThread);
          launchThread.start();
        }

        // Check what the current available resources in the cluster are
        // TODO should we do anything if the available resources are not enough?
        Resource availableResources = amResp.getAvailableResources();
        LOG.info("Current available resources in the cluster "
            + availableResources);

        // Check the completed containers
        List<ContainerStatus> completedContainers = amResp
            .getCompletedContainersStatuses();
        LOG.info("Got response from RM for container ask, completedCnt="
            + completedContainers.size());
        for (ContainerStatus containerStatus : completedContainers) {
          LOG.info("Got container status for containerID="
              + containerStatus.getContainerId() + ", state="
              + containerStatus.getState() + ", exitStatus="
              + containerStatus.getExitStatus() + ", diagnostics="
              + containerStatus.getDiagnostics());

          // non complete containers should not be here
          assert (containerStatus.getState() == ContainerState.COMPLETE);

          // increment counters for completed/failed containers
          int exitStatus = containerStatus.getExitStatus();
          if (0 != exitStatus) {
            // container failed
            if (-100 != exitStatus) {
              // shell script failed
              // counts as completed
              numCompletedContainers.incrementAndGet();
              numFailedContainers.incrementAndGet();
            } else {
              // something else bad happened
              // app job did not complete for some reason
              // we should re-try as the container was lost for some reason
              numAllocatedContainers.decrementAndGet();
              numRequestedContainers.decrementAndGet();
              // we do not need to release the container as it would be done
              // by the RM/CM.
            }
          } else {
            // nothing to do
            // container completed successfully
            numCompletedContainers.incrementAndGet();
            LOG.info("Container completed successfully." + ", containerId="
                + containerStatus.getContainerId());
          }
        }
        if (numCompletedContainers.get() == numTotalContainers) {
          appDone = true;
        }

        LOG.info("Current application state: loop=" + loopCounter
            + ", appDone=" + appDone + ", total=" + numTotalContainers
            + ", requested=" + numRequestedContainers + ", completed="
            + numCompletedContainers + ", failed=" + numFailedContainers
            + ", currentAllocated=" + numAllocatedContainers);

        // TODO
        // Add a timeout handling layer
        // for misbehaving shell commands
      }

      // Join all launched threads
      // needed for when we time out
      // and we need to release containers
      for (Thread launchThread : launchThreads) {
        try {
          launchThread.join(10000);
        } catch (InterruptedException e) {
          LOG.info("Exception thrown in thread join: " + e.getMessage());
          e.printStackTrace();
        }
      }

      // When the application completes, it should send a finish application
      // signal to the RM
      LOG.info("Application completed. Signalling finish to RM");

      FinalApplicationStatus appStatus;
      String appMessage = null;
      boolean isSuccess = true;
      if (numFailedContainers.get() == 0) {
        appStatus = FinalApplicationStatus.SUCCEEDED;
      } else {
        appStatus = FinalApplicationStatus.FAILED;
        appMessage = "Diagnostics." + ", total=" + numTotalContainers
            + ", completed=" + numCompletedContainers.get() + ", allocated="
            + numAllocatedContainers.get() + ", failed="
            + numFailedContainers.get();
        isSuccess = false;
      }
      resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
      return isSuccess;
    } finally {
      resourceManager.stop();
    }
  }

  /**
   * Thread to connect to the {@link ContainerManager} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    Container container;
    // Handle to communicate with ContainerManager
    ContainerManager cm;

    /**
     * @param lcontainer Allocated container
     */
    public LaunchContainerRunnable(Container lcontainer) {
      this.container = lcontainer;
    }

    /**
     * Helper function to connect to CM
     */
    private void connectToCM() {
      LOG.debug("Connecting to ContainerManager for containerid="
          + container.getId());
      String cmIpPortStr = container.getNodeId().getHost() + ":"
          + container.getNodeId().getPort();
      InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
      LOG.info("Connecting to ContainerManager at " + cmIpPortStr);
      this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class,
          cmAddress, conf));
    }

    @Override
    /**
     * Connects to CM, sets up container launch context 
     * for shell command and eventually dispatches the container 
     * start request to the CM. 
     */
    public void run() {
      // Connect to ContainerManager
      connectToCM();

      LOG.info("Setting up container launch container for containerid="
          + container.getId());
      ContainerLaunchContext ctx = Records
          .newRecord(ContainerLaunchContext.class);

      ctx.setContainerId(container.getId());
      ctx.setResource(container.getResource());

      String jobUserName = System.getenv(ApplicationConstants.Environment.USER
          .name());
      ctx.setUser(jobUserName);
      LOG.info("Setting user in ContainerLaunchContext to: " + jobUserName);

      // Set the environment
      ctx.setEnvironment(shellEnv);

      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      if (!shellScriptPath.isEmpty()) {
        LocalResource shellRsrc = Records.newRecord(LocalResource.class);
        shellRsrc.setType(LocalResourceType.FILE);
        shellRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
        try {
          shellRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
              shellScriptPath)));
        } catch (URISyntaxException e) {
          LOG.error("Error when trying to use shell script path specified"
              + " in env, path=" + shellScriptPath);
          e.printStackTrace();

          // A failure scenario on bad input such as invalid shell script path
          // We know we cannot continue launching the container
          // so we should release it.
          // TODO
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }
        shellRsrc.setTimestamp(shellScriptPathTimestamp);
        shellRsrc.setSize(shellScriptPathLen);
        localResources.put(ExecShellStringPath, shellRsrc);
      }
      ctx.setLocalResources(localResources);

      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);

      // Set executable command
      vargs.add(shellCommand);
      // Set shell script path
      if (!shellScriptPath.isEmpty()) {
        vargs.add(ExecShellStringPath);
      }

      // Set args for the shell command if any
      vargs.add(shellArgs);
      // Add log redirect params
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

      // Get final commmand
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }

      List<String> commands = new ArrayList<String>();
      commands.add(command.toString());
      ctx.setCommands(commands);

      StartContainerRequest startReq = Records
          .newRecord(StartContainerRequest.class);
      startReq.setContainerLaunchContext(ctx);
      try {
        cm.startContainer(startReq);
      } catch (YarnRemoteException e) {
        LOG.info("Start container failed for :" + ", containerId="
            + container.getId());
        e.printStackTrace();
        // TODO do we need to release this container?
      }

      // Get container status?
      // Left commented out as the shell scripts are short lived
      // and we are relying on the status for completed containers
      // from RM to detect status

      // GetContainerStatusRequest statusReq =
      // Records.newRecord(GetContainerStatusRequest.class);
      // statusReq.setContainerId(container.getId());
      // GetContainerStatusResponse statusResp;
      // try {
      // statusResp = cm.getContainerStatus(statusReq);
      // LOG.info("Container Status"
      // + ", id=" + container.getId()
      // + ", status=" +statusResp.getStatus());
      // } catch (YarnRemoteException e) {
      // e.printStackTrace();
      // }
    }
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @param numContainers Containers to ask for from RM
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM(int numContainers) {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);

    ContainerRequest request = new ContainerRequest(capability, null, null,
        pri, numContainers);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  /**
   * Ask RM to allocate given no. of containers to this Application Master
   *
   * @param requestedContainers Containers to ask for from RM
   * @return Response from RM to AM with allocated containers
   * @throws YarnRemoteException
   */
  private AMResponse sendContainerAskToRM() throws YarnRemoteException {
    float progressIndicator = (float) numCompletedContainers.get()
        / numTotalContainers;

    LOG.info("Sending request to RM for containers" + ", progress="
        + progressIndicator);

    AllocateResponse resp = resourceManager.allocate(progressIndicator);
    return resp.getAMResponse();
  }
}
