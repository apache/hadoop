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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ProfileCapability;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntityGroupId;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.TimelineServiceHelper;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jersey.api.client.ClientHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * status/job history if needed. However, in the distributedshell, trackingurl
 * and appMasterHost:appMasterRpcPort are not supported.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
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
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
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

  private static final Logger LOG = LoggerFactory
      .getLogger(ApplicationMaster.class);

  @VisibleForTesting
  @Private
  public enum DSEvent {
    DS_APP_ATTEMPT_START, DS_APP_ATTEMPT_END, DS_CONTAINER_START, DS_CONTAINER_END
  }
  
  @VisibleForTesting
  @Private
  public enum DSEntity {
    DS_APP_ATTEMPT, DS_CONTAINER
  }

  private static final String YARN_SHELL_ID = "YARN_SHELL_ID";

  // Configuration
  private Configuration conf;

  // Handle to communicate with the Resource Manager
  @SuppressWarnings("rawtypes")
  private AMRMClientAsync amRMClient;

  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;

  // Handle to communicate with the Node Manager
  private NMClientAsync nmClientAsync;
  // Listen to process the response from the Node Manager
  private NMCallbackHandler containerListener;

  // Application Attempt Id ( combination of attemptId and fail count )
  @VisibleForTesting
  protected ApplicationAttemptId appAttemptID;

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  private boolean timelineServiceV2Enabled = false;

  private boolean timelineServiceV1Enabled = false;

  // App Master configuration
  // No. of containers to run shell command on
  @VisibleForTesting
  protected int numTotalContainers = 1;
  // Memory to request for the container on which the shell command will run
  private static final long DEFAULT_CONTAINER_MEMORY = 10;
  private long containerMemory = DEFAULT_CONTAINER_MEMORY;
  // VirtualCores to request for the container on which the shell command will run
  private static final int DEFAULT_CONTAINER_VCORES = 1;
  private int containerVirtualCores = DEFAULT_CONTAINER_VCORES;
  // Priority of the request
  private int requestPriority;

  // Resource profile for the container
  private String containerResourceProfile = "";
  Map<String, Resource> resourceProfiles;

  // Counter for completed containers ( complete denotes successful or failed )
  private AtomicInteger numCompletedContainers = new AtomicInteger();
  // Allocated container count so that we know how many containers has the RM
  // allocated to us
  @VisibleForTesting
  protected AtomicInteger numAllocatedContainers = new AtomicInteger();
  // Count of failed containers
  private AtomicInteger numFailedContainers = new AtomicInteger();
  // Count of containers already requested from the RM
  // Needed as once requested, we should not request for containers again.
  // Only request for more if the original requirement changes.
  @VisibleForTesting
  protected AtomicInteger numRequestedContainers = new AtomicInteger();

  // Shell command to be executed
  private String shellCommand = "";
  // Args to be passed to the shell command
  private String shellArgs = "";
  // Env variables to be setup for the shell command
  private Map<String, String> shellEnv = new HashMap<String, String>();

  // Location of shell script ( obtained from info set in env )
  // Shell script path in fs
  private String scriptPath = "";
  // Timestamp needed for creating a local resource
  private long shellScriptPathTimestamp = 0;
  // File length needed for local resource
  private long shellScriptPathLen = 0;

  // Container retry options
  private ContainerRetryPolicy containerRetryPolicy =
      ContainerRetryPolicy.NEVER_RETRY;
  private Set<Integer> containerRetryErrorCodes = null;
  private int containerMaxRetries = 0;
  private int containrRetryInterval = 0;

  // Timeline domain ID
  private String domainId = null;

  // Hardcoded path to shell script in launch container's local env
  private static final String EXEC_SHELL_STRING_PATH = Client.SCRIPT_PATH
      + ".sh";
  private static final String EXEC_BAT_SCRIPT_STRING_PATH = Client.SCRIPT_PATH
      + ".bat";

  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  private static final String shellCommandPath = "shellCommands";
  private static final String shellArgsPath = "shellArgs";

  private volatile boolean done;

  private ByteBuffer allTokens;

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  // Timeline Client
  @VisibleForTesting
  TimelineClient timelineClient;

  // Timeline v2 Client
  private TimelineV2Client timelineV2Client;

  static final String CONTAINER_ENTITY_GROUP_ID = "CONTAINERS";
  static final String APPID_TIMELINE_FILTER_NAME = "appId";
  static final String USER_TIMELINE_FILTER_NAME = "user";

  private final String linux_bash_command = "bash";
  private final String windows_command = "cmd /c";

  private int yarnShellIdCounter = 1;

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
      Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  /**
   * Container start times used to set id prefix while publishing entity
   * to ATSv2.
   */
  private final ConcurrentMap<ContainerId, Long> containerStartTimes =
      new ConcurrentHashMap<ContainerId, Long>();

  private ConcurrentMap<ContainerId, Long> getContainerStartTimes() {
    return containerStartTimes;
  }

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
      appMaster.run();
      result = appMaster.finish();
    } catch (Throwable t) {
      LOG.error("Error running ApplicationMaster", t);
      LogManager.shutdown();
      ExitUtil.terminate(1, t);
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

    BufferedReader buf = null;
    try {
      String lines = Shell.WINDOWS ? Shell.execCommand("cmd", "/c", "dir") :
        Shell.execCommand("ls", "-al");
      buf = new BufferedReader(new StringReader(lines));
      String line = "";
      while ((line = buf.readLine()) != null) {
        LOG.info("System CWD content: " + line);
        System.out.println("System CWD content: " + line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.cleanupWithLogger(LOG, buf);
    }
  }

  public ApplicationMaster() {
    // Set up the configuration
    conf = new YarnConfiguration();
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
    opts.addOption("shell_env", true,
        "Environment for shell script. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true,
        "Amount of memory in MB to be requested to run the shell command");
    opts.addOption("container_vcores", true,
        "Amount of virtual cores to be requested to run the shell command");
    opts.addOption("container_resource_profile", true,
        "Resource profile to be requested to run the shell command");
    opts.addOption("num_containers", true,
        "No. of containers on which the shell command needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");
    opts.addOption("container_retry_policy", true,
        "Retry policy when container fails to run, "
            + "0: NEVER_RETRY, 1: RETRY_ON_ALL_ERRORS, "
            + "2: RETRY_ON_SPECIFIC_ERROR_CODES");
    opts.addOption("container_retry_error_codes", true,
        "When retry policy is set to RETRY_ON_SPECIFIC_ERROR_CODES, error "
            + "codes is specified with this option, "
            + "e.g. --container_retry_error_codes 1,2,3");
    opts.addOption("container_max_retries", true,
        "If container could retry, it specifies max retires");
    opts.addOption("container_retry_interval", true,
        "Interval between each retry, unit is milliseconds");
    opts.addOption("debug", false, "Dump out debug information");

    opts.addOption("help", false, "Print usage");
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
          "No args specified for application master to initialize");
    }

    //Check whether customer log4j.properties file exists
    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
            log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    if (cliParser.hasOption("help")) {
      printUsage(opts);
      return false;
    }

    if (cliParser.hasOption("debug")) {
      dumpOutDebugInfo();
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption("app_attempt_id")) {
        String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
        appAttemptID = ApplicationAttemptId.fromString(appIdStr);
      } else {
        throw new IllegalArgumentException(
            "Application Attempt Id not set in the environment");
      }
    } else {
      ContainerId containerId = ContainerId.fromString(envs
          .get(Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
    }

    if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
      throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HOST.name())) {
      throw new RuntimeException(Environment.NM_HOST.name()
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
      throw new RuntimeException(Environment.NM_HTTP_PORT
          + " not set in the environment");
    }
    if (!envs.containsKey(Environment.NM_PORT.name())) {
      throw new RuntimeException(Environment.NM_PORT.name()
          + " not set in the environment");
    }

    LOG.info("Application master for app" + ", appId="
        + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
        + appAttemptID.getApplicationId().getClusterTimestamp()
        + ", attemptId=" + appAttemptID.getAttemptId());

    if (!fileExist(shellCommandPath)
        && envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION).isEmpty()) {
      throw new IllegalArgumentException(
          "No shell command or shell script specified to be executed by application master");
    }

    if (fileExist(shellCommandPath)) {
      shellCommand = readContent(shellCommandPath);
    }

    if (fileExist(shellArgsPath)) {
      shellArgs = readContent(shellArgsPath);
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
      scriptPath = envs.get(DSConstants.DISTRIBUTEDSHELLSCRIPTLOCATION);

      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP)) {
        shellScriptPathTimestamp = Long.parseLong(envs
            .get(DSConstants.DISTRIBUTEDSHELLSCRIPTTIMESTAMP));
      }
      if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN)) {
        shellScriptPathLen = Long.parseLong(envs
            .get(DSConstants.DISTRIBUTEDSHELLSCRIPTLEN));
      }
      if (!scriptPath.isEmpty()
          && (shellScriptPathTimestamp <= 0 || shellScriptPathLen <= 0)) {
        LOG.error("Illegal values in env for shell script path" + ", path="
            + scriptPath + ", len=" + shellScriptPathLen + ", timestamp="
            + shellScriptPathTimestamp);
        throw new IllegalArgumentException(
            "Illegal values in env for shell script path");
      }
    }

    if (envs.containsKey(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN)) {
      domainId = envs.get(DSConstants.DISTRIBUTEDSHELLTIMELINEDOMAIN);
    }

    containerMemory = Integer.parseInt(cliParser.getOptionValue(
        "container_memory", "-1"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
        "container_vcores", "-1"));
    containerResourceProfile =
        cliParser.getOptionValue("container_resource_profile", "");
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
        "num_containers", "1"));
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException(
          "Cannot run distributed shell with no containers");
    }
    requestPriority = Integer.parseInt(cliParser
        .getOptionValue("priority", "0"));

    containerRetryPolicy = ContainerRetryPolicy.values()[
        Integer.parseInt(cliParser.getOptionValue(
            "container_retry_policy", "0"))];
    if (cliParser.hasOption("container_retry_error_codes")) {
      containerRetryErrorCodes = new HashSet<>();
      for (String errorCode :
          cliParser.getOptionValue("container_retry_error_codes").split(",")) {
        containerRetryErrorCodes.add(Integer.parseInt(errorCode));
      }
    }
    containerMaxRetries = Integer.parseInt(
        cliParser.getOptionValue("container_max_retries", "0"));
    containrRetryInterval = Integer.parseInt(cliParser.getOptionValue(
        "container_retry_interval", "0"));

    if (YarnConfiguration.timelineServiceEnabled(conf)) {
      timelineServiceV2Enabled =
          ((int) YarnConfiguration.getTimelineServiceVersion(conf) == 2);
      timelineServiceV1Enabled = !timelineServiceV2Enabled;
    } else {
      timelineClient = null;
      timelineV2Client = null;
      LOG.warn("Timeline service is not enabled");
    }

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
   * @throws YarnException
   * @throws IOException
   */
  @SuppressWarnings({ "unchecked" })
  public void run() throws YarnException, IOException, InterruptedException {
    LOG.info("Starting ApplicationMaster");

    // Note: Credentials, Token, UserGroupInformation, DataOutputBuffer class
    // are marked as LimitedPrivate
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    // Now remove the AM->RM token so that containers cannot access it.
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    LOG.info("Executing with tokens:");
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token.toString());
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());

    // Create appSubmitterUgi and add original tokens to it
    String appSubmitterUserName =
        System.getenv(ApplicationConstants.Environment.USER.name());
    appSubmitterUgi =
        UserGroupInformation.createRemoteUser(appSubmitterUserName);
    appSubmitterUgi.addCredentials(credentials);

    AMRMClientAsync.AbstractCallbackHandler allocListener =
        new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    startTimelineClient(conf);
    if (timelineServiceV2Enabled) {
      // need to bind timelineClient
      amRMClient.registerTimelineV2Client(timelineV2Client);
    }

    if (timelineServiceV2Enabled) {
      publishApplicationAttemptEventOnTimelineServiceV2(
          DSEvent.DS_APP_ATTEMPT_START);
    } else if (timelineServiceV1Enabled) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          DSEvent.DS_APP_ATTEMPT_START, domainId, appSubmitterUgi);
    }

    // Setup local RPC Server to accept status requests directly from clients
    // TODO need to setup a protocol for client to be able to communicate to
    // the RPC server
    // TODO use the rpc port info to register with the RM for the client to
    // send requests to this app master

    // Register self with ResourceManager
    // This will start heartbeating to the RM
    appMasterHostname = NetUtils.getHostname();
    RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
            appMasterTrackingUrl);
    resourceProfiles = response.getResourceProfiles();
    // Dump out information about cluster capability as seen by the
    // resource manager
    long maxMem = response.getMaximumResourceCapability().getMemorySize();
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
      LOG.info("Container virtual cores specified above max threshold of cluster."
          + " Using max value." + ", specified=" + containerVirtualCores + ", max="
          + maxVCores);
      containerVirtualCores = maxVCores;
    }

    List<Container> previousAMRunningContainers =
        response.getContainersFromPreviousAttempts();
    LOG.info(appAttemptID + " received " + previousAMRunningContainers.size()
      + " previous attempts' running containers on AM registration.");
    for(Container container: previousAMRunningContainers) {
      launchedContainers.add(container.getId());
    }
    numAllocatedContainers.addAndGet(previousAMRunningContainers.size());


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
    numRequestedContainers.set(numTotalContainers);
  }

  @VisibleForTesting
  void startTimelineClient(final Configuration conf)
      throws YarnException, IOException, InterruptedException {
    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
          if (YarnConfiguration.timelineServiceEnabled(conf)) {
            // Creating the Timeline Client
            if (timelineServiceV2Enabled) {
              timelineV2Client = TimelineV2Client.createTimelineClient(
                  appAttemptID.getApplicationId());
              timelineV2Client.init(conf);
              timelineV2Client.start();
              LOG.info("Timeline service V2 client is enabled");
            } else {
              timelineClient = TimelineClient.createTimelineClient();
              timelineClient.init(conf);
              timelineClient.start();
              LOG.info("Timeline service V1 client is enabled");
            }
          } else {
            timelineClient = null;
            timelineV2Client = null;
            LOG.warn("Timeline service is not enabled");
          }
          return null;
        }
      });
    } catch (UndeclaredThrowableException e) {
      throw new YarnException(e.getCause());
    }
  }

  @VisibleForTesting
  NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler(this);
  }

  @VisibleForTesting
  protected boolean finish() {
    // wait for completion.
    while (!done
        && (numCompletedContainers.get() != numTotalContainers)) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {}
    }

    if (timelineServiceV2Enabled) {
      publishApplicationAttemptEventOnTimelineServiceV2(
          DSEvent.DS_APP_ATTEMPT_END);
    } else if (timelineServiceV1Enabled) {
      publishApplicationAttemptEvent(timelineClient, appAttemptID.toString(),
          DSEvent.DS_APP_ATTEMPT_END, domainId, appSubmitterUgi);
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

    // When the application completes, it should stop all running containers
    LOG.info("Application completed. Stopping running containers");
    nmClientAsync.stop();

    // When the application completes, it should send a finish application
    // signal to the RM
    LOG.info("Application completed. Signalling finish to RM");

    FinalApplicationStatus appStatus;
    String appMessage = null;
    boolean success = true;
    if (numCompletedContainers.get() - numFailedContainers.get()
        >= numTotalContainers) {
      appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
      appStatus = FinalApplicationStatus.FAILED;
      appMessage = "Diagnostics." + ", total=" + numTotalContainers
          + ", completed=" + numCompletedContainers.get() + ", allocated="
          + numAllocatedContainers.get() + ", failed="
          + numFailedContainers.get();
      LOG.info(appMessage);
      success = false;
    }
    try {
      amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (YarnException ex) {
      LOG.error("Failed to unregister application", ex);
    } catch (IOException e) {
      LOG.error("Failed to unregister application", e);
    }
    
    amRMClient.stop();

    // Stop Timeline Client
    if(timelineServiceV1Enabled) {
      timelineClient.stop();
    } else if (timelineServiceV2Enabled) {
      timelineV2Client.stop();
    }

    return success;
  }

  @VisibleForTesting
  class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
          + completedContainers.size());
      for (ContainerStatus containerStatus : completedContainers) {
        LOG.info(appAttemptID + " got container status for containerID="
            + containerStatus.getContainerId() + ", state="
            + containerStatus.getState() + ", exitStatus="
            + containerStatus.getExitStatus() + ", diagnostics="
            + containerStatus.getDiagnostics());

        // non complete containers should not be here
        assert (containerStatus.getState() == ContainerState.COMPLETE);
        // ignore containers we know nothing about - probably from a previous
        // attempt
        if (!launchedContainers.contains(containerStatus.getContainerId())) {
          LOG.info("Ignoring completed status of "
              + containerStatus.getContainerId()
              + "; unknown container(probably launched by previous attempt)");
          continue;
        }

        // increment counters for completed/failed containers
        int exitStatus = containerStatus.getExitStatus();
        if (0 != exitStatus) {
          // container failed
          if (ContainerExitStatus.ABORTED != exitStatus) {
            // shell script failed
            // counts as completed
            numCompletedContainers.incrementAndGet();
            numFailedContainers.incrementAndGet();
          } else {
            // container was killed by framework, possibly preempted
            // we should re-try as the container was lost for some reason
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            // we do not need to release the container as it would be done
            // by the RM
          }
        } else {
          // nothing to do
          // container completed successfully
          numCompletedContainers.incrementAndGet();
          LOG.info("Container completed successfully." + ", containerId="
              + containerStatus.getContainerId());
        }
        if (timelineServiceV2Enabled) {
          Long containerStartTime =
              containerStartTimes.get(containerStatus.getContainerId());
          if (containerStartTime == null) {
            containerStartTime = SystemClock.getInstance().getTime();
            containerStartTimes.put(containerStatus.getContainerId(),
                containerStartTime);
          }
          publishContainerEndEventOnTimelineServiceV2(containerStatus,
              containerStartTime);
        } else if (timelineServiceV1Enabled) {
          publishContainerEndEvent(timelineClient, containerStatus, domainId,
              appSubmitterUgi);
        }
      }

      // ask for more containers if any failed
      int askCount = numTotalContainers - numRequestedContainers.get();
      numRequestedContainers.addAndGet(askCount);

      if (askCount > 0) {
        for (int i = 0; i < askCount; ++i) {
          ContainerRequest containerAsk = setupContainerAskForRM();
          amRMClient.addContainerRequest(containerAsk);
        }
      }
      
      if (numCompletedContainers.get() == numTotalContainers) {
        done = true;
      }
    }

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt="
          + allocatedContainers.size());
      numAllocatedContainers.addAndGet(allocatedContainers.size());
      for (Container allocatedContainer : allocatedContainers) {
        String yarnShellId = Integer.toString(yarnShellIdCounter);
        yarnShellIdCounter++;
        LOG.info("Launching shell command on a new container."
            + ", containerId=" + allocatedContainer.getId()
            + ", yarnShellId=" + yarnShellId
            + ", containerNode=" + allocatedContainer.getNodeId().getHost()
            + ":" + allocatedContainer.getNodeId().getPort()
            + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
            + ", containerResourceMemory"
            + allocatedContainer.getResource().getMemorySize()
            + ", containerResourceVirtualCores"
            + allocatedContainer.getResource().getVirtualCores());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());

        Thread launchThread = createLaunchContainerThread(allocatedContainer,
            yarnShellId);

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchedContainers.add(allocatedContainer.getId());
        launchThread.start();
      }
    }

    @Override
    public void onContainersUpdated(
        List<UpdatedContainer> containers) {}

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {}

    @Override
    public float getProgress() {
      // set progress to deliver to RM on next heartbeat
      float progress = (float) numCompletedContainers.get()
          / numTotalContainers;
      return progress;
    }

    @Override
    public void onError(Throwable e) {
      LOG.error("Error in RMCallbackHandler: ", e);
      done = true;
      amRMClient.stop();
    }
  }

  @VisibleForTesting
  static class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

    private ConcurrentMap<ContainerId, Container> containers =
        new ConcurrentHashMap<ContainerId, Container>();
    private final ApplicationMaster applicationMaster;

    public NMCallbackHandler(ApplicationMaster applicationMaster) {
      this.applicationMaster = applicationMaster;
    }

    public void addContainer(ContainerId containerId, Container container) {
      containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to stop Container " + containerId);
      }
      containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Container Status: id=" + containerId + ", status=" +
            containerStatus);
      }
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Succeeded to start Container " + containerId);
      }
      Container container = containers.get(containerId);
      if (container != null) {
        applicationMaster.nmClientAsync.getContainerStatusAsync(
            containerId, container.getNodeId());
      }
      if (applicationMaster.timelineServiceV2Enabled) {
        long startTime = SystemClock.getInstance().getTime();
        applicationMaster.getContainerStartTimes().put(containerId, startTime);
        applicationMaster.publishContainerStartEventOnTimelineServiceV2(
            container, startTime);
      } else if (applicationMaster.timelineServiceV1Enabled) {
        applicationMaster.publishContainerStartEvent(
            applicationMaster.timelineClient, container,
            applicationMaster.domainId, applicationMaster.appSubmitterUgi);
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId, t);
      containers.remove(containerId);
      applicationMaster.numCompletedContainers.incrementAndGet();
      applicationMaster.numFailedContainers.incrementAndGet();
    }

    @Override
    public void onGetContainerStatusError(
        ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of Container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop Container " + containerId);
      containers.remove(containerId);
    }

    @Deprecated
    @Override
    public void onIncreaseContainerResourceError(
        ContainerId containerId, Throwable t) {}

    @Deprecated
    @Override
    public void onContainerResourceIncreased(
        ContainerId containerId, Resource resource) {}

    @Override
    public void onUpdateContainerResourceError(
        ContainerId containerId, Throwable t) {
    }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId,
        Resource resource) {
    }
  }

  /**
   * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Runnable {

    // Allocated container
    private Container container;
    private String shellId;

    NMCallbackHandler containerListener;

    /**
     * @param lcontainer Allocated container
     * @param containerListener Callback handler of the container
     */
    public LaunchContainerRunnable(Container lcontainer,
        NMCallbackHandler containerListener, String shellId) {
      this.container = lcontainer;
      this.containerListener = containerListener;
      this.shellId = shellId;
    }

    @Override
    /**
     * Connects to CM, sets up container launch context 
     * for shell command and eventually dispatches the container 
     * start request to the CM. 
     */
    public void run() {
      LOG.info("Setting up container launch container for containerid="
          + container.getId() + " with shellid=" + shellId);

      // Set the local resources
      Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

      // The container for the eventual shell commands needs its own local
      // resources too.
      // In this scenario, if a shell script is specified, we need to have it
      // copied and made available to the container.
      if (!scriptPath.isEmpty()) {
        Path renamedScriptPath = null;
        if (Shell.WINDOWS) {
          renamedScriptPath = new Path(scriptPath + ".bat");
        } else {
          renamedScriptPath = new Path(scriptPath + ".sh");
        }

        try {
          // rename the script file based on the underlying OS syntax.
          renameScriptFile(renamedScriptPath);
        } catch (Exception e) {
          LOG.error(
              "Not able to add suffix (.bat/.sh) to the shell script filename",
              e);
          // We know we cannot continue launching the container
          // so we should release it.
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }

        URL yarnUrl = null;
        try {
          yarnUrl = URL.fromURI(new URI(renamedScriptPath.toString()));
        } catch (URISyntaxException e) {
          LOG.error("Error when trying to use shell script path specified"
              + " in env, path=" + renamedScriptPath, e);
          // A failure scenario on bad input such as invalid shell script path
          // We know we cannot continue launching the container
          // so we should release it.
          // TODO
          numCompletedContainers.incrementAndGet();
          numFailedContainers.incrementAndGet();
          return;
        }
        LocalResource shellRsrc = LocalResource.newInstance(yarnUrl,
          LocalResourceType.FILE, LocalResourceVisibility.APPLICATION,
          shellScriptPathLen, shellScriptPathTimestamp);
        localResources.put(Shell.WINDOWS ? EXEC_BAT_SCRIPT_STRING_PATH :
            EXEC_SHELL_STRING_PATH, shellRsrc);
        shellCommand = Shell.WINDOWS ? windows_command : linux_bash_command;
      }

      // Set the necessary command to execute on the allocated container
      Vector<CharSequence> vargs = new Vector<CharSequence>(5);

      // Set executable command
      vargs.add(shellCommand);
      // Set shell script path
      if (!scriptPath.isEmpty()) {
        vargs.add(Shell.WINDOWS ? EXEC_BAT_SCRIPT_STRING_PATH
            : EXEC_SHELL_STRING_PATH);
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

      // Set up ContainerLaunchContext, setting local resource, environment,
      // command and token for constructor.

      // Note for tokens: Set up tokens for the container too. Today, for normal
      // shell commands, the container in distribute-shell doesn't need any
      // tokens. We are populating them mainly for NodeManagers to be able to
      // download anyfiles in the distributed file-system. The tokens are
      // otherwise also useful in cases, for e.g., when one is running a
      // "hadoop dfs" command inside the distributed shell.
      Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
      myShellEnv.put(YARN_SHELL_ID, shellId);
      ContainerRetryContext containerRetryContext =
          ContainerRetryContext.newInstance(
              containerRetryPolicy, containerRetryErrorCodes,
              containerMaxRetries, containrRetryInterval);
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
        localResources, myShellEnv, commands, null, allTokens.duplicate(),
          null, containerRetryContext);
      containerListener.addContainer(container.getId(), container);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  private void renameScriptFile(final Path renamedScriptPath)
      throws IOException, InterruptedException {
    appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws IOException {
        FileSystem fs = renamedScriptPath.getFileSystem(conf);
        fs.rename(new Path(scriptPath), renamedScriptPath);
        return null;
      }
    });
    LOG.info("User " + appSubmitterUgi.getUserName()
        + " added suffix(.sh/.bat) to script file as " + renamedScriptPath);
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   *
   * @return the setup ResourceRequest to be sent to RM
   */
  private ContainerRequest setupContainerAskForRM() {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    // TODO - what is the range for priority? how to decide?
    Priority pri = Priority.newInstance(requestPriority);

    // Set up resource type requirements
    ContainerRequest request =
        new ContainerRequest(createProfileCapability(), null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

  private boolean fileExist(String filePath) {
    return new File(filePath).exists();
  }

  private String readContent(String filePath) throws IOException {
    DataInputStream ds = null;
    try {
      ds = new DataInputStream(new FileInputStream(filePath));
      return ds.readUTF();
    } finally {
      org.apache.commons.io.IOUtils.closeQuietly(ds);
    }
  }

  private void publishContainerStartEvent(
      final TimelineClient timelineClient, final Container container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getId().toString());
    entity.setEntityType(DSEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME, container.getId()
        .getApplicationAttemptId().getApplicationId().toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DSEvent.DS_CONTAINER_START.toString());
    event.addEventInfo("Node", container.getNodeId().toString());
    event.addEventInfo("Resources", container.getResource().toString());
    entity.addEvent(event);

    try {
      processTimelineResponseErrors(
          putContainerEntity(timelineClient,
              container.getId().getApplicationAttemptId(),
              entity));
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error("Container start event could not be published for "
          + container.getId().toString(), e);
    }
  }

  @VisibleForTesting
  void publishContainerEndEvent(
      final TimelineClient timelineClient, ContainerStatus container,
      String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(container.getContainerId().toString());
    entity.setEntityType(DSEntity.DS_CONTAINER.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    entity.addPrimaryFilter(APPID_TIMELINE_FILTER_NAME,
        container.getContainerId().getApplicationAttemptId()
            .getApplicationId().toString());
    TimelineEvent event = new TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setEventType(DSEvent.DS_CONTAINER_END.toString());
    event.addEventInfo("State", container.getState().name());
    event.addEventInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    try {
      processTimelineResponseErrors(
          putContainerEntity(timelineClient,
              container.getContainerId().getApplicationAttemptId(),
              entity));
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(), e);
    }
  }

  private TimelinePutResponse putContainerEntity(
      TimelineClient timelineClient, ApplicationAttemptId currAttemptId,
      TimelineEntity entity)
      throws YarnException, IOException {
    if (TimelineUtils.timelineServiceV1_5Enabled(conf)) {
      TimelineEntityGroupId groupId = TimelineEntityGroupId.newInstance(
          currAttemptId.getApplicationId(),
          CONTAINER_ENTITY_GROUP_ID);
      return timelineClient.putEntities(currAttemptId, groupId, entity);
    } else {
      return timelineClient.putEntities(entity);
    }
  }

  private void publishApplicationAttemptEvent(
      final TimelineClient timelineClient, String appAttemptId,
      DSEvent appEvent, String domainId, UserGroupInformation ugi) {
    final TimelineEntity entity = new TimelineEntity();
    entity.setEntityId(appAttemptId);
    entity.setEntityType(DSEntity.DS_APP_ATTEMPT.toString());
    entity.setDomainId(domainId);
    entity.addPrimaryFilter(USER_TIMELINE_FILTER_NAME, ugi.getShortUserName());
    TimelineEvent event = new TimelineEvent();
    event.setEventType(appEvent.toString());
    event.setTimestamp(System.currentTimeMillis());
    entity.addEvent(event);
    try {
      TimelinePutResponse response = timelineClient.putEntities(entity);
      processTimelineResponseErrors(response);
    } catch (YarnException | IOException | ClientHandlerException e) {
      LOG.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptID, e);
    }
  }

  private TimelinePutResponse processTimelineResponseErrors(
      TimelinePutResponse response) {
    List<TimelinePutResponse.TimelinePutError> errors = response.getErrors();
    if (errors.size() == 0) {
      LOG.debug("Timeline entities are successfully put");
    } else {
      for (TimelinePutResponse.TimelinePutError error : errors) {
        LOG.error(
            "Error when publishing entity [" + error.getEntityType() + ","
                + error.getEntityId() + "], server side error code: "
                + error.getErrorCode());
      }
    }
    return response;
  }

  RMCallbackHandler getRMCallbackHandler() {
    return new RMCallbackHandler();
  }

  @VisibleForTesting
  void setAmRMClient(AMRMClientAsync client) {
    this.amRMClient = client;
  }

  @VisibleForTesting
  int getNumCompletedContainers() {
    return numCompletedContainers.get();
  }

  @VisibleForTesting
  boolean getDone() {
    return done;
  }

  @VisibleForTesting
  Thread createLaunchContainerThread(Container allocatedContainer,
      String shellId) {
    LaunchContainerRunnable runnableLaunchContainer =
        new LaunchContainerRunnable(allocatedContainer, containerListener,
            shellId);
    return new Thread(runnableLaunchContainer);
  }

  private void publishContainerStartEventOnTimelineServiceV2(
      Container container, long startTime) {
    final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
        entity =
            new org.apache.hadoop.yarn.api.records.timelineservice.
            TimelineEntity();
    entity.setId(container.getId().toString());
    entity.setType(DSEntity.DS_CONTAINER.toString());
    entity.setCreatedTime(startTime);
    entity.addInfo("user", appSubmitterUgi.getShortUserName());

    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
        new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
    event.setTimestamp(startTime);
    event.setId(DSEvent.DS_CONTAINER_START.toString());
    event.addInfo("Node", container.getNodeId().toString());
    event.addInfo("Resources", container.getResource().toString());
    entity.addEvent(event);
    entity.setIdPrefix(TimelineServiceHelper.invertLong(startTime));

    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          timelineV2Client.putEntities(entity);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Container start event could not be published for "
          + container.getId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }

  private void publishContainerEndEventOnTimelineServiceV2(
      final ContainerStatus container, long containerStartTime) {
    final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
        entity =
            new org.apache.hadoop.yarn.api.records.timelineservice.
            TimelineEntity();
    entity.setId(container.getContainerId().toString());
    entity.setType(DSEntity.DS_CONTAINER.toString());
    //entity.setDomainId(domainId);
    entity.addInfo("user", appSubmitterUgi.getShortUserName());
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
        new  org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
    event.setTimestamp(System.currentTimeMillis());
    event.setId(DSEvent.DS_CONTAINER_END.toString());
    event.addInfo("State", container.getState().name());
    event.addInfo("Exit Status", container.getExitStatus());
    entity.addEvent(event);
    entity.setIdPrefix(TimelineServiceHelper.invertLong(containerStartTime));

    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          timelineV2Client.putEntities(entity);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("Container end event could not be published for "
          + container.getContainerId().toString(),
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }

  private void publishApplicationAttemptEventOnTimelineServiceV2(
      DSEvent appEvent) {
    final org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity
        entity =
            new org.apache.hadoop.yarn.api.records.timelineservice.
            TimelineEntity();
    entity.setId(appAttemptID.toString());
    entity.setType(DSEntity.DS_APP_ATTEMPT.toString());
    long ts = System.currentTimeMillis();
    if (appEvent == DSEvent.DS_APP_ATTEMPT_START) {
      entity.setCreatedTime(ts);
    }
    entity.addInfo("user", appSubmitterUgi.getShortUserName());
    org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent event =
        new org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent();
    event.setId(appEvent.toString());
    event.setTimestamp(ts);
    entity.addEvent(event);
    entity.setIdPrefix(
        TimelineServiceHelper.invertLong(appAttemptID.getAttemptId()));

    try {
      appSubmitterUgi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public TimelinePutResponse run() throws Exception {
          timelineV2Client.putEntitiesAsync(entity);
          return null;
        }
      });
    } catch (Exception e) {
      LOG.error("App Attempt "
          + (appEvent.equals(DSEvent.DS_APP_ATTEMPT_START) ? "start" : "end")
          + " event could not be published for "
          + appAttemptID,
          e instanceof UndeclaredThrowableException ? e.getCause() : e);
    }
  }

  private ProfileCapability createProfileCapability()
      throws YarnRuntimeException {
    if (containerMemory < -1 || containerMemory == 0) {
      throw new YarnRuntimeException("Value of AM memory '" + containerMemory
          + "' has to be greater than 0");
    }
    if (containerVirtualCores < -1 || containerVirtualCores == 0) {
      throw new YarnRuntimeException(
          "Value of AM vcores '" + containerVirtualCores
              + "' has to be greater than 0");
    }

    Resource resourceCapability =
        Resource.newInstance(containerMemory, containerVirtualCores);
    if (resourceProfiles == null) {
      containerMemory = containerMemory == -1 ? DEFAULT_CONTAINER_MEMORY :
          containerMemory;
      containerVirtualCores =
          containerVirtualCores == -1 ? DEFAULT_CONTAINER_VCORES :
              containerVirtualCores;
      resourceCapability.setMemorySize(containerMemory);
      resourceCapability.setVirtualCores(containerVirtualCores);
    }

    String profileName = containerResourceProfile;
    if ("".equals(containerResourceProfile) && resourceProfiles != null) {
      profileName = "default";
    }
    ProfileCapability capability =
        ProfileCapability.newInstance(profileName, resourceCapability);
    return capability;
  }
}
