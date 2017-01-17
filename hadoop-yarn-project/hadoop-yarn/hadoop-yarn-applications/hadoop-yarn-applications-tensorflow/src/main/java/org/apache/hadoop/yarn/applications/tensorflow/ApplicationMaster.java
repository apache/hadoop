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

package org.apache.hadoop.yarn.applications.tensorflow;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.SysInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.log4j.LogManager;

import com.google.common.annotations.VisibleForTesting;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

  private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

  private Configuration conf;

  public Configuration getConfiguration() {
    return conf;
  }

  private AMRMClientAsync amRMClient;

  // In both secure and non-secure modes, this points to the job-submitter.
  @VisibleForTesting
  UserGroupInformation appSubmitterUgi;

  private NMClientAsync nmClientAsync;
  public NMClientAsync getNMClientAsync() {
    return nmClientAsync;
  }
  private NMCallbackHandler containerListener;

  @VisibleForTesting
  protected ApplicationAttemptId appAttemptID;

  public ApplicationAttemptId getAppAttempId() {
    return appAttemptID;
  }

  // TODO
  // For status update for clients - yet to be implemented
  // Hostname of the container
  private String appMasterHostname = "";
  // Port on which the app master listens for status updates from clients
  private int appMasterRpcPort = -1;
  // Tracking url to which app master publishes info for clients to monitor
  private String appMasterTrackingUrl = "";

  @VisibleForTesting
  protected int numTotalContainers = 1;
  private long containerMemory = 10;
  private int containerVirtualCores = 1;
  private int requestPriority;

  private int numTotalWokerContainers = 1;

  private int numTotalParamServerContainer = 0;

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

  // Container retry options
  private ContainerRetryPolicy containerRetryPolicy =
          ContainerRetryPolicy.NEVER_RETRY;
  private Set<Integer> containerRetryErrorCodes = null;
  private int containerMaxRetries = 0;
  private int containrRetryInterval = 0;

  // TF server jar file path on hdfs
  private String tfServerJar = "";

  // Hardcoded path to custom log_properties
  private static final String log4jPath = "log4j.properties";

  private volatile boolean done;

  private ByteBuffer allTokens;
  public ByteBuffer getAllTokens() {
    return allTokens;
  }

  // Launch threads
  private List<Thread> launchThreads = new ArrayList<Thread>();

  private int yarnShellIdCounter = 1;

  private  ClusterSpec clusterSpec;

  @VisibleForTesting
  protected final Set<ContainerId> launchedContainers =
          Collections.newSetFromMap(new ConcurrentHashMap<ContainerId, Boolean>());

  protected final Set<Container> allocatedContainers =
          Collections.newSetFromMap(new ConcurrentHashMap<Container, Boolean>());

  /**
   * @param args TF server args
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
      LOG.fatal("Error running ApplicationMaster", t);
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

  public ApplicationMaster() {
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
    opts.addOption(TFApplication.OPT_TF_APP_ATTEMPT_ID, true,
            "App Attempt ID. Not to be used unless for testing purposes");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_MEMORY, true,
            "Amount of memory in MB to be requested to run the shell command");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_VCORES, true,
            "Amount of virtual cores to be requested to run the shell command");
    opts.addOption(TFApplication.OPT_TF_PRIORITY, true, "Application Priority. Default 0");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_RETRY_POLICY, true,
            "Retry policy when container fails to run, "
                    + "0: NEVER_RETRY, 1: RETRY_ON_ALL_ERRORS, "
                    + "2: RETRY_ON_SPECIFIC_ERROR_CODES");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_RETRY_ERROR_CODES, true,
            "When retry policy is set to RETRY_ON_SPECIFIC_ERROR_CODES, error "
                    + "codes is specified with this option, "
                    + "e.g. --container_retry_error_codes 1,2,3");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_MAX_RETRIES, true,
            "If container could retry, it specifies max retires");
    opts.addOption(TFApplication.OPT_TF_CONTAINER_RETRY_INTERVAL, true,
            "Interval between each retry, unit is milliseconds");

    opts.addOption(TFApplication.OPT_TF_SERVER_JAR, true, "Provide container jar of tensorflow");
    opts.addOption(TFApplication.OPT_TF_WORKER_NUM, true, "Provide worker server number of tensorflow");
    opts.addOption(TFApplication.OPT_TF_PS_NUM, true, "Provide ps server number of tensorflow");

    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      printUsage(opts);
      throw new IllegalArgumentException(
              "No args specified for application master to initialize");
    }

    if (fileExist(log4jPath)) {
      try {
        Log4jPropertyHelper.updateLog4jConfiguration(ApplicationMaster.class,
                log4jPath);
      } catch (Exception e) {
        LOG.warn("Can not set up custom log4j properties. " + e);
      }
    }

    Map<String, String> envs = System.getenv();

    if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
      if (cliParser.hasOption(TFApplication.OPT_TF_APP_ATTEMPT_ID)) {
        String appIdStr = cliParser.getOptionValue(TFApplication.OPT_TF_APP_ATTEMPT_ID, "");
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

    containerMemory = Integer.parseInt(cliParser.getOptionValue(TFApplication.OPT_TF_CONTAINER_MEMORY, "256"));
    containerVirtualCores = Integer.parseInt(cliParser.getOptionValue(
            TFApplication.OPT_TF_CONTAINER_VCORES, "1"));


    numTotalWokerContainers = Integer.parseInt(cliParser.getOptionValue(
            TFApplication.OPT_TF_WORKER_NUM, "1"));
    if (numTotalWokerContainers == 0) {
      throw new IllegalArgumentException(
              "Cannot run tensroflow application with no worker containers");
    }

    numTotalParamServerContainer = Integer.parseInt(cliParser.getOptionValue(
            TFApplication.OPT_TF_PS_NUM, "0"));
    numTotalContainers = numTotalWokerContainers + numTotalParamServerContainer;
    if (numTotalContainers == 0) {
      throw new IllegalArgumentException(
              "Cannot run distributed shell with no containers");
    }

    requestPriority = Integer.parseInt(cliParser
            .getOptionValue(TFApplication.OPT_TF_PRIORITY, "0"));

    containerRetryPolicy = ContainerRetryPolicy.values()[
            Integer.parseInt(cliParser.getOptionValue(
                    TFApplication.OPT_TF_CONTAINER_RETRY_POLICY, "0"))];
    if (cliParser.hasOption(TFApplication.OPT_TF_CONTAINER_RETRY_ERROR_CODES)) {
      containerRetryErrorCodes = new HashSet<>();
      for (String errorCode :
              cliParser.getOptionValue(TFApplication.OPT_TF_CONTAINER_RETRY_ERROR_CODES).split(",")) {
        containerRetryErrorCodes.add(Integer.parseInt(errorCode));
      }
    }
    containerMaxRetries = Integer.parseInt(
            cliParser.getOptionValue(TFApplication.OPT_TF_CONTAINER_MAX_RETRIES, "0"));
    containrRetryInterval = Integer.parseInt(cliParser.getOptionValue(
            TFApplication.OPT_TF_CONTAINER_RETRY_INTERVAL, "0"));

    tfServerJar = cliParser.getOptionValue(TFApplication.OPT_TF_SERVER_JAR, TFAmContainer.APPMASTER_JAR_PATH);

    clusterSpec = ClusterSpec.makeClusterSpec(numTotalWokerContainers, numTotalParamServerContainer);

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

  private final class RpcForClient implements TFApplicationRpc {

    @Override
    public String getClusterSpec() throws IOException, YarnException {
      String cs = "";
      if (clusterSpec != null) {

        try {
          cs = clusterSpec.getJsonString();
        } catch (ClusterSpecException e) {
          cs = "";
          LOG.info("Cluster spec is not prepared yet when getting cluster spec!");
          //e.printStackTrace();
        }
      }

      return cs;
    }
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
      LOG.info(token);
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

    appMasterHostname = System.getenv(Environment.NM_HOST.name());
    TFApplicationRpcServer rpcServer = new TFApplicationRpcServer(appMasterHostname, new RpcForClient());
    appMasterRpcPort = rpcServer.getRpcPort();
    rpcServer.startRpcServiceThread();

    // Register self with ResourceManager
    // This will start heartbeating to the RM

    RegisterApplicationMasterResponse response = amRMClient
            .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                    appMasterTrackingUrl);
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

    return success;
  }

  public boolean startAllContainers() throws Exception {
    if (numAllocatedContainers.get() == numTotalContainers) {


      int numWorkerContainers = 0;
      int numPsContainers = 0;
      if (this.allocatedContainers.size() < numTotalWokerContainers + numTotalParamServerContainer) {
        LOG.error("not enough ps and woker containers allocated!");
        return false;
      }

      for (Container allocatedContainer : this.allocatedContainers) {
        if (numWorkerContainers < numTotalWokerContainers) {
          LOG.info("work cid: " + allocatedContainer.getId().toString());
          clusterSpec.addWorkerSpec(allocatedContainer.getId().toString(), allocatedContainer.getNodeId().getHost());
          numWorkerContainers++;
          continue;
        }

        if (numPsContainers < this.numTotalParamServerContainer) {
          LOG.info("ps cid: " + allocatedContainer.getId().toString());
          clusterSpec.addPsSpec(allocatedContainer.getId().toString(), allocatedContainer.getNodeId().getHost());
          numPsContainers++;
        }

      }

      for (Container allocatedContainer : this.allocatedContainers) {

        LOG.info("Launching a new container."
                + ", containerId=" + allocatedContainer.getId()
                + ", containerNode=" + allocatedContainer.getNodeId().getHost()
                + ":" + allocatedContainer.getNodeId().getPort()
                + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
                + ", containerResourceMemory"
                + allocatedContainer.getResource().getMemorySize()
                + ", containerResourceVirtualCores"
                + allocatedContainer.getResource().getVirtualCores());
        // + ", containerToken"
        // +allocatedContainer.getContainerToken().getIdentifier().toString());

        LOG.info("server cid: " + allocatedContainer.getId().toString());
        LaunchContainerThread launchDelegator = new LaunchContainerThread(allocatedContainer,
                this, clusterSpec.getServerAddress(allocatedContainer.getId().toString()));
        launchDelegator.setTfServerJar(tfServerJar);
        launchDelegator.setContainerMemory(containerMemory);
        launchDelegator.setContainerRetryPolicy(containerRetryPolicy);
        launchDelegator.setContainerRetryErrorCodes(containerRetryErrorCodes);
        launchDelegator.setContainerMaxRetries(containerMaxRetries);
        launchDelegator.setContainrRetryInterval(containrRetryInterval);
        Thread launchThread = new Thread(launchDelegator);

        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchedContainers.add(allocatedContainer.getId());
        launchThread.start();
      }
    } else {
      throw  new Exception("containers are not allocated!");
    }
    return true;
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
      ApplicationMaster.this.allocatedContainers.addAll(allocatedContainers);
      if (numAllocatedContainers.get() == numTotalContainers) {
        try {
          startAllContainers();
        } catch (Exception e) {
          e.printStackTrace();
        }
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

  public void addContainer(Container container) {
    if (containerListener != null && container != null) {
      containerListener.addContainer(container.getId(), container);
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
    }

    @Override
    public void onContainerResourceIncreased(
            ContainerId containerId, Resource resource) {}

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start Container " + containerId);
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

    @Override
    public void onIncreaseContainerResourceError(
            ContainerId containerId, Throwable t) {}

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
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Resource.newInstance(containerMemory,
            containerVirtualCores);

    ContainerRequest request = new ContainerRequest(capability, null, null,
            pri);
    //LOG.info("Requested container ask: " + request.toString());
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

}
