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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import static org.apache.hadoop.service.Service.STATE.STARTED;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidAuxServiceException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.NonAggregatingLogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.annotations.VisibleForTesting;

public class ContainerManagerImpl extends CompositeService implements
    ServiceStateChangeListener, ContainerManagementProtocol,
    EventHandler<ContainerManagerEvent> {

  /**
   * Extra duration to wait for applications to be killed on shutdown.
   */
  private static final int SHUTDOWN_CLEANUP_SLOP_MS = 1000;

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);

  final Context context;
  private final ContainersMonitor containersMonitor;
  private Server server;
  private final ResourceLocalizationService rsrcLocalizationSrvc;
  private final ContainersLauncher containersLauncher;
  private final AuxServices auxiliaryServices;
  private final NodeManagerMetrics metrics;

  private final NodeStatusUpdater nodeStatusUpdater;

  protected LocalDirsHandlerService dirsHandler;
  protected final AsyncDispatcher dispatcher;
  private final ApplicationACLsManager aclsManager;

  private final DeletionService deletionService;
  private AtomicBoolean blockNewContainerRequests = new AtomicBoolean(false);
  private boolean serviceStopped = false;
  private final ReadLock readLock;
  private final WriteLock writeLock;

  private long waitForContainersOnShutdownMillis;

  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    super(ContainerManagerImpl.class.getName());
    this.context = context;
    this.dirsHandler = dirsHandler;

    // ContainerManager level dispatcher.
    dispatcher = new AsyncDispatcher();
    this.deletionService = deletionContext;
    this.metrics = metrics;

    rsrcLocalizationSrvc =
        createResourceLocalizationService(exec, deletionContext);
    addService(rsrcLocalizationSrvc);

    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);

    this.nodeStatusUpdater = nodeStatusUpdater;
    this.aclsManager = aclsManager;

    // Start configurable services
    auxiliaryServices = new AuxServices();
    auxiliaryServices.registerServiceListener(this);
    addService(auxiliaryServices);

    this.containersMonitor =
        new ContainersMonitorImpl(exec, dispatcher, this.context);
    addService(this.containersMonitor);

    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        new ApplicationEventDispatcher());
    dispatcher.register(LocalizationEventType.class, rsrcLocalizationSrvc);
    dispatcher.register(AuxServicesEventType.class, auxiliaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    
    addService(dispatcher);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LogHandler logHandler =
      createLogHandler(conf, this.context, this.deletionService);
    addIfService(logHandler);
    dispatcher.register(LogHandlerEventType.class, logHandler);
    
    waitForContainersOnShutdownMillis =
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) +
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS) +
        SHUTDOWN_CLEANUP_SLOP_MS;

    super.serviceInit(conf);
  }

  protected LogHandler createLogHandler(Configuration conf, Context context,
      DeletionService deletionService) {
    if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      return new LogAggregationService(this.dispatcher, context,
          deletionService, dirsHandler);
    } else {
      return new NonAggregatingLogHandler(this.dispatcher, deletionService,
                                          dirsHandler);
    }
  }

  public ContainersMonitor getContainersMonitor() {
    return this.containersMonitor;
  }

  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext, dirsHandler);
  }

  protected ContainersLauncher createContainersLauncher(Context context,
      ContainerExecutor exec) {
    return new ContainersLauncher(context, this.dispatcher, exec, dirsHandler, this);
  }

  @Override
  protected void serviceStart() throws Exception {

    // Enqueue user dirs in deletion context

    Configuration conf = getConfig();
    Configuration serverConf = new Configuration(conf);

    // always enforce it to be token-based.
    serverConf.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      SaslRpcServer.AuthMethod.TOKEN.toString());
    
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress initialAddress = conf.getSocketAddr(
        YarnConfiguration.NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_PORT);

    server =
        rpc.getServer(ContainerManagementProtocol.class, this, initialAddress, 
            serverConf, this.context.getNMTokenSecretManager(),
            conf.getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new NMPolicyProvider());
    }
    
    LOG.info("Blocking new container-requests as container manager rpc" +
    		" server is still starting.");
    this.setBlockNewContainerRequests(true);
    server.start();
    InetSocketAddress connectAddress = NetUtils.getConnectAddress(server);
    NodeId nodeId = NodeId.newInstance(
        connectAddress.getAddress().getCanonicalHostName(),
        connectAddress.getPort());
    ((NodeManager.NMContext)context).setNodeId(nodeId);
    this.context.getNMTokenSecretManager().setNodeId(nodeId);
    this.context.getContainerTokenSecretManager().setNodeId(nodeId);
    LOG.info("ContainerManager started at " + connectAddress);
    super.serviceStart();
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() throws Exception {
    setBlockNewContainerRequests(true);
    this.writeLock.lock();
    try {
      serviceStopped = true;
      if (context != null) {
        cleanUpApplicationsOnNMShutDown();
      }
    } finally {
      this.writeLock.unlock();
    }
    if (auxiliaryServices.getServiceState() == STARTED) {
      auxiliaryServices.unregisterServiceListener(this);
    }
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  public void cleanUpApplicationsOnNMShutDown() {
    Map<ApplicationId, Application> applications =
        this.context.getApplications();
    if (applications.isEmpty()) {
      return;
    }
    LOG.info("Applications still running : " + applications.keySet());

    List<ApplicationId> appIds =
        new ArrayList<ApplicationId>(applications.keySet());
    this.handle(
        new CMgrCompletedAppsEvent(appIds,
            CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));

    LOG.info("Waiting for Applications to be Finished");

    long waitStartTime = System.currentTimeMillis();
    while (!applications.isEmpty()
        && System.currentTimeMillis() - waitStartTime < waitForContainersOnShutdownMillis) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.warn(
          "Interrupted while sleeping on applications finish on shutdown", ex);
      }
    }

    // All applications Finished
    if (applications.isEmpty()) {
      LOG.info("All applications in FINISHED state");
    } else {
      LOG.info("Done waiting for Applications to be Finished. Still alive: " +
          applications.keySet());
    }
  }

  public void cleanupContainersOnNMResync() {
    Map<ContainerId, Container> containers = context.getContainers();
    if (containers.isEmpty()) {
      return;
    }
    LOG.info("Containers still running on "
        + CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC + " : "
        + containers.keySet());

    List<ContainerId> containerIds =
      new ArrayList<ContainerId>(containers.keySet());

    LOG.info("Waiting for containers to be killed");

    this.handle(new CMgrCompletedContainersEvent(containerIds,
      CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC));

    /*
     * We will wait till all the containers change their state to COMPLETE. We
     * will not remove the container statuses from nm context because these
     * are used while re-registering node manager with resource manager.
     */
    boolean allContainersCompleted = false;
    while (!containers.isEmpty() && !allContainersCompleted) {
      allContainersCompleted = true;
      for (Entry<ContainerId, Container> container : containers.entrySet()) {
        if (((ContainerImpl) container.getValue()).getCurrentState()
            != ContainerState.COMPLETE) {
          allContainersCompleted = false;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while sleeping on container kill on resync",
              ex);
          }
          break;
        }
      }
    }
    // All containers killed
    if (allContainersCompleted) {
      LOG.info("All containers in DONE state");
    } else {
      LOG.info("Done waiting for containers to be killed. Still alive: " +
        containers.keySet());
    }
  }

  // Get the remoteUGI corresponding to the api call.
  protected UserGroupInformation getRemoteUgi()
      throws YarnException {
    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name. Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    return remoteUgi;
  }

  // Obtain the needed ContainerTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to
  // be sure.
  @Private
  @VisibleForTesting
  protected NMTokenIdentifier selectNMTokenIdentifier(
      UserGroupInformation remoteUgi) {
    Set<TokenIdentifier> tokenIdentifiers = remoteUgi.getTokenIdentifiers();
    NMTokenIdentifier resultId = null;
    for (TokenIdentifier id : tokenIdentifiers) {
      if (id instanceof NMTokenIdentifier) {
        resultId = (NMTokenIdentifier) id;
        break;
      }
    }
    return resultId;
  }

  protected void authorizeUser(UserGroupInformation remoteUgi,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    if (!remoteUgi.getUserName().equals(
      nmTokenIdentifier.getApplicationAttemptId().toString())) {
      throw RPCUtil.getRemoteException("Expected applicationAttemptId: "
          + remoteUgi.getUserName() + "Found: "
          + nmTokenIdentifier.getApplicationAttemptId());
    }
  }

  /**
   * @param containerTokenIdentifier
   *          of the container to be started
   * @throws YarnException
   */
  @Private
  @VisibleForTesting
  protected void authorizeStartRequest(NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException {

    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIDStr = containerId.toString();
    boolean unauthorized = false;
    StringBuilder messageBuilder =
        new StringBuilder("Unauthorized request to start container. ");
    if (!nmTokenIdentifier.getApplicationAttemptId().equals(
        containerId.getApplicationAttemptId())) {
      unauthorized = true;
      messageBuilder.append("\nNMToken for application attempt : ")
        .append(nmTokenIdentifier.getApplicationAttemptId())
        .append(" was used for starting container with container token")
        .append(" issued for application attempt : ")
        .append(containerId.getApplicationAttemptId());
    } else if (!this.context.getContainerTokenSecretManager()
        .isValidStartContainerRequest(containerTokenIdentifier)) {
      // Is the container being relaunched? Or RPC layer let startCall with
      // tokens generated off old-secret through?
      unauthorized = true;
      messageBuilder.append("\n Attempt to relaunch the same ")
        .append("container with id ").append(containerIDStr).append(".");
    } else if (containerTokenIdentifier.getExpiryTimeStamp() < System
      .currentTimeMillis()) {
      // Ensure the token is not expired.
      unauthorized = true;
      messageBuilder.append("\nThis token is expired. current time is ")
        .append(System.currentTimeMillis()).append(" found ")
        .append(containerTokenIdentifier.getExpiryTimeStamp());
    }
    if (unauthorized) {
      String msg = messageBuilder.toString();
      LOG.error(msg);
      throw RPCUtil.getRemoteException(msg);
    }
  }

  /**
   * Start a list of containers on this NodeManager.
   */
  @Override
  public StartContainersResponse
      startContainers(StartContainersRequest requests) throws YarnException,
          IOException {
    if (blockNewContainerRequests.get()) {
      throw new NMNotYetReadyException(
        "Rejecting new containers as NodeManager has not"
            + " yet connected with ResourceManager");
    }
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi,nmTokenIdentifier);
    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();
    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      ContainerId containerId = null;
      try {
        ContainerTokenIdentifier containerTokenIdentifier =
            BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
        verifyAndGetContainerTokenIdentifier(request.getContainerToken(),
          containerTokenIdentifier);
        containerId = containerTokenIdentifier.getContainerID();
        startContainerInternal(nmTokenIdentifier, containerTokenIdentifier,
          request);
        succeededContainers.add(containerId);
      } catch (YarnException e) {
        failedContainers.put(containerId, SerializedException.newInstance(e));
      } catch (InvalidToken ie) {
        failedContainers.put(containerId, SerializedException.newInstance(ie));
        throw ie;
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
    }

    return StartContainersResponse.newInstance(getAuxServiceMetaData(),
      succeededContainers, failedContainers);
  }

  @SuppressWarnings("unchecked")
  private void startContainerInternal(NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier,
      StartContainerRequest request) throws YarnException, IOException {

    /*
     * 1) It should save the NMToken into NMTokenSecretManager. This is done
     * here instead of RPC layer because at the time of opening/authenticating
     * the connection it doesn't know what all RPC calls user will make on it.
     * Also new NMToken is issued only at startContainer (once it gets renewed).
     * 
     * 2) It should validate containerToken. Need to check below things. a) It
     * is signed by correct master key (part of retrieve password). b) It
     * belongs to correct Node Manager (part of retrieve password). c) It has
     * correct RMIdentifier. d) It is not expired.
     */
    authorizeStartRequest(nmTokenIdentifier, containerTokenIdentifier);
 
    if (containerTokenIdentifier.getRMIdentifer() != nodeStatusUpdater
        .getRMIdentifier()) {
        // Is the container coming from unknown RM
        StringBuilder sb = new StringBuilder("\nContainer ");
        sb.append(containerTokenIdentifier.getContainerID().toString())
          .append(" rejected as it is allocated by a previous RM");
        throw new InvalidContainerException(sb.toString());
    }
    // update NMToken
    updateNMTokenIdentifier(nmTokenIdentifier);

    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIdStr = containerId.toString();
    String user = containerTokenIdentifier.getApplicationSubmitter();

    LOG.info("Start request for " + containerIdStr + " by user " + user);

    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    Map<String, ByteBuffer> serviceData = getAuxServiceMetaData();
    if (launchContext.getServiceData()!=null && 
        !launchContext.getServiceData().isEmpty()) {
      for (Map.Entry<String, ByteBuffer> meta : launchContext.getServiceData()
          .entrySet()) {
        if (null == serviceData.get(meta.getKey())) {
          throw new InvalidAuxServiceException("The auxService:" + meta.getKey()
              + " does not exist");
        }
      }
    }

    Credentials credentials = parseCredentials(launchContext);

    Container container =
        new ContainerImpl(getConfig(), this.dispatcher, launchContext,
          credentials, metrics, containerTokenIdentifier);
    ApplicationId applicationID =
        containerId.getApplicationAttemptId().getApplicationId();
    if (context.getContainers().putIfAbsent(containerId, container) != null) {
      NMAuditLogger.logFailure(user, AuditConstants.START_CONTAINER,
        "ContainerManagerImpl", "Container already running on this node!",
        applicationID, containerId);
      throw RPCUtil.getRemoteException("Container " + containerIdStr
          + " already is running on this node!!");
    }

    this.readLock.lock();
    try {
      if (!serviceStopped) {
        // Create the application
        Application application =
            new ApplicationImpl(dispatcher, user, applicationID, credentials, context);
        if (null == context.getApplications().putIfAbsent(applicationID,
          application)) {
          LOG.info("Creating a new application reference for app " + applicationID);

          dispatcher.getEventHandler().handle(
            new ApplicationInitEvent(applicationID, container.getLaunchContext()
              .getApplicationACLs()));
        }

        dispatcher.getEventHandler().handle(
          new ApplicationContainerInitEvent(container));

        this.context.getContainerTokenSecretManager().startContainerSuccessful(
          containerTokenIdentifier);
        NMAuditLogger.logSuccess(user, AuditConstants.START_CONTAINER,
          "ContainerManageImpl", applicationID, containerId);
        // TODO launchedContainer misplaced -> doesn't necessarily mean a container
        // launch. A finished Application will not launch containers.
        metrics.launchedContainer();
        metrics.allocateContainer(containerTokenIdentifier.getResource());
      } else {
        throw new YarnException(
            "Container start failed as the NodeManager is " +
            "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  protected ContainerTokenIdentifier verifyAndGetContainerTokenIdentifier(
      org.apache.hadoop.yarn.api.records.Token token,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException,
      InvalidToken {
    byte[] password =
        context.getContainerTokenSecretManager().retrievePassword(
          containerTokenIdentifier);
    byte[] tokenPass = token.getPassword().array();
    if (password == null || tokenPass == null
        || !Arrays.equals(password, tokenPass)) {
      throw new InvalidToken(
        "Invalid container token used for starting container on : "
            + context.getNodeId().toString());
    }
    return containerTokenIdentifier;
  }

  @Private
  @VisibleForTesting
  protected void updateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier)
      throws InvalidToken {
    context.getNMTokenSecretManager().appAttemptStartContainer(
      nmTokenIdentifier);
  }

  private Credentials parseCredentials(ContainerLaunchContext launchContext)
      throws YarnException {
    Credentials credentials = new Credentials();
    // //////////// Parse credentials
    ByteBuffer tokens = launchContext.getTokens();

    if (tokens != null) {
      DataInputByteBuffer buf = new DataInputByteBuffer();
      tokens.rewind();
      buf.reset(tokens);
      try {
        credentials.readTokenStorageStream(buf);
        if (LOG.isDebugEnabled()) {
          for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
            LOG.debug(tk.getService() + " = " + tk.toString());
          }
        }
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
    }
    // //////////// End of parsing credentials
    return credentials;
  }

  /**
   * Stop a list of containers running on this NodeManager.
   */
  @Override
  public StopContainersResponse stopContainers(StopContainersRequest requests)
      throws YarnException, IOException {

    List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    for (ContainerId id : requests.getContainerIds()) {
      try {
        stopContainerInternal(identifier, id);
        succeededRequests.add(id);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return StopContainersResponse
      .newInstance(succeededRequests, failedRequests);
  }

  @SuppressWarnings("unchecked")
  private void stopContainerInternal(NMTokenIdentifier nmTokenIdentifier,
      ContainerId containerID) throws YarnException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);
    LOG.info("Stopping container with container Id: " + containerIDStr);
    authorizeGetAndStopContainerRequest(containerID, container, true,
      nmTokenIdentifier);

    if (container == null) {
      if (!nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    } else {
      dispatcher.getEventHandler().handle(
        new ContainerKillEvent(containerID,
          "Container killed by the ApplicationMaster."));

      NMAuditLogger.logSuccess(container.getUser(),    
        AuditConstants.STOP_CONTAINER, "ContainerManageImpl", containerID
          .getApplicationAttemptId().getApplicationId(), containerID);

      // TODO: Move this code to appropriate place once kill_container is
      // implemented.
      nodeStatusUpdater.sendOutofBandHeartBeat();
    }
  }

  /**
   * Get a list of container statuses running on this NodeManager
   */
  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {

    List<ContainerStatus> succeededRequests = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    for (ContainerId id : request.getContainerIds()) {
      try {
        ContainerStatus status = getContainerStatusInternal(id, identifier);
        succeededRequests.add(status);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return GetContainerStatusesResponse.newInstance(succeededRequests,
      failedRequests);
  }

  private ContainerStatus getContainerStatusInternal(ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);

    LOG.info("Getting container-status for " + containerIDStr);
    authorizeGetAndStopContainerRequest(containerID, container, false,
      nmTokenIdentifier);

    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    }
    ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
    LOG.info("Returning " + containerStatus);
    return containerStatus;
  }

  @Private
  @VisibleForTesting
  protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
      Container container, boolean stopRequest, NMTokenIdentifier identifier)
      throws YarnException {
    /*
     * For get/stop container status; we need to verify that 1) User (NMToken)
     * application attempt only has started container. 2) Requested containerId
     * belongs to the same application attempt (NMToken) which was used. (Note:-
     * This will prevent user in knowing another application's containers).
     */

    if ((!identifier.getApplicationAttemptId().equals(
      containerId.getApplicationAttemptId()))
        || (container != null && !identifier.getApplicationAttemptId().equals(
          container.getContainerId().getApplicationAttemptId()))) {
      if (stopRequest) {
        LOG.warn(identifier.getApplicationAttemptId()
            + " attempted to stop non-application container : "
            + container.getContainerId().toString());
        NMAuditLogger.logFailure("UnknownUser", AuditConstants.STOP_CONTAINER,
          "ContainerManagerImpl", "Trying to stop unknown container!",
          identifier.getApplicationAttemptId().getApplicationId(),
          container.getContainerId());
      } else {
        LOG.warn(identifier.getApplicationAttemptId()
            + " attempted to get status for non-application container : "
            + container.getContainerId().toString());
      }
    }

  }

  class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      Map<ContainerId,Container> containers =
        ContainerManagerImpl.this.context.getContainers();
      Container c = containers.get(event.getContainerID());
      if (c != null) {
        c.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent container " +
            event.getContainerID());
      }
    }
  }

  class ApplicationEventDispatcher implements EventHandler<ApplicationEvent> {

    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          ContainerManagerImpl.this.context.getApplications().get(
              event.getApplicationID());
      if (app != null) {
        app.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent application "
            + event.getApplicationID());
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
    case FINISH_APPS:
      CMgrCompletedAppsEvent appsFinishedEvent =
          (CMgrCompletedAppsEvent) event;
      for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
        String diagnostic = "";
        if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN) {
          diagnostic = "Application killed on shutdown";
        } else if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER) {
          diagnostic = "Application killed by ResourceManager";
        }
        this.dispatcher.getEventHandler().handle(
            new ApplicationFinishEvent(appID,
                diagnostic));
      }
      break;
    case FINISH_CONTAINERS:
      CMgrCompletedContainersEvent containersFinishedEvent =
          (CMgrCompletedContainersEvent) event;
      for (ContainerId container : containersFinishedEvent
          .getContainersToCleanup()) {
          this.dispatcher.getEventHandler().handle(
              new ContainerKillEvent(container,
                  "Container Killed by ResourceManager"));
      }
      break;
    default:
        throw new YarnRuntimeException(
            "Got an unknown ContainerManagerEvent type: " + event.getType());
    }
  }

  public void setBlockNewContainerRequests(boolean blockNewContainerRequests) {
    this.blockNewContainerRequests.set(blockNewContainerRequests);
  }

  @Private
  @VisibleForTesting
  public boolean getBlockNewContainerRequestsStatus() {
    return this.blockNewContainerRequests.get();
  }
  
  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }
  
  public Context getContext() {
    return this.context;
  }

  public Map<String, ByteBuffer> getAuxServiceMetaData() {
    return this.auxiliaryServices.getMetaData();
  }
}
