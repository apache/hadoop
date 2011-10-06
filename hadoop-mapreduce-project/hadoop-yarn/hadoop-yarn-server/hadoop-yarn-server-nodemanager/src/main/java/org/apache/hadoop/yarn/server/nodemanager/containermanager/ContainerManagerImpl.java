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

import static org.apache.hadoop.yarn.service.Service.STATE.STARTED;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.ipc.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerManagerSecurityInfo;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.event.LogAggregatorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;
import org.apache.hadoop.yarn.service.ServiceStateChangeListener;

public class ContainerManagerImpl extends CompositeService implements
    ServiceStateChangeListener, ContainerManager,
    EventHandler<ContainerManagerEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);

  final Context context;
  private final ContainersMonitor containersMonitor;
  private Server server;
  private final ResourceLocalizationService rsrcLocalizationSrvc;
  private final ContainersLauncher containersLauncher;
  private final AuxServices auxiluaryServices;
  private final NodeManagerMetrics metrics;

  private final NodeStatusUpdater nodeStatusUpdater;
  private ContainerTokenSecretManager containerTokenSecretManager;

  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  
  protected final AsyncDispatcher dispatcher;

  private final DeletionService deletionService;

  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, ContainerTokenSecretManager 
      containerTokenSecretManager) {
    super(ContainerManagerImpl.class.getName());
    this.context = context;
    dispatcher = new AsyncDispatcher();
    this.deletionService = deletionContext;
    this.metrics = metrics;

    rsrcLocalizationSrvc =
        createResourceLocalizationService(exec, deletionContext);
    addService(rsrcLocalizationSrvc);

    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);

    this.nodeStatusUpdater = nodeStatusUpdater;
    this.containerTokenSecretManager = containerTokenSecretManager;

    // Start configurable services
    auxiluaryServices = new AuxServices();
    auxiluaryServices.register(this);
    addService(auxiluaryServices);

    this.containersMonitor =
        new ContainersMonitorImpl(exec, dispatcher, this.context);
    addService(this.containersMonitor);

    LogAggregationService logAggregationService =
        createLogAggregationService(this.context, this.deletionService);
    addService(logAggregationService);

    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        new ApplicationEventDispatcher());
    dispatcher.register(LocalizationEventType.class, rsrcLocalizationSrvc);
    dispatcher.register(AuxServicesEventType.class, auxiluaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    dispatcher.register(LogAggregatorEventType.class, logAggregationService);
    addService(dispatcher);
  }

  protected LogAggregationService createLogAggregationService(Context context,
      DeletionService deletionService) {
    return new LogAggregationService(context, deletionService);
  }

  public ContainersMonitor getContainersMonitor() {
    return this.containersMonitor;
  }

  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext);
  }

  protected ContainersLauncher createContainersLauncher(Context context,
      ContainerExecutor exec) {
    return new ContainersLauncher(context, this.dispatcher, exec);
  }

  @Override
  public void start() {

    // Enqueue user dirs in deletion context

    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress initialAddress = NetUtils.createSocketAddr(conf.get(
        YarnConfiguration.NM_ADDRESS, YarnConfiguration.DEFAULT_NM_ADDRESS));

    server =
        rpc.getServer(ContainerManager.class, this, initialAddress, conf,
            this.containerTokenSecretManager,
            conf.getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    server.start();
    InetAddress hostNameResolved = null;
    try {
      hostNameResolved = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new YarnException(e);
    }
    this.context.getNodeId().setHost(hostNameResolved.getCanonicalHostName());
    this.context.getNodeId().setPort(server.getPort());
    LOG.info("ContainerManager started at "
        + this.context.getNodeId().toString());
    super.start();
  }

  @Override
  public void stop() {
    if (auxiluaryServices.getServiceState() == STARTED) {
      auxiluaryServices.unregister(this);
    }
    if (server != null) {
      server.close();
    }
    super.stop();
  }

  /**
   * Start a container on this NodeManager.
   */
  @Override
  public StartContainerResponse startContainer(StartContainerRequest request)
      throws YarnRemoteException {
    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    LOG.info(" container is " + request);
  
    // //////////// Parse credentials
    ByteBuffer tokens = launchContext.getContainerTokens();
    Credentials credentials = new Credentials();
    if (tokens != null) {
      DataInputByteBuffer buf = new DataInputByteBuffer();
      tokens.rewind();
      buf.reset(tokens);
      try {
        credentials.readTokenStorageStream(buf);
        if (LOG.isDebugEnabled()) {
          for (Token<? extends TokenIdentifier> tk : credentials
              .getAllTokens()) {
            LOG.debug(tk.getService() + " = " + tk.toString());
          }
        }
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
    }
    // //////////// End of parsing credentials

    Container container =
        new ContainerImpl(this.dispatcher, launchContext, credentials, metrics);
    ContainerId containerID = launchContext.getContainerId();
    ApplicationId applicationID = 
        containerID.getApplicationAttemptId().getApplicationId();
    if (context.getContainers().putIfAbsent(containerID, container) != null) {
      NMAuditLogger.logFailure(launchContext.getUser(), 
          AuditConstants.START_CONTAINER, "ContainerManagerImpl",
          "Container already running on this node!",
          applicationID, containerID);
      throw RPCUtil.getRemoteException("Container " + containerID
          + " already is running on this node!!");
    }

    // Create the application
    Application application = new ApplicationImpl(dispatcher,
        launchContext.getUser(), applicationID, credentials);
    if (null ==
        context.getApplications().putIfAbsent(applicationID, application)) {
      LOG.info("Creating a new application reference for app "
          + applicationID);
    }

    // TODO: Validate the request
    dispatcher.getEventHandler().handle(new ApplicationInitEvent(container));

    NMAuditLogger.logSuccess(launchContext.getUser(), 
        AuditConstants.START_CONTAINER, "ContainerManageImpl", 
        applicationID, containerID);

    StartContainerResponse response =
        recordFactory.newRecordInstance(StartContainerResponse.class);
    response.addAllServiceResponse(auxiluaryServices.getMeta());
    metrics.launchedContainer();
    metrics.allocateContainer(launchContext.getResource());
    return response;
  }

  @Override
  public StopContainerResponse stopContainer(StopContainerRequest request)
      throws YarnRemoteException {

    StopContainerResponse response =
        recordFactory.newRecordInstance(StopContainerResponse.class);

    ContainerId containerID = request.getContainerId();
    Container container = this.context.getContainers().get(containerID);
    if (container == null) {
      LOG.warn("Trying to stop unknown container " + containerID);
      String userName;
      try {
        userName = UserGroupInformation.getCurrentUser().getUserName();
      } catch (IOException e) {
        LOG.error("Error finding userName", e);
        return response;
      }
      NMAuditLogger.logFailure(userName,
          AuditConstants.STOP_CONTAINER, "ContainerManagerImpl",
          "Trying to stop unknown container!",
          containerID.getApplicationAttemptId().getApplicationId(), 
          containerID);
      return response; // Return immediately.
    }
    dispatcher.getEventHandler().handle(
        new ContainerKillEvent(containerID,
            "Container killed by the ApplicationMaster."));

    // user logged here not ideal since just getting user from container but
    // request doesn't have anything and should be coming from user of AM so 
    // should be the same or should be rejected by auth before here. 
    NMAuditLogger.logSuccess(container.getUser(), 
        AuditConstants.STOP_CONTAINER, "ContainerManageImpl", 
        containerID.getApplicationAttemptId().getApplicationId(), 
        containerID);

    // TODO: Move this code to appropriate place once kill_container is
    // implemented.
    nodeStatusUpdater.sendOutofBandHeartBeat();

    return response;
  }

  @Override
  public GetContainerStatusResponse getContainerStatus(GetContainerStatusRequest request) throws YarnRemoteException {
    ContainerId containerID = request.getContainerId();
    LOG.info("Getting container-status for " + containerID);
    Container container = this.context.getContainers().get(containerID);
    if (container != null) {
      ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
      LOG.info("Returning " + containerStatus);
      GetContainerStatusResponse response = recordFactory.newRecordInstance(GetContainerStatusResponse.class);
      response.setStatus(containerStatus);
      return response;
    } else {
      throw RPCUtil.getRemoteException("Container " + containerID
          + " is not handled by this NodeManager");
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
        LOG.warn("Event " + event + " sent to absent application " +
            event.getApplicationID());
      }
    }
    
  }

  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
    case FINISH_APPS:
      CMgrCompletedAppsEvent appsFinishedEvent =
          (CMgrCompletedAppsEvent) event;
      for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
        this.dispatcher.getEventHandler().handle(
            new ApplicationEvent(appID,
                ApplicationEventType.FINISH_APPLICATION));
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
      LOG.warn("Invalid event " + event.getType() + ". Ignoring.");
    }
  }

  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }

}
