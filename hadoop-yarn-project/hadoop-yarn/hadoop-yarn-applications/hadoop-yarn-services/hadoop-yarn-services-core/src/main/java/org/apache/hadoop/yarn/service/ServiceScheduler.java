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

package org.apache.hadoop.yarn.service;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.service.api.ServiceApiConstants;
import org.apache.hadoop.yarn.service.api.records.ContainerState;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.api.records.ServiceState;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.component.ComponentRestartPolicy;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.ComponentEventType;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.ContainerLaunchService;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.service.registry.YarnRegistryViewForProviders;
import org.apache.hadoop.yarn.service.timelineservice.ServiceMetricsSink;
import org.apache.hadoop.yarn.service.timelineservice.ServiceTimelinePublisher;
import org.apache.hadoop.yarn.service.utils.HttpUtil;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.apache.hadoop.yarn.service.utils.ServiceRegistryUtils;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.util.BoundedAppender;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.registry.client.api.RegistryConstants.*;
import static org.apache.hadoop.yarn.api.records.ContainerExitStatus
    .KILLED_AFTER_APP_COMPLETION;
import static org.apache.hadoop.yarn.service.api.ServiceApiConstants.*;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.*;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.START;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConstants
    .CONTAINER_STATE_REPORT_AS_SERVICE_STATE;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes
    .EXIT_FALSE;
import static org.apache.hadoop.yarn.service.exceptions.LauncherExitCodes
    .EXIT_SUCCESS;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTPS_PREFIX;
import static org.apache.hadoop.yarn.webapp.util.WebAppUtils.HTTP_PREFIX;

/**
 *
 */
public class ServiceScheduler extends CompositeService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceScheduler.class);
  private Service app;

   // This encapsulates the <code>app</code> with methods to upgrade the app.
  private ServiceManager serviceManager;

  // component_name -> component
  private final Map<String, Component> componentsByName =
      new ConcurrentHashMap<>();

  // id - > component
  protected final Map<Long, Component> componentsById =
      new ConcurrentHashMap<>();

  private final Map<ContainerId, ComponentInstance> liveInstances =
      new ConcurrentHashMap<>();

  private ServiceMetrics serviceMetrics;

  private ServiceTimelinePublisher serviceTimelinePublisher;

  private boolean timelineServiceEnabled;

  // Global diagnostics that will be reported to RM on exit.
  // The unit the number of characters. This will be limited to 64 * 1024
  // characters.
  private final BoundedAppender diagnostics = new BoundedAppender(64 * 1024);

  // A cache for loading config files from remote such as hdfs
  public LoadingCache<ConfigFile, Object> configFileCache = null;

  public ScheduledExecutorService executorService;
  public Map<String, String> globalTokens = new HashMap<>();

  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private NMClientAsync nmClient;
  private AsyncDispatcher dispatcher;
  private YarnRegistryViewForProviders yarnRegistryOperations;
  private final ServiceContext context;
  private ContainerLaunchService containerLaunchService;
  private final Map<ContainerId, ComponentInstance> unRecoveredInstances =
      new ConcurrentHashMap<>();
  private long containerRecoveryTimeout;

  // If even one component of a service uses placement constraints, then use
  // placement scheduler to schedule containers for all components (including
  // the ones with no constraints). Mixing of container requests and scheduling
  // requests for a single service is not recommended.
  private boolean hasAtLeastOnePlacementConstraint;

  private boolean gracefulStop = false;

  private volatile FinalApplicationStatus finalApplicationStatus =
      FinalApplicationStatus.ENDED;

  private final Clock systemClock;

  // For unit test override since we don't want to terminate UT process.
  private final ServiceUtils.ProcessTerminationHandler
      terminationHandler = new ServiceUtils.ProcessTerminationHandler();

  public ServiceScheduler(ServiceContext context) {
    super(context.getService().getName());
    this.context = context;
    this.app = context.getService();
    this.systemClock = SystemClock.getInstance();
  }

  public void buildInstance(ServiceContext context, Configuration configuration)
      throws YarnException {
    app = context.service;
    executorService = Executors.newScheduledThreadPool(10);
    RegistryOperations registryClient;
    if (UserGroupInformation.isSecurityEnabled() &&
        !StringUtils.isEmpty(context.principal)
        && !StringUtils.isEmpty(context.keytab)) {
      Configuration conf = getConfig();
      // Only take the first section of the principal
      // e.g. hdfs-demo@EXAMPLE.COM will take hdfs-demo
      // This is because somehow zookeeper client only uses the first section
      // for acl validations.
      String username = new HadoopKerberosName(context.principal.trim())
          .getServiceName();
      LOG.info("Set registry user accounts: sasl:" + username);
      conf.set(KEY_REGISTRY_USER_ACCOUNTS, "sasl:" + username);
      registryClient = RegistryOperationsFactory
          .createKerberosInstance(conf,
              "Client", context.principal, context.keytab);
    } else {
      registryClient = RegistryOperationsFactory
          .createInstance("ServiceScheduler", configuration);
    }
    addIfService(registryClient);
    yarnRegistryOperations =
        createYarnRegistryOperations(context, registryClient);

    // register metrics,
    serviceMetrics = ServiceMetrics
        .register(app.getName(), "Metrics for service");
    serviceMetrics.tag("type", "Metrics type [component or service]", "service");
    serviceMetrics.tag("appId", "Service id for service", app.getId());

    amRMClient = createAMRMClient();
    addIfService(amRMClient);

    nmClient = createNMClient();
    nmClient.getClient().cleanupRunningContainersOnStop(false);
    addIfService(nmClient);

    dispatcher = createAsyncDispatcher();
    dispatcher.register(ServiceEventType.class, new ServiceEventHandler());
    dispatcher.register(ComponentEventType.class,
        new ComponentEventHandler());
    dispatcher.register(ComponentInstanceEventType.class,
        new ComponentInstanceEventHandler());
    dispatcher.setDrainEventsOnStop();
    addIfService(dispatcher);

    containerLaunchService = new ContainerLaunchService(context);
    addService(containerLaunchService);

    if (YarnConfiguration.timelineServiceV2Enabled(configuration)) {
      TimelineV2Client timelineClient = TimelineV2Client
          .createTimelineClient(context.attemptId.getApplicationId());
      amRMClient.registerTimelineV2Client(timelineClient);
      serviceTimelinePublisher = new ServiceTimelinePublisher(timelineClient);
      addService(serviceTimelinePublisher);
      DefaultMetricsSystem.instance().register("ServiceMetricsSink",
          "For processing metrics to ATS",
          new ServiceMetricsSink(serviceTimelinePublisher));
      LOG.info("Timeline v2 is enabled.");
    }

    initGlobalTokensForSubstitute(context);
    //substitute quicklinks
    ProviderUtils.substituteMapWithTokens(app.getQuicklinks(), globalTokens);
    createConfigFileCache(context.fs.getFileSystem());

    createAllComponents();
    containerRecoveryTimeout = YarnServiceConf.getInt(
        YarnServiceConf.CONTAINER_RECOVERY_TIMEOUT_MS,
        YarnServiceConf.DEFAULT_CONTAINER_RECOVERY_TIMEOUT_MS,
        app.getConfiguration(), getConfig());

    if (YarnConfiguration
        .timelineServiceV2Enabled(getConfig())) {
      timelineServiceEnabled = true;
    }

    serviceManager = createServiceManager();
    context.setServiceManager(serviceManager);

  }

  protected YarnRegistryViewForProviders createYarnRegistryOperations(
      ServiceContext context, RegistryOperations registryClient) {
    return new YarnRegistryViewForProviders(registryClient,
        RegistryUtils.currentUser(), YarnServiceConstants.APP_TYPE, app.getName(),
        context.attemptId);
  }

  protected ServiceManager createServiceManager() {
    return new ServiceManager(context);
  }

  protected AsyncDispatcher createAsyncDispatcher() {
    return new AsyncDispatcher("Component  dispatcher");
  }

  protected NMClientAsync createNMClient() {
    return NMClientAsync.createNMClientAsync(new NMClientCallback());
  }

  protected AMRMClientAsync<AMRMClient.ContainerRequest> createAMRMClient() {
    return AMRMClientAsync
        .createAMRMClientAsync(1000, new AMRMClientCallback());
  }

  public void setGracefulStop(FinalApplicationStatus applicationStatus) {
    this.gracefulStop = true;
    this.finalApplicationStatus = applicationStatus;
    nmClient.getClient().cleanupRunningContainersOnStop(true);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    try {
      buildInstance(context, conf);
    } catch (YarnException e) {
      throw new YarnRuntimeException(e);
    }
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    LOG.info("Stopping service scheduler");

    if (executorService != null) {
      executorService.shutdownNow();
    }

    DefaultMetricsSystem.shutdown();

    // only stop the entire service when a graceful stop has been initiated
    // (e.g. via client RPC, not through the AM receiving a SIGTERM)
    if (gracefulStop) {

      if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {

        // mark other component-instances/containers as STOPPED
        final Map<ContainerId, ComponentInstance> liveInst =
            getLiveInstances();
        for (Map.Entry<ContainerId, ComponentInstance> instance : liveInst
            .entrySet()) {
          if (!ComponentInstance.isFinalState(
              instance.getValue().getContainerSpec().getState())) {
            LOG.info("{} Component instance state changed from {} to {}",
                instance.getValue().getCompInstanceName(),
                instance.getValue().getContainerSpec().getState(),
                ContainerState.STOPPED);
            serviceTimelinePublisher.componentInstanceFinished(
                instance.getKey(), KILLED_AFTER_APP_COMPLETION,
                ContainerState.STOPPED, getDiagnostics().toString());
          }
        }

        LOG.info("Service state changed to {}", finalApplicationStatus);
        // mark attempt as unregistered
        serviceTimelinePublisher.serviceAttemptUnregistered(context,
            finalApplicationStatus, diagnostics.toString());
      }

      // unregister AM
      amRMClient.unregisterApplicationMaster(finalApplicationStatus,
          diagnostics.toString(), "");
      LOG.info("Service {} unregistered with RM, with attemptId = {} "
              + ", diagnostics = {} ", app.getName(), context.attemptId,
          diagnostics);
    }
    super.serviceStop();
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
    InetSocketAddress bindAddress = context.clientAMService.getBindAddress();
    // When yarn.resourcemanager.placement-constraints.handler is set to
    // placement-processor then constraints need to be added during
    // registerApplicationMaster.
    RegisterApplicationMasterResponse response = amRMClient
        .registerApplicationMaster(bindAddress.getHostName(),
            bindAddress.getPort(), "N/A");

    // Update internal resource types according to response.
    if (response.getResourceTypes() != null) {
      ResourceUtils.reinitializeResources(response.getResourceTypes());
    }

    if (response.getClientToAMTokenMasterKey() != null
        && response.getClientToAMTokenMasterKey().remaining() != 0) {
      context.secretManager
          .setMasterKey(response.getClientToAMTokenMasterKey().array());
    }
    registerServiceInstance(context.attemptId, app);

    // Since AM has been started and registered, the service is in STARTED state
    app.setState(ServiceState.STARTED);

    ServiceApiUtil.checkServiceDependencySatisified(context.service);

    // recover components based on containers sent from RM
    recoverComponents(response);

    for (Component component : componentsById.values()) {
      // Trigger initial evaluation of components
      if (component.areDependenciesReady()) {
        LOG.info("Triggering initial evaluation of component {}",
            component.getName());
        ComponentEvent event = new ComponentEvent(component.getName(), FLEX)
            .setDesired(component.getComponentSpec().getNumberOfContainers());
        component.handle(event);
      }
    }
  }

  private void recoverComponents(RegisterApplicationMasterResponse response) {
    List<Container> containersFromPrevAttempt = response
        .getContainersFromPreviousAttempts();
    LOG.info("Received {} containers from previous attempt.",
        containersFromPrevAttempt.size());
    Map<String, ServiceRecord> existingRecords = new HashMap<>();
    List<String> existingComps = null;
    try {
      existingComps = yarnRegistryOperations.listComponents();
      LOG.info("Found {} containers from ZK registry: {}", existingComps.size(),
          existingComps);
    } catch (Exception e) {
      LOG.info("Could not read component paths: {}", e.getMessage());
    }
    if (existingComps != null) {
      for (String existingComp : existingComps) {
        try {
          ServiceRecord record =
              yarnRegistryOperations.getComponent(existingComp);
          existingRecords.put(existingComp, record);
        } catch (Exception e) {
          LOG.warn("Could not resolve record for component {}: {}",
              existingComp, e);
        }
      }
    }
    for (Container container : containersFromPrevAttempt) {
      LOG.info("Handling {} from previous attempt", container.getId());
      ServiceRecord record = existingRecords.remove(RegistryPathUtils
          .encodeYarnID(container.getId().toString()));
      if (record != null) {
        Component comp = componentsById.get(container.getAllocationRequestId());
        ComponentEvent event =
            new ComponentEvent(comp.getName(), CONTAINER_RECOVERED)
                .setContainer(container)
                .setInstance(comp.getComponentInstance(record.description));
        comp.handle(event);
        // do not remove requests in this case because we do not know if they
        // have already been removed
      } else {
        LOG.info("Record not found in registry for container {} from previous" +
            " attempt, releasing", container.getId());
        amRMClient.releaseAssignedContainer(container.getId());
      }
    }
    ApplicationId appId = ApplicationId.fromString(app.getId());
    existingRecords.forEach((encodedContainerId, record) -> {
      String componentName = record.get(YarnRegistryAttributes.YARN_COMPONENT);
      if (componentName != null) {
        Component component = componentsByName.get(componentName);
        if (component != null) {
          ComponentInstance compInstance = component.getComponentInstance(
              record.description);
          ContainerId containerId = ContainerId.fromString(record.get(
              YarnRegistryAttributes.YARN_ID));
          if (containerId.getApplicationAttemptId().getApplicationId()
              .equals(appId)) {
            unRecoveredInstances.put(containerId, compInstance);
            component.removePendingInstance(compInstance);
          }
        }
      }
    });

    if (!unRecoveredInstances.isEmpty()) {
      executorService.schedule(() -> {
        synchronized (unRecoveredInstances) {
          // after containerRecoveryTimeout, all the containers that haven't be
          // recovered by the RM will released. The corresponding Component
          // Instances are added to the pending queues of their respective
          // component.
          unRecoveredInstances.forEach((containerId, instance) -> {
            LOG.info("{}, wait on container {} expired",
                instance.getCompInstanceId(), containerId);
            instance.cleanupRegistryAndCompHdfsDir(containerId);
            Component component = componentsByName.get(instance.getCompName());
            component.requestContainers(1);
            component.reInsertPendingInstance(instance);
            amRMClient.releaseAssignedContainer(containerId);
          });
          unRecoveredInstances.clear();
        }
      }, containerRecoveryTimeout, TimeUnit.MILLISECONDS);
    }
  }

  private void initGlobalTokensForSubstitute(ServiceContext context) {
    // ZK
    globalTokens.put(ServiceApiConstants.CLUSTER_ZK_QUORUM, getConfig()
        .getTrimmed(KEY_REGISTRY_ZK_QUORUM, DEFAULT_REGISTRY_ZK_QUORUM));
    String user = RegistryUtils.currentUser();
    globalTokens.put(SERVICE_ZK_PATH,
        ServiceRegistryUtils.mkServiceHomePath(user, app.getName()));

    globalTokens.put(ServiceApiConstants.USER, user);
    String dnsDomain = getConfig().getTrimmed(KEY_DNS_DOMAIN);
    if (dnsDomain != null && !dnsDomain.isEmpty()) {
      globalTokens.put(ServiceApiConstants.DOMAIN, dnsDomain);
    }
    // HDFS
    String clusterFs = getConfig().getTrimmed(FS_DEFAULT_NAME_KEY);
    if (clusterFs != null && !clusterFs.isEmpty()) {
      globalTokens.put(ServiceApiConstants.CLUSTER_FS_URI, clusterFs);
      globalTokens.put(ServiceApiConstants.CLUSTER_FS_HOST,
          URI.create(clusterFs).getHost());
    }
    globalTokens.put(SERVICE_HDFS_DIR, context.serviceHdfsDir);
    // service name
    globalTokens.put(SERVICE_NAME_LC, app.getName().toLowerCase());
    globalTokens.put(SERVICE_NAME, app.getName());
  }

  private void createConfigFileCache(final FileSystem fileSystem) {
    this.configFileCache =
        CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES)
            .build(new CacheLoader<ConfigFile, Object>() {
              @Override
              public Object load(ConfigFile key) throws Exception {
                switch (key.getType()) {
                case HADOOP_XML:
                  try (FSDataInputStream input = fileSystem
                      .open(new Path(key.getSrcFile()))) {
                    org.apache.hadoop.conf.Configuration confRead =
                        new org.apache.hadoop.conf.Configuration(false);
                    confRead.addResource(input);
                    Map<String, String> map = new HashMap<>(confRead.size());
                    for (Map.Entry<String, String> entry : confRead) {
                      map.put(entry.getKey(), entry.getValue());
                    }
                    return map;
                  }
                case TEMPLATE:
                  try (FSDataInputStream fileInput = fileSystem
                      .open(new Path(key.getSrcFile()))) {
                    return IOUtils.toString(fileInput, StandardCharsets.UTF_8);
                  }
                default:
                  return null;
                }
              }
            });
    context.configCache = configFileCache;
  }

  private void registerServiceInstance(ApplicationAttemptId attemptId,
      Service service) {
    LOG.info("Registering {}, {} into registry.", attemptId, service.getName());
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(YarnRegistryAttributes.YARN_ID,
        attemptId.getApplicationId().toString());
    serviceRecord.set(YarnRegistryAttributes.YARN_PERSISTENCE,
        PersistencePolicies.APPLICATION);
    serviceRecord.description = "YarnServiceMaster";

    executorService.submit(() -> {
      try {
        yarnRegistryOperations.registerSelf(serviceRecord, false);
        LOG.info("Registered service under {}; absolute path {}",
            yarnRegistryOperations.getSelfRegistrationPath(),
            yarnRegistryOperations.getAbsoluteSelfRegistrationPath());
        boolean isFirstAttempt = 1 == attemptId.getAttemptId();
        // delete the children in case there are any and this is an AM startup.
        // just to make sure everything underneath is purged
        if (isFirstAttempt) {
          yarnRegistryOperations.deleteChildren(
              yarnRegistryOperations.getSelfRegistrationPath(), true);
        }
      } catch (IOException e) {
        LOG.error("Failed to register app {} in registry.", app.getName(), e);
      }
    });
    if (YarnConfiguration.timelineServiceV2Enabled(getConfig())) {
      serviceTimelinePublisher.serviceAttemptRegistered(app, getConfig());
    }
  }

  private void createAllComponents() {
    long allocateId = 0;

    // sort components by dependencies
    Collection<org.apache.hadoop.yarn.service.api.records.Component> sortedComponents =
        ServiceApiUtil.sortByDependencies(app.getComponents());

    for (org.apache.hadoop.yarn.service.api.records.Component compSpec : sortedComponents) {
      Component component = new Component(compSpec, allocateId, context);
      componentsById.put(allocateId, component);
      componentsByName.put(component.getName(), component);
      allocateId++;
      if (!hasAtLeastOnePlacementConstraint
          && compSpec.getPlacementPolicy() != null
          && compSpec.getPlacementPolicy().getConstraints() != null
          && !compSpec.getPlacementPolicy().getConstraints().isEmpty()) {
        hasAtLeastOnePlacementConstraint = true;
      }
    }
  }

  private final class ServiceEventHandler
      implements EventHandler<ServiceEvent> {
    @Override
    public void handle(ServiceEvent event) {
      try {
        serviceManager.handle(event);
      } catch (Throwable t) {
        LOG.error(MessageFormat
            .format("[SERVICE]: Error in handling event type {0}",
                event.getType()), t);
      }
    }
  }

  private final class ComponentEventHandler
      implements EventHandler<ComponentEvent> {
    @Override
    public void handle(ComponentEvent event) {
      Component component = componentsByName.get(event.getName());

      if (component == null) {
        LOG.error("No component exists for {}.", event.getName());
        return;
      }
      try {
        component.handle(event);
      } catch (Throwable t) {
        LOG.error(MessageFormat
            .format("[COMPONENT {0}]: Error in handling event type {1}",
                component.getName(), event.getType()), t);
      }
    }
  }

  private final class ComponentInstanceEventHandler
      implements EventHandler<ComponentInstanceEvent> {
    @Override
    public void handle(ComponentInstanceEvent event) {
      ComponentInstance instance =
          liveInstances.get(event.getContainerId());
      if (instance == null) {
        LOG.error("No component instance exists for {}.", event.getContainerId());
        return;
      }
      try {
        instance.handle(event);
      } catch (Throwable t) {
        LOG.error("{} : Error in handling event type {}.",
            instance.getCompInstanceId(), event.getType(), t);
      }
    }
  }

  class AMRMClientCallback extends AMRMClientAsync.AbstractCallbackHandler {

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("{} containers allocated. ", containers.size());
      for (Container container : containers) {
        Component comp = componentsById.get(container.getAllocationRequestId());
        ComponentEvent event =
            new ComponentEvent(comp.getName(), CONTAINER_ALLOCATED)
                .setContainer(container);
        dispatcher.getEventHandler().handle(event);
        try {
          Collection<AMRMClient.ContainerRequest> requests = amRMClient
              .getMatchingRequests(container.getAllocationRequestId());
          LOG.info("[COMPONENT {}]: remove {} outstanding container requests " +
              "for allocateId {}.", comp.getName(), requests.size(),
              container.getAllocationRequestId());
          // remove the corresponding request
          if (requests.iterator().hasNext()) {
            AMRMClient.ContainerRequest request = requests.iterator().next();
            amRMClient.removeContainerRequest(request);
          }
        } catch(Exception e) {
          //TODO Due to YARN-7490, exception may be thrown, catch and ignore for
          //now.
          LOG.error("Exception when removing the matching requests. ", e);
        }
      }
    }


    @Override
    public void onContainersReceivedFromPreviousAttempts(
        List<Container> containers) {
      LOG.info("Containers recovered after AM registered: {}", containers);
      if (containers == null || containers.isEmpty()) {
        return;
      }
      for (Container container : containers) {
        ComponentInstance compInstance;
        synchronized (unRecoveredInstances) {
          compInstance = unRecoveredInstances.remove(container.getId());
        }
        if (compInstance != null) {
          Component component = componentsById.get(
              container.getAllocationRequestId());
          ComponentEvent event = new ComponentEvent(component.getName(),
              CONTAINER_RECOVERED)
              .setInstance(compInstance)
              .setContainerId(container.getId())
              .setContainer(container);
          component.handle(event);
        } else {
          LOG.info("Not waiting to recover container {}, releasing",
              container.getId());
          amRMClient.releaseAssignedContainer(container.getId());
        }
      }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus status : statuses) {
        ContainerId containerId = status.getContainerId();
        ComponentInstance instance = liveInstances.get(status.getContainerId());
        if (instance == null) {
          LOG.warn(
              "Container {} Completed. No component instance exists. exitStatus={}. diagnostics={} ",
              containerId, status.getExitStatus(), status.getDiagnostics());
          continue;
        }
        ComponentEvent event =
            new ComponentEvent(instance.getCompName(), CONTAINER_COMPLETED)
                .setStatus(status).setInstance(instance)
                .setContainerId(containerId);
        dispatcher.getEventHandler().handle(event);
      }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
    }

    @Override public void onShutdownRequest() {
      //Was used for non-work-preserving restart in YARN, should be deprecated.
    }

    @Override public void onNodesUpdated(List<NodeReport> updatedNodes) {
      StringBuilder str = new StringBuilder();
      str.append("Nodes updated info: ").append(System.lineSeparator());
      for (NodeReport report : updatedNodes) {
        str.append(report.getNodeId()).append(", state = ")
            .append(report.getNodeState()).append(", healthDiagnostics = ")
            .append(report.getHealthReport()).append(System.lineSeparator());
      }
      LOG.warn(str.toString());
    }

    @Override public float getProgress() {
      // get running containers over desired containers
      long total = 0;
      for (org.apache.hadoop.yarn.service.api.records.Component component : app
          .getComponents()) {
        total += component.getNumberOfContainers();
      }
      // Probably due to user flexed down to 0
      if (total == 0) {
        return 100;
      }
      return Math.max((float) liveInstances.size() / total * 100, 100);
    }

    @Override public void onError(Throwable e) {
      LOG.error("Error in AMRMClient callback handler ", e);
    }

    @Override
    public void onRequestsRejected(
        List<RejectedSchedulingRequest> rejectedSchedulingRequests) {
      LOG.error("Error in AMRMClient callback handler. Following scheduling "
          + "requests were rejected: {}", rejectedSchedulingRequests);
    }
  }

  private class NMClientCallback extends NMClientAsync.AbstractCallbackHandler {

    @Override public void onContainerStarted(ContainerId containerId,
        Map<String, ByteBuffer> allServiceResponse) {
      ComponentInstance instance = liveInstances.get(containerId);
      if (instance == null) {
        LOG.error("No component instance exists for {}.", containerId);
        return;
      }
      ComponentEvent event =
          new ComponentEvent(instance.getCompName(), CONTAINER_STARTED)
              .setInstance(instance).setContainerId(containerId);
      dispatcher.getEventHandler().handle(event);
    }

    @Override public void onContainerStatusReceived(ContainerId containerId,
        ContainerStatus containerStatus) {

    }

    @Override public void onContainerStopped(ContainerId containerId) {

    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      ComponentInstance instance = liveInstances.get(containerId);
      if (instance == null) {
        LOG.error("No component instance exists for {}.", containerId);
        return;
      }
      LOG.error("Failed to start {}.", containerId, t);
      amRMClient.releaseAssignedContainer(containerId);
      // After container released, it'll get CONTAINER_COMPLETED event from RM
      // automatically which will trigger stopping COMPONENT INSTANCE
    }

    @Override
    public void onContainerReInitialize(ContainerId containerId) {
      ComponentInstance instance = liveInstances.get(containerId);
      if (instance == null) {
        LOG.error("No component instance exists for {}", containerId);
        return;
      }
      dispatcher.getEventHandler().handle(
          new ComponentInstanceEvent(containerId, START));
    }

    @Override
    public void onContainerReInitializeError(ContainerId containerId,
        Throwable t) {
      ComponentInstance instance = liveInstances.get(containerId);
      if (instance == null) {
        LOG.error("No component instance exists for {}", containerId);
        return;
      }
      ComponentEvent event = new ComponentEvent(instance.getCompName(),
          ComponentEventType.CONTAINER_COMPLETED)
          .setInstance(instance).setContainerId(containerId);
      dispatcher.getEventHandler().handle(event);
    }

    @Override public void onContainerResourceIncreased(ContainerId containerId,
        Resource resource) {

    }

    @Override public void onContainerResourceUpdated(ContainerId containerId,
        Resource resource) {

    }

    @Override public void onGetContainerStatusError(ContainerId containerId,
        Throwable t) {

    }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId,
        Throwable t) {

    }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId,
        Throwable t) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {

    }
  }

  public ServiceMetrics getServiceMetrics() {
    return serviceMetrics;
  }

  public AMRMClientAsync<AMRMClient.ContainerRequest> getAmRMClient() {
    return amRMClient;
  }

  public NMClientAsync getNmClient() {
    return nmClient;
  }

  public void addLiveCompInstance(ContainerId containerId,
      ComponentInstance instance) {
    liveInstances.put(containerId, instance);
  }

  public void removeLiveCompInstance(ContainerId containerId) {
    liveInstances.remove(containerId);
  }

  public YarnRegistryViewForProviders getYarnRegistryOperations() {
    return yarnRegistryOperations;
  }

  public ServiceTimelinePublisher getServiceTimelinePublisher() {
    return serviceTimelinePublisher;
  }

  public Map<ContainerId, ComponentInstance> getLiveInstances() {
    return liveInstances;
  }

  public ContainerLaunchService getContainerLaunchService() {
    return containerLaunchService;
  }

  public ServiceContext getContext() {
    return context;
  }

  public Map<String, Component> getAllComponents() {
    return componentsByName;
  }

  public Service getApp() {
    return app;
  }

  public AsyncDispatcher getDispatcher() {
    return dispatcher;
  }

  public BoundedAppender getDiagnostics() {
    return diagnostics;
  }

  public boolean hasAtLeastOnePlacementConstraint() {
    return hasAtLeastOnePlacementConstraint;
  }

  public boolean terminateServiceIfNeeded(Component component) {
    return
        terminateServiceIfDominantComponentFinished(component) ||
            terminateServiceIfAllComponentsFinished();
  }

  /**
   * If the service state component is finished, the service is also terminated.
   * @param component service component.
   */
  private boolean terminateServiceIfDominantComponentFinished(Component
      component) {
    boolean shouldTerminate = false;
    boolean componentIsDominant = component.getComponentSpec()
        .getConfiguration().getPropertyBool(
            CONTAINER_STATE_REPORT_AS_SERVICE_STATE, false);
    if (componentIsDominant) {
      ComponentRestartPolicy restartPolicy =
          component.getRestartPolicyHandler();
      if (restartPolicy.shouldTerminate(component)) {
        shouldTerminate = true;
        boolean isSucceeded = restartPolicy.hasCompletedSuccessfully(component);
        org.apache.hadoop.yarn.service.api.records.ComponentState state
            = isSucceeded ?
            org.apache.hadoop.yarn.service.api.records.ComponentState.SUCCEEDED
            : org.apache.hadoop.yarn.service.api.records.ComponentState.FAILED;
        LOG.info("{} Component state changed from {} to {}",
            component.getName(), component.getComponentSpec().getState(),
            state);
        component.getComponentSpec().setState(state);
        LOG.info("Dominate component {} finished, exiting Service Master... " +
            ", final status={}.", component.getName(), (isSucceeded ? "Succeeded" : "Failed"));
        terminateService(isSucceeded);
      }
    }
    return shouldTerminate;
  }

  /*
   * Check if all components of the scheduler finished.
   * If all components finished
   * (which #failed-instances + #suceeded-instances = #total-n-containers)
   * The service will be terminated.
  */
  private boolean terminateServiceIfAllComponentsFinished() {
    boolean shouldTerminate = true;

    // Succeeded comps and failed comps, for logging purposes.
    Set<String> succeededComponents = new HashSet<>();
    Set<String> failedComponents = new HashSet<>();

    for (Component comp : getAllComponents().values()) {
      ComponentRestartPolicy restartPolicy = comp.getRestartPolicyHandler();

      if (restartPolicy.shouldTerminate(comp)) {
        if (restartPolicy.hasCompletedSuccessfully(comp)) {
          LOG.info("{} Component state changed from {} to {}",
              comp.getName(), comp.getComponentSpec().getState(),
              org.apache.hadoop
                  .yarn.service.api.records.ComponentState.SUCCEEDED);
          comp.getComponentSpec().setState(org.apache.hadoop
              .yarn.service.api.records.ComponentState.SUCCEEDED);
        } else {
          LOG.info("{} Component state changed from {} to {}",
              comp.getName(), comp.getComponentSpec().getState(),
              org.apache.hadoop
                  .yarn.service.api.records.ComponentState.FAILED);
          comp.getComponentSpec().setState(org.apache.hadoop
              .yarn.service.api.records.ComponentState.FAILED);
        }

        if (isTimelineServiceEnabled()) {
          // record in ATS
          serviceTimelinePublisher.componentFinished(comp.getComponentSpec(),
              comp.getComponentSpec().getState(), systemClock.getTime());
        }
      } else {
        shouldTerminate = false;
        break;
      }

      long nFailed = comp.getNumFailedInstances();

      if (nFailed > 0) {
        failedComponents.add(comp.getName());
      } else {
        succeededComponents.add(comp.getName());
      }
    }

    if (shouldTerminate) {
      LOG.info("All component finished, exiting Service Master... " +
          ", final status={}", (failedComponents.isEmpty() ? "Succeeded" : "Failed"));
      LOG.info("Succeeded components: [" + StringUtils.join(succeededComponents, ",") + "]");
      LOG.info("Failed components: [" + StringUtils.join(failedComponents, ",") + "]");

      terminateService(failedComponents.isEmpty());
    }
    return shouldTerminate;
  }

  private void terminateService(boolean isSucceeded) {
    int exitStatus = EXIT_SUCCESS;
    if (isSucceeded) {
      setGracefulStop(FinalApplicationStatus.SUCCEEDED);
      app.setState(ServiceState.SUCCEEDED);
    } else {
      setGracefulStop(FinalApplicationStatus.FAILED);
      app.setState(ServiceState.FAILED);
      exitStatus = EXIT_FALSE;
    }

    getTerminationHandler().terminate(exitStatus);
  }

  public Clock getSystemClock() {
    return systemClock;
  }

  public boolean isTimelineServiceEnabled() {
    return timelineServiceEnabled;
  }

  public ServiceUtils.ProcessTerminationHandler getTerminationHandler() {
    return terminationHandler;
  }

  public void syncSysFs(Service yarnApp) {
    boolean success = true;
    Configuration conf = getConfig();
    String spec;
    boolean useKerberos = UserGroupInformation.isSecurityEnabled();
    boolean printSyncResult = false;
    try {
      String port = conf.get("yarn.nodemanager.webapp.address").split(":")[1];
      spec = ServiceApiUtil.jsonSerDeser.toJson(yarnApp);
      for (org.apache.hadoop.yarn.service.api.records.Component c :
          yarnApp.getComponents()) {
        Set<String> nodes = new HashSet<>();
        boolean update = Boolean.parseBoolean(c.getConfiguration()
            .getEnv(ApplicationConstants.Environment
                .YARN_CONTAINER_RUNTIME_YARN_SYSFS_ENABLE.name()));
        if (!update) {
          continue;
        }
        printSyncResult = true;
        for (org.apache.hadoop.yarn.service.api.records.Container container :
            c.getContainers()) {
          String bareHost = container.getBareHost();
          nodes.add(bareHost);
        }
        for (String bareHost : nodes) {
          StringBuilder requestPath = new StringBuilder();
          if (YarnConfiguration.useHttps(conf)) {
            requestPath.append(HTTPS_PREFIX);
          } else {
            requestPath.append(HTTP_PREFIX);
          }
          requestPath.append(bareHost)
              .append(":")
              .append(port)
              .append("/ws/v1/node/yarn/sysfs/")
              .append(UserGroupInformation.getCurrentUser()
                  .getShortUserName())
              .append("/")
              .append(yarnApp.getId());
          if (!useKerberos) {
            requestPath.append("?user.name=")
                .append(UserGroupInformation.getCurrentUser()
                    .getShortUserName());
          }
          Invocation.Builder builder = HttpUtil.connect(requestPath.toString());
          Response response = builder.put(Entity.entity(spec, MediaType.APPLICATION_JSON));
          if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            LOG.warn("Error synchronize YARN sysfs: {}.", response.readEntity(String.class));
            success = false;
          }
        }
      }
      if (printSyncResult && success) {
        LOG.info("YARN sysfs synchronized.");
      }
    } catch (IOException | URISyntaxException | InterruptedException e) {
      LOG.error("Fail to sync service spec.", e);
    }
  }
}
