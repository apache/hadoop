/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.server.appmaster;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.*;
import static org.apache.slider.common.Constants.HADOOP_JAAS_DEBUG;

import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSecretManager;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.webapp.WebAppException;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.proto.SliderClusterAPI;
import org.apache.slider.client.SliderYarnClientImpl;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.params.AbstractActionArgs;
import org.apache.slider.common.params.SliderAMArgs;
import org.apache.slider.common.params.SliderAMCreateAction;
import org.apache.slider.common.params.SliderActions;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.PortScanner;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.buildutils.InstanceIO;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.launch.CredentialUtils;
import org.apache.slider.core.main.ExitCodeProvider;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.main.ServiceLauncher;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.providers.ProviderCompleted;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderService;
import org.apache.slider.providers.SliderProviderFactory;
import org.apache.slider.providers.agent.AgentKeys;
import org.apache.slider.providers.agent.AgentProviderService;
import org.apache.slider.providers.slideram.SliderAMClientProvider;
import org.apache.slider.providers.slideram.SliderAMProviderService;
import org.apache.slider.server.appmaster.actions.ActionRegisterServiceInstance;
import org.apache.slider.server.appmaster.actions.EscalateOutstandingRequests;
import org.apache.slider.server.appmaster.actions.RegisterComponentInstance;
import org.apache.slider.server.appmaster.actions.QueueExecutor;
import org.apache.slider.server.appmaster.actions.QueueService;
import org.apache.slider.server.appmaster.actions.ActionStopSlider;
import org.apache.slider.server.appmaster.actions.ActionUpgradeContainers;
import org.apache.slider.server.appmaster.actions.AsyncAction;
import org.apache.slider.server.appmaster.actions.RenewingAction;
import org.apache.slider.server.appmaster.actions.ResetFailureWindow;
import org.apache.slider.server.appmaster.actions.ReviewAndFlexApplicationSize;
import org.apache.slider.server.appmaster.actions.UnregisterComponentInstance;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.management.YarnServiceHealthCheck;
import org.apache.slider.server.appmaster.monkey.ChaosKillAM;
import org.apache.slider.server.appmaster.monkey.ChaosKillContainer;
import org.apache.slider.server.appmaster.monkey.ChaosMonkeyService;
import org.apache.slider.server.appmaster.operations.AsyncRMOperationHandler;
import org.apache.slider.server.appmaster.operations.ProviderNotifyingOperationHandler;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.appmaster.rpc.SliderAMPolicyProvider;
import org.apache.slider.server.appmaster.rpc.SliderClusterProtocolPBImpl;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.rpc.SliderIPCService;
import org.apache.slider.server.appmaster.security.SecurityConfiguration;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ContainerAssignment;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.operations.RMOperationHandler;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.web.AgentService;
import org.apache.slider.server.appmaster.web.rest.InsecureAmFilterInitializer;
import org.apache.slider.server.appmaster.web.rest.agent.AgentWebApp;
import org.apache.slider.server.appmaster.web.SliderAMWebApp;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResouceContentCacheFactory;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.services.security.CertificateManager;
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;
import org.apache.slider.server.services.utility.WebAppService;
import org.apache.slider.server.services.workflow.ServiceThreadFactory;
import org.apache.slider.server.services.workflow.WorkflowExecutorService;
import org.apache.slider.server.services.workflow.WorkflowRpcService;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This is the AM, which directly implements the callbacks from the AM and NM
 */
public class SliderAppMaster extends AbstractSliderLaunchedService 
  implements AMRMClientAsync.CallbackHandler,
    NMClientAsync.CallbackHandler,
    RunService,
    SliderExitCodes,
    SliderKeys,
    ServiceStateChangeListener,
    RoleKeys,
    ProviderCompleted,
    AppMasterActionOperations {

  protected static final Logger log =
    LoggerFactory.getLogger(SliderAppMaster.class);

  /**
   * log for YARN events
   */
  protected static final Logger LOG_YARN = log;

  public static final String SERVICE_CLASSNAME_SHORT = "SliderAppMaster";
  public static final String SERVICE_CLASSNAME =
      "org.apache.slider.server.appmaster." + SERVICE_CLASSNAME_SHORT;

  public static final int HEARTBEAT_INTERVAL = 1000;
  public static final int NUM_RPC_HANDLERS = 5;

  /**
   * Metrics and monitoring services.
   * Deployed in {@link #serviceInit(Configuration)}
   */
  private final MetricsAndMonitoring metricsAndMonitoring = new MetricsAndMonitoring(); 

  /**
   * metrics registry
   */
  public MetricRegistry metrics;

  /** Error string on chaos monkey launch failure action: {@value} */
  public static final String E_TRIGGERED_LAUNCH_FAILURE =
      "Chaos monkey triggered launch failure";

  /** YARN RPC to communicate with the Resource Manager or Node Manager */
  private YarnRPC yarnRPC;

  /** Handle to communicate with the Resource Manager*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private AMRMClientAsync asyncRMClient;

  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RMOperationHandler rmOperationHandler;
  
  private RMOperationHandler providerRMOperationHandler;

  /** Handle to communicate with the Node Manager*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  public NMClientAsync nmClientAsync;

  /**
   * Credentials for propagating down to launched containers
   */
  private Credentials containerCredentials;

  /**
   * Slider IPC: Real service handler
   */
  private SliderIPCService sliderIPCService;
  /**
   * Slider IPC: binding
   */
  private WorkflowRpcService rpcService;

  /**
   * Secret manager
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ClientToAMTokenSecretManager secretManager;
  
  /** Hostname of the container*/
  private String appMasterHostname = "";
  /* Port on which the app master listens for status updates from clients*/
  private int appMasterRpcPort = 0;
  /** Tracking url to which app master publishes info for clients to monitor*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private String appMasterTrackingUrl = "";

  /** Proxied app master URL (as retrieved from AM report at launch time) */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private String appMasterProxiedUrl = "";

  /** Application Attempt Id ( combination of attemptId and fail count )*/
  private ApplicationAttemptId appAttemptID;

  /**
   * App ACLs
   */
  protected Map<ApplicationAccessType, String> applicationACLs;

  /**
   * Ongoing state of the cluster: containers, nodes they
   * live on, etc.
   */
  private final AppState appState =
      new AppState(new ProtobufClusterServices(), metricsAndMonitoring);

  /**
   * App state for external objects. This is almost entirely
   * a read-only view of the application state. To change the state,
   * Providers (or anything else) are expected to queue async changes.
   */
  private final ProviderAppState stateForProviders =
      new ProviderAppState("undefined", appState);

  /**
   * model the state using locks and conditions
   */
  private final ReentrantLock AMExecutionStateLock = new ReentrantLock();
  private final Condition isAMCompleted = AMExecutionStateLock.newCondition();

  /**
   * Flag set if the AM is to be shutdown
   */
  private final AtomicBoolean amCompletionFlag = new AtomicBoolean(false);

  /**
   * Flag set during the init process
   */
  private final AtomicBoolean initCompleted = new AtomicBoolean(false);

  /**
   * Flag to set if the process exit code was set before shutdown started
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private boolean spawnedProcessExitedBeforeShutdownTriggered;


  /** Arguments passed in : raw*/
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private SliderAMArgs serviceArgs;

  /**
   * ID of the AM container
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ContainerId appMasterContainerID;

  /**
   * Monkey Service -may be null
   */
  private ChaosMonkeyService monkey;
  
  /**
   * ProviderService of this cluster
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private ProviderService providerService;

  /**
   * The YARN registry service
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RegistryOperations registryOperations;

  /**
   * The stop request received...the exit details are extracted
   * from this
   */
  private volatile ActionStopSlider stopAction;

  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RoleLaunchService launchService;
  
  //username -null if it is not known/not to be set
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private String hadoop_user_name;
  private String service_user_name;
  
  private SliderAMWebApp webApp;
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private InetSocketAddress rpcServiceAddress;
  private SliderAMProviderService sliderAMProvider;
  private CertificateManager certificateManager;

  /**
   * Executor.
   * Assigned in {@link #serviceInit(Configuration)}
   */
  private WorkflowExecutorService<ExecutorService> executorService;

  /**
   * Action queues. Created at instance creation, but
   * added as a child and inited in {@link #serviceInit(Configuration)}
   */
  private final QueueService actionQueues = new QueueService();
  private String agentOpsUrl;
  private String agentStatusUrl;
  private YarnRegistryViewForProviders yarnRegistryOperations;
  //private FsDelegationTokenManager fsDelegationTokenManager;
  private RegisterApplicationMasterResponse amRegistrationData;
  private PortScanner portScanner;
  private SecurityConfiguration securityConfiguration;

  /**
   * Is security enabled?
   * Set early on in the {@link #createAndRunCluster(String)} operation.
   */
  private boolean securityEnabled;
  private ContentCache contentCache;

  /**
   * resource limits
   */
  private Resource maximumResourceCapability;

  /**
   * Service Constructor
   */
  public SliderAppMaster() {
    super(SERVICE_CLASSNAME_SHORT);
    new HdfsConfiguration();
    new YarnConfiguration();
  }

/* =================================================================== */
/* service lifecycle methods */
/* =================================================================== */

  @Override //AbstractService
  public synchronized void serviceInit(Configuration conf) throws Exception {
    // slider client if found
    
    Configuration customConf = SliderUtils.loadSliderClientXML();
    // Load in the server configuration - if it is actually on the Classpath
    URL serverXmlUrl = ConfigHelper.getResourceUrl(SLIDER_SERVER_XML);
    if (serverXmlUrl != null) {
      log.info("Loading {} at {}", SLIDER_SERVER_XML, serverXmlUrl);
      Configuration serverConf = ConfigHelper.loadFromResource(SLIDER_SERVER_XML);
      ConfigHelper.mergeConfigurations(customConf, serverConf,
          SLIDER_SERVER_XML, true);
    }
    serviceArgs.applyDefinitions(customConf);
    serviceArgs.applyFileSystemBinding(customConf);
    // conf now contains all customizations

    AbstractActionArgs action = serviceArgs.getCoreAction();
    SliderAMCreateAction createAction = (SliderAMCreateAction) action;

    // sort out the location of the AM
    String rmAddress = createAction.getRmAddress();
    if (rmAddress != null) {
      log.debug("Setting RM address from the command line: {}", rmAddress);
      SliderUtils.setRmSchedulerAddress(customConf, rmAddress);
    }

    log.info("AM configuration:\n{}",
        ConfigHelper.dumpConfigToString(customConf));
    for (Map.Entry<String, String> envs : System.getenv().entrySet()) {
      log.info("System env {}={}", envs.getKey(), envs.getValue());
    }

    ConfigHelper.mergeConfigurations(conf, customConf, SLIDER_CLIENT_XML, true);
    //init security with our conf
    if (SliderUtils.isHadoopClusterSecure(conf)) {
      log.info("Secure mode with kerberos realm {}",
               SliderUtils.getKerberosRealm());
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      log.debug("Authenticating as {}", ugi);
      SliderUtils.verifyPrincipalSet(conf, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    } else {
      log.info("Cluster is insecure");
    }
    log.info("Login user is {}", UserGroupInformation.getLoginUser());

    //look at settings of Hadoop Auth, to pick up a problem seen once
    checkAndWarnForAuthTokenProblems();
    
    // validate server env
    boolean dependencyChecks =
        !conf.getBoolean(KEY_SLIDER_AM_DEPENDENCY_CHECKS_DISABLED,
            false);
    SliderUtils.validateSliderServerEnvironment(log, dependencyChecks);

    // create and register monitoring services
    addService(metricsAndMonitoring);
    metrics = metricsAndMonitoring.getMetrics();
/* TODO: turn these one once the metrics testing is more under control
    metrics.registerAll(new ThreadStatesGaugeSet());
    metrics.registerAll(new MemoryUsageGaugeSet());
    metrics.registerAll(new GarbageCollectorMetricSet());

*/
    contentCache = ApplicationResouceContentCacheFactory.createContentCache(stateForProviders);

    executorService = new WorkflowExecutorService<>("AmExecutor",
        Executors.newFixedThreadPool(2,
            new ServiceThreadFactory("AmExecutor", true)));
    addService(executorService);

    addService(actionQueues);

    //init all child services
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    HealthCheckRegistry health = metricsAndMonitoring.getHealth();
    health.register("AM Health", new YarnServiceHealthCheck(this));
  }

  /**
   * Start the queue processing
   */
  private void startQueueProcessing() {
    log.info("Queue Processing started");
    executorService.execute(actionQueues);
    executorService.execute(new QueueExecutor(this, actionQueues));
  }
  
/* =================================================================== */
/* RunService methods called from ServiceLauncher */
/* =================================================================== */

  /**
   * pick up the args from the service launcher
   * @param config configuration
   * @param args argument list
   */
  @Override // RunService
  public Configuration bindArgs(Configuration config, String... args) throws Exception {
    // let the superclass process it
    Configuration superConf = super.bindArgs(config, args);
    // add the slider XML config
    ConfigHelper.injectSliderXMLResource();

    //yarn-ify
    YarnConfiguration yarnConfiguration = new YarnConfiguration(
        superConf);
    serviceArgs = new SliderAMArgs(args);
    serviceArgs.parse();

    return SliderUtils.patchConfiguration(yarnConfiguration);
  }


  /**
   * this is called by service launcher; when it returns the application finishes
   * @return the exit code to return by the app
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {
    SliderVersionInfo.loadAndPrintVersionInfo(log);

    //dump the system properties if in debug mode
    if (log.isDebugEnabled()) {
      log.debug("System properties:\n" + SliderUtils.propertiesToString(System.getProperties()));
    }

    //choose the action
    String action = serviceArgs.getAction();
    List<String> actionArgs = serviceArgs.getActionArgs();
    int exitCode;
    switch (action) {
      case SliderActions.ACTION_HELP:
        log.info("{}: {}", getName(), serviceArgs.usage());
        exitCode = SliderExitCodes.EXIT_USAGE;
        break;
      case SliderActions.ACTION_CREATE:
        exitCode = createAndRunCluster(actionArgs.get(0));
        break;
      default:
        throw new SliderException("Unimplemented: " + action);
    }
    log.info("Exiting AM; final exit code = {}", exitCode);
    return exitCode;
  }

  /**
   * Initialize a newly created service then add it. 
   * Because the service is not started, this MUST be done before
   * the AM itself starts, or it is explicitly added after
   * @param service the service to init
   */
  public Service initAndAddService(Service service) {
    service.init(getConfig());
    addService(service);
    return service;
  }

  /* =================================================================== */

  /**
   * Create and run the cluster.
   * @param clustername cluster name
   * @return exit code
   * @throws Throwable on a failure
   */
  private int createAndRunCluster(String clustername) throws Throwable {

    //load the cluster description from the cd argument
    String sliderClusterDir = serviceArgs.getSliderClusterURI();
    URI sliderClusterURI = new URI(sliderClusterDir);
    Path clusterDirPath = new Path(sliderClusterURI);
    log.info("Application defined at {}", sliderClusterURI);
    SliderFileSystem fs = getClusterFS();

    // build up information about the running application -this
    // will be passed down to the cluster status
    MapOperations appInformation = new MapOperations(); 

    AggregateConf instanceDefinition =
      InstanceIO.loadInstanceDefinitionUnresolved(fs, clusterDirPath);
    instanceDefinition.setName(clustername);

    log.info("Deploying cluster {}:", instanceDefinition);

    // and resolve it
    AggregateConf resolvedInstance = new AggregateConf( instanceDefinition);
    resolvedInstance.resolve();

    stateForProviders.setApplicationName(clustername);

    Configuration serviceConf = getConfig();

    // extend AM configuration with component resource
    MapOperations amConfiguration = resolvedInstance
      .getAppConfOperations().getComponent(COMPONENT_AM);
    // and patch configuration with prefix
    if (amConfiguration != null) {
      Map<String, String> sliderAppConfKeys = amConfiguration.prefixedWith("slider.");
      for (Map.Entry<String, String> entry : sliderAppConfKeys.entrySet()) {
        String k = entry.getKey();
        String v = entry.getValue();
        boolean exists = serviceConf.get(k) != null;
        log.info("{} {} to {}", (exists ? "Overwriting" : "Setting"), k, v);
        serviceConf.set(k, v);
      }
    }

    securityConfiguration = new SecurityConfiguration(serviceConf, resolvedInstance, clustername);
    // obtain security state
    securityEnabled = securityConfiguration.isSecurityEnabled();
    // set the global security flag for the instance definition
    instanceDefinition.getAppConfOperations().set(KEY_SECURITY_ENABLED, securityEnabled);

    // triggers resolution and snapshotting for agent
    appState.setInitialInstanceDefinition(instanceDefinition);

    File confDir = getLocalConfDir();
    if (!confDir.exists() || !confDir.isDirectory()) {
      log.info("Conf dir {} does not exist.", confDir);
      File parentFile = confDir.getParentFile();
      log.info("Parent dir {}:\n{}", parentFile, SliderUtils.listDir(parentFile));
    }
    
    //get our provider
    MapOperations globalInternalOptions = getGlobalInternalOptions();
    String providerType = globalInternalOptions.getMandatoryOption(
      InternalKeys.INTERNAL_PROVIDER_NAME);
    log.info("Cluster provider type is {}", providerType);
    SliderProviderFactory factory =
      SliderProviderFactory.createSliderProviderFactory(providerType);
    providerService = factory.createServerProvider();
    // init the provider BUT DO NOT START IT YET
    initAndAddService(providerService);
    providerRMOperationHandler = new ProviderNotifyingOperationHandler(providerService);
    
    // create a slider AM provider
    sliderAMProvider = new SliderAMProviderService();
    initAndAddService(sliderAMProvider);
    
    InetSocketAddress rmSchedulerAddress = SliderUtils.getRmSchedulerAddress(serviceConf);
    log.info("RM is at {}", rmSchedulerAddress);
    yarnRPC = YarnRPC.create(serviceConf);

    // set up the YARN client. This may require patching in the RM client-API address if it
    // is (somehow) unset server-side.    String clientRMaddr = serviceConf.get(YarnConfiguration.RM_ADDRESS);
    InetSocketAddress clientRpcAddress = SliderUtils.getRmAddress(serviceConf);
    if (!SliderUtils.isAddressDefined(clientRpcAddress)) {
      // client addr is being unset. We can lift it from the other RM APIs
      log.warn("Yarn RM address was unbound; attempting to fix up");
      serviceConf.set(YarnConfiguration.RM_ADDRESS,
          String.format("%s:%d", rmSchedulerAddress.getHostString(), clientRpcAddress.getPort() ));
    }

    /*
     * Extract the container ID. This is then
     * turned into an (incomplete) container
     */
    appMasterContainerID = ConverterUtils.toContainerId(
      SliderUtils.mandatoryEnvVariable(ApplicationConstants.Environment.CONTAINER_ID.name()));
    appAttemptID = appMasterContainerID.getApplicationAttemptId();

    ApplicationId appid = appAttemptID.getApplicationId();
    log.info("AM for ID {}", appid.getId());

    appInformation.put(StatusKeys.INFO_AM_CONTAINER_ID, appMasterContainerID.toString());
    appInformation.put(StatusKeys.INFO_AM_APP_ID, appid.toString());
    appInformation.put(StatusKeys.INFO_AM_ATTEMPT_ID, appAttemptID.toString());

    Map<String, String> envVars;
    List<Container> liveContainers;

    /*
     * It is critical this section is synchronized, to stop async AM events
     * arriving while registering a restarting AM.
     */
    synchronized (appState) {
      int heartbeatInterval = HEARTBEAT_INTERVAL;

      // add the RM client -this brings the callbacks in
      asyncRMClient = AMRMClientAsync.createAMRMClientAsync(heartbeatInterval, this);
      addService(asyncRMClient);
      //now bring it up
      deployChildService(asyncRMClient);


      // nmclient relays callbacks back to this class
      nmClientAsync = new NMClientAsyncImpl("nmclient", this);
      deployChildService(nmClientAsync);

      // set up secret manager
      secretManager = new ClientToAMTokenSecretManager(appAttemptID, null);

      if (securityEnabled) {
        // fix up the ACLs if they are not set
        String acls = serviceConf.get(KEY_PROTOCOL_ACL);
        if (acls == null) {
          getConfig().set(KEY_PROTOCOL_ACL, "*");
        }
      }

      certificateManager = new CertificateManager();

      //bring up the Slider RPC service
      buildPortScanner(instanceDefinition);
      startSliderRPCServer(instanceDefinition);

      rpcServiceAddress = rpcService.getConnectAddress();
      appMasterHostname = rpcServiceAddress.getAddress().getCanonicalHostName();
      appMasterRpcPort = rpcServiceAddress.getPort();
      appMasterTrackingUrl = null;
      log.info("AM Server is listening at {}:{}", appMasterHostname, appMasterRpcPort);
      appInformation.put(StatusKeys.INFO_AM_HOSTNAME, appMasterHostname);
      appInformation.set(StatusKeys.INFO_AM_RPC_PORT, appMasterRpcPort);

      log.info("Starting Yarn registry");
      registryOperations = startRegistryOperationsService();
      log.info(registryOperations.toString());

      //build the role map
      List<ProviderRole> providerRoles = new ArrayList<>(providerService.getRoles());
      providerRoles.addAll(SliderAMClientProvider.ROLES);

      // Start up the WebApp and track the URL for it
      MapOperations component = instanceDefinition.getAppConfOperations()
          .getComponent(SliderKeys.COMPONENT_AM);
      certificateManager.initialize(component, appMasterHostname,
                                    appMasterContainerID.toString(),
                                    clustername);
      certificateManager.setPassphrase(instanceDefinition.getPassphrase());
 
      if (component.getOptionBool(
          AgentKeys.KEY_AGENT_TWO_WAY_SSL_ENABLED, false)) {
        uploadServerCertForLocalization(clustername, fs);
      }

      // Web service endpoints: initialize
      WebAppApiImpl webAppApi =
          new WebAppApiImpl(
              stateForProviders,
              providerService,
              certificateManager,
              registryOperations,
              metricsAndMonitoring,
              actionQueues,
              this,
              contentCache);
      initAMFilterOptions(serviceConf);

      // start the agent web app
      startAgentWebApp(appInformation, serviceConf, webAppApi);
      int webAppPort = deployWebApplication(webAppApi);

      String scheme = WebAppUtils.HTTP_PREFIX;
      appMasterTrackingUrl = scheme + appMasterHostname + ":" + webAppPort;

      appInformation.put(StatusKeys.INFO_AM_WEB_URL, appMasterTrackingUrl + "/");
      appInformation.set(StatusKeys.INFO_AM_WEB_PORT, webAppPort);

      // *****************************************************
      // Register self with ResourceManager
      // This will start heartbeating to the RM
      // address = SliderUtils.getRmSchedulerAddress(asyncRMClient.getConfig());
      // *****************************************************
      log.info("Connecting to RM at {}; AM tracking URL={}",
               appMasterRpcPort, appMasterTrackingUrl);
      amRegistrationData = asyncRMClient.registerApplicationMaster(appMasterHostname,
                                   appMasterRpcPort,
                                   appMasterTrackingUrl);
      maximumResourceCapability = amRegistrationData.getMaximumResourceCapability();

      int minMemory = serviceConf.getInt(RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
          DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
       // validate scheduler vcores allocation setting
      int minCores = serviceConf.getInt(RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
          DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
      int maxMemory = maximumResourceCapability.getMemory();
      int maxCores = maximumResourceCapability.getVirtualCores();
      appState.setContainerLimits(minMemory,maxMemory, minCores, maxCores );

      // build the handler for RM request/release operations; this uses
      // the max value as part of its lookup
      rmOperationHandler = new AsyncRMOperationHandler(asyncRMClient, maximumResourceCapability);

      // set the RM-defined maximum cluster values
      appInformation.put(ResourceKeys.YARN_CORES, Integer.toString(maxCores));
      appInformation.put(ResourceKeys.YARN_MEMORY, Integer.toString(maxMemory));

      processAMCredentials(securityConfiguration);

      if (securityEnabled) {
        secretManager.setMasterKey(
            amRegistrationData.getClientToAMTokenMasterKey().array());
        applicationACLs = amRegistrationData.getApplicationACLs();

        //tell the server what the ACLs are
        rpcService.getServer().refreshServiceAcl(serviceConf,
            new SliderAMPolicyProvider());
        if (securityConfiguration.isKeytabProvided()) {
          // perform keytab based login to establish kerberos authenticated
          // principal.  Can do so now since AM registration with RM above required
          // tokens associated to principal
          String principal = securityConfiguration.getPrincipal();
          File localKeytabFile = securityConfiguration.getKeytabFile(instanceDefinition);
          // Now log in...
          login(principal, localKeytabFile);
          // obtain new FS reference that should be kerberos based and different
          // than the previously cached reference
          fs = new SliderFileSystem(serviceConf);
        }
      }

      // YARN client.
      // Important: this is only valid at startup, and must be executed within
      // the right UGI context. Use with care.
      SliderYarnClientImpl yarnClient = null;
      List<NodeReport> nodeReports;
      try {
        yarnClient = new SliderYarnClientImpl();
        yarnClient.init(getConfig());
        yarnClient.start();
        nodeReports = getNodeReports(yarnClient);
        log.info("Yarn node report count: {}", nodeReports.size());
        // look up the application itself -this is needed to get the proxied
        // URL of the AM, for registering endpoints.
        // this call must be made after the AM has registered itself, obviously
        ApplicationAttemptReport report = getApplicationAttemptReport(yarnClient);
        appMasterProxiedUrl = report.getTrackingUrl();
        if (SliderUtils.isUnset(appMasterProxiedUrl)) {
          log.warn("Proxied URL is not set in application report");
          appMasterProxiedUrl = appMasterTrackingUrl;
        }
      } finally {
        // at this point yarnClient is no longer needed.
        // stop it immediately
        ServiceOperations.stop(yarnClient);
        yarnClient = null;
      }

      // extract container list

      liveContainers = amRegistrationData.getContainersFromPreviousAttempts();

      //now validate the installation
      Configuration providerConf =
        providerService.loadProviderConfigurationInformation(confDir);

      providerService.initializeApplicationConfiguration(instanceDefinition,
          fs, null);

      providerService.validateApplicationConfiguration(instanceDefinition,
          confDir,
          securityEnabled);

      //determine the location for the role history data
      Path historyDir = new Path(clusterDirPath, HISTORY_DIR_NAME);

      //build the instance
      AppStateBindingInfo binding = new AppStateBindingInfo();
      binding.instanceDefinition = instanceDefinition;
      binding.serviceConfig = serviceConf;
      binding.publishedProviderConf = providerConf;
      binding.roles = providerRoles;
      binding.fs = fs.getFileSystem();
      binding.historyPath = historyDir;
      binding.liveContainers = liveContainers;
      binding.applicationInfo = appInformation;
      binding.releaseSelector = providerService.createContainerReleaseSelector();
      binding.nodeReports = nodeReports;
      appState.buildInstance(binding);

      providerService.rebuildContainerDetails(liveContainers,
          instanceDefinition.getName(), appState.getRolePriorityMap());

      // add the AM to the list of nodes in the cluster

      appState.buildAppMasterNode(appMasterContainerID,
          appMasterHostname,
          webAppPort,
          appMasterHostname + ":" + webAppPort);

      // build up environment variables that the AM wants set in every container
      // irrespective of provider and role.
      envVars = new HashMap<>();
      if (hadoop_user_name != null) {
        envVars.put(HADOOP_USER_NAME, hadoop_user_name);
      }
      String debug_kerberos = System.getenv(HADOOP_JAAS_DEBUG);
      if (debug_kerberos != null) {
        envVars.put(HADOOP_JAAS_DEBUG, debug_kerberos);
      }
    }
    String rolesTmpSubdir = appMasterContainerID.toString() + "/roles";

    String amTmpDir = globalInternalOptions.getMandatoryOption(InternalKeys.INTERNAL_AM_TMP_DIR);

    Path tmpDirPath = new Path(amTmpDir);
    Path launcherTmpDirPath = new Path(tmpDirPath, rolesTmpSubdir);
    fs.getFileSystem().mkdirs(launcherTmpDirPath);

    //launcher service
    launchService = new RoleLaunchService(actionQueues,
                                          providerService,
                                          fs,
                                          new Path(getGeneratedConfDir()),
                                          envVars,
                                          launcherTmpDirPath);

    deployChildService(launchService);

    appState.noteAMLaunched();


    //Give the provider access to the state, and AM
    providerService.bind(stateForProviders, actionQueues, liveContainers);
    sliderAMProvider.bind(stateForProviders, actionQueues, liveContainers);

    // chaos monkey
    maybeStartMonkey();

    // setup token renewal and expiry handling for long lived apps
//    if (!securityConfiguration.isKeytabProvided() &&
//        SliderUtils.isHadoopClusterSecure(getConfig())) {
//      fsDelegationTokenManager = new FsDelegationTokenManager(actionQueues);
//      fsDelegationTokenManager.acquireDelegationToken(getConfig());
//    }

    // if not a secure cluster, extract the username -it will be
    // propagated to workers
    if (!UserGroupInformation.isSecurityEnabled()) {
      hadoop_user_name = System.getenv(HADOOP_USER_NAME);
      log.info(HADOOP_USER_NAME + "='{}'", hadoop_user_name);
    }
    service_user_name = RegistryUtils.currentUser();
    log.info("Registry service username ={}", service_user_name);


    // declare the cluster initialized
    log.info("Application Master Initialization Completed");
    initCompleted.set(true);

    scheduleFailureWindowResets(instanceDefinition.getResources());
    scheduleEscalation(instanceDefinition.getInternal());

    try {
      // schedule YARN Registry registration
      queue(new ActionRegisterServiceInstance(clustername, appid));

      // log the YARN and web UIs
      log.info("RM Webapp address {}",
          serviceConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS));
      log.info("Slider webapp address {} proxied at {}",
        appMasterTrackingUrl, appMasterProxiedUrl);

      // Start the Slider AM provider
      sliderAMProvider.start();

      // launch the real provider; this is expected to trigger a callback that
      // starts the node review process
      launchProviderService(instanceDefinition, confDir);

      // start handling any scheduled events

      startQueueProcessing();

      //now block waiting to be told to exit the process
      waitForAMCompletionSignal();
    } catch(Exception e) {
      log.error("Exception : {}", e, e);
      // call the AM stop command as if it had been queued (but without
      // going via the queue, which may not have started
      onAMStop(new ActionStopSlider(e));
    }
    //shutdown time
    return finish();
  }

  /**
   * Get the YARN application Attempt report as the logged in user
   * @param yarnClient client to the RM
   * @return the application report
   * @throws YarnException
   * @throws IOException
   * @throws InterruptedException
   */
  private ApplicationAttemptReport getApplicationAttemptReport(
    final SliderYarnClientImpl yarnClient)
      throws YarnException, IOException, InterruptedException {
    Preconditions.checkNotNull(yarnClient, "Null Yarn client");
    ApplicationAttemptReport report;
    if (securityEnabled) {
      UserGroupInformation ugi = UserGroupInformation.getLoginUser();
      report = ugi.doAs(new PrivilegedExceptionAction<ApplicationAttemptReport>() {
        @Override
        public ApplicationAttemptReport run() throws Exception {
          return yarnClient.getApplicationAttemptReport(appAttemptID);
        }
      });
    } else {
      report = yarnClient.getApplicationAttemptReport(appAttemptID);
    }
    return report;
  }

  /**
   * List the node reports: uses {@link SliderYarnClientImpl} as the login user
   * @param yarnClient client to the RM
   * @return the node reports
   * @throws IOException
   * @throws YarnException
   * @throws InterruptedException
   */
  private List<NodeReport> getNodeReports(final SliderYarnClientImpl yarnClient)
    throws IOException, YarnException, InterruptedException {
    Preconditions.checkNotNull(yarnClient, "Null Yarn client");
    List<NodeReport> nodeReports;
    if (securityEnabled) {
      nodeReports = UserGroupInformation.getLoginUser().doAs(
        new PrivilegedExceptionAction<List<NodeReport>>() {
          @Override
          public List<NodeReport> run() throws Exception {
            return yarnClient.getNodeReports(NodeState.RUNNING);
          }
        });
    } else {
      nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    }
    log.info("Yarn node report count: {}", nodeReports.size());
    return nodeReports;
  }

  /**
   * Deploy the web application.
   * <p>
   *   Creates and starts the web application, and adds a
   *   <code>WebAppService</code> service under the AM, to ensure
   *   a managed web application shutdown.
   * @param webAppApi web app API instance
   * @return port the web application is deployed on
   * @throws IOException general problems starting the webapp (network, etc)
   * @throws WebAppException other issues
   */
  private int deployWebApplication(WebAppApiImpl webAppApi)
      throws IOException, SliderException {

    try {
      webApp = new SliderAMWebApp(webAppApi);
      HttpConfig.Policy policy = HttpConfig.Policy.HTTP_ONLY;
      int port = getPortToRequest();
      log.info("Launching web application at port {} with policy {}", port, policy);

      WebApps.$for(SliderAMWebApp.BASE_PATH,
          WebAppApi.class,
          webAppApi,
          RestPaths.WS_CONTEXT)
             .withHttpPolicy(getConfig(), policy)
             .at("0.0.0.0", port, true)
             .inDevMode()
             .start(webApp);

      WebAppService<SliderAMWebApp> webAppService =
        new WebAppService<>("slider", webApp);

      deployChildService(webAppService);
      return webApp.port();
    } catch (WebAppException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException)e.getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Process the initial user to obtain the set of user
   * supplied credentials (tokens were passed in by client).
   * Removes the AM/RM token.
   * If a keytab has been provided, also strip the HDFS delegation token.
   * @param securityConfig slider security config
   * @throws IOException
   */
  private void processAMCredentials(SecurityConfiguration securityConfig)
      throws IOException {

    List<Text> filteredTokens = new ArrayList<>(3);
    filteredTokens.add(AMRMTokenIdentifier.KIND_NAME);
    filteredTokens.add(TimelineDelegationTokenIdentifier.KIND_NAME);

    boolean keytabProvided = securityConfig.isKeytabProvided();
    log.info("Slider AM Security Mode: {}", keytabProvided ? "KEYTAB" : "TOKEN");
    if (keytabProvided) {
      filteredTokens.add(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    }
    containerCredentials = CredentialUtils.filterTokens(
        UserGroupInformation.getCurrentUser().getCredentials(),
        filteredTokens);
    log.info(CredentialUtils.dumpTokens(containerCredentials, "\n"));
  }

  /**
   * Build up the port scanner. This may include setting a port range.
   */
  private void buildPortScanner(AggregateConf instanceDefinition)
      throws BadConfigException {
    portScanner = new PortScanner();
    String portRange = instanceDefinition.
        getAppConfOperations().getGlobalOptions().
          getOption(SliderKeys.KEY_ALLOWED_PORT_RANGE, "0");
    if (!"0".equals(portRange)) {
        portScanner.setPortRange(portRange);
    }
  }
  
  /**
   * Locate a port to request for a service such as RPC or web/REST.
   * This uses port range definitions in the <code>instanceDefinition</code>
   * to fix the port range —if one is set.
   * <p>
   * The port returned is available at the time of the request; there are
   * no guarantees as to how long that situation will last.
   * @return the port to request.
   * @throws SliderException
   */
  private int getPortToRequest() throws SliderException, IOException {
    return portScanner.getAvailablePort();
  }

  private void uploadServerCertForLocalization(String clustername,
                                               SliderFileSystem fs)
      throws IOException {
    Path certsDir = fs.buildClusterSecurityDirPath(clustername);
    if (!fs.getFileSystem().exists(certsDir)) {
      fs.getFileSystem().mkdirs(certsDir,
        new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    }
    Path destPath = new Path(certsDir, SliderKeys.CRT_FILE_NAME);
    if (!fs.getFileSystem().exists(destPath)) {
      fs.getFileSystem().copyFromLocalFile(
          new Path(CertificateManager.getServerCertficateFilePath().getAbsolutePath()),
          destPath);
      log.info("Uploaded server cert to localization path {}", destPath);
    }

    fs.getFileSystem().setPermission(destPath,
        new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));
  }

  protected void login(String principal, File localKeytabFile)
      throws IOException, SliderException {
    log.info("Logging in as {} with keytab {}", principal, localKeytabFile);
    UserGroupInformation.loginUserFromKeytab(principal,
                                             localKeytabFile.getAbsolutePath());
    validateLoginUser(UserGroupInformation.getLoginUser());
  }

  /**
   * Ensure that the user is generated from a keytab and has no HDFS delegation
   * tokens.
   *
   * @param user user to validate
   * @throws SliderException
   */
  protected void validateLoginUser(UserGroupInformation user)
      throws SliderException {
    if (!user.isFromKeytab()) {
      log.error("User is not holding on a keytab in a secure deployment:" +
          " slider will fail as tokens expire");
    }
    Credentials credentials = user.getCredentials();
    Iterator<Token<? extends TokenIdentifier>> iter =
        credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<? extends TokenIdentifier> token = iter.next();
      log.info("Token {}", token.getKind());
      if (token.getKind().equals(
          DelegationTokenIdentifier.HDFS_DELEGATION_KIND)) {
        log.info("HDFS delegation token {}.  Removing...", token);
        iter.remove();
      }
    }
  }

  /**
   * Set up and start the agent web application 
   * @param appInformation application information
   * @param serviceConf service configuration
   * @param webAppApi web app API instance to bind to
   * @throws IOException
   */
  private void startAgentWebApp(MapOperations appInformation,
      Configuration serviceConf, WebAppApiImpl webAppApi) throws IOException, SliderException {
    URL[] urls = ((URLClassLoader) AgentWebApp.class.getClassLoader() ).getURLs();
    StringBuilder sb = new StringBuilder("AM classpath:");
    for (URL url : urls) {
      sb.append("\n").append(url.toString());
    }
    LOG_YARN.debug(sb.append("\n").toString());
    initAMFilterOptions(serviceConf);


    // Start up the agent web app and track the URL for it
    MapOperations appMasterConfig = getInstanceDefinition()
        .getAppConfOperations().getComponent(SliderKeys.COMPONENT_AM);
    AgentWebApp agentWebApp = AgentWebApp.$for(AgentWebApp.BASE_PATH,
        webAppApi,
        RestPaths.AGENT_WS_CONTEXT)
        .withComponentConfig(appMasterConfig)
        .withPort(getPortToRequest())
        .withSecuredPort(getPortToRequest())
            .start();
    agentOpsUrl =
        "https://" + appMasterHostname + ":" + agentWebApp.getSecuredPort();
    agentStatusUrl =
        "https://" + appMasterHostname + ":" + agentWebApp.getPort();
    AgentService agentService =
      new AgentService("slider-agent", agentWebApp);

    agentService.init(serviceConf);
    agentService.start();
    addService(agentService);

    appInformation.put(StatusKeys.INFO_AM_AGENT_OPS_URL, agentOpsUrl + "/");
    appInformation.put(StatusKeys.INFO_AM_AGENT_STATUS_URL, agentStatusUrl + "/");
    appInformation.set(StatusKeys.INFO_AM_AGENT_STATUS_PORT,
                       agentWebApp.getPort());
    appInformation.set(StatusKeys.INFO_AM_AGENT_OPS_PORT,
                       agentWebApp.getSecuredPort());
  }

  /**
   * Set up the AM filter 
   * @param serviceConf configuration to patch
   */
  private void initAMFilterOptions(Configuration serviceConf) {
    // IP filtering
    String amFilterName = AM_FILTER_NAME;

    // This is here until YARN supports proxy & redirect operations
    // on verbs other than GET, and is only supported for testing
    if (X_DEV_INSECURE_REQUIRED && serviceConf.getBoolean(X_DEV_INSECURE_WS, 
        X_DEV_INSECURE_DEFAULT)) {
      log.warn("Insecure filter enabled: REST operations are unauthenticated");
      amFilterName = InsecureAmFilterInitializer.NAME;
    }

    serviceConf.set(HADOOP_HTTP_FILTER_INITIALIZERS, amFilterName);
  }

  /**
   * This registers the service instance and its external values
   * @param instanceName name of this instance
   * @param appId application ID
   * @throws IOException
   */
  public void registerServiceInstance(String instanceName,
      ApplicationId appId) throws IOException {
    
    
    // the registry is running, so register services
    URL amWebURI = new URL(appMasterProxiedUrl);
    URL agentOpsURI = new URL(agentOpsUrl);
    URL agentStatusURI = new URL(agentStatusUrl);

    //Give the provider restricted access to the state, registry
    setupInitialRegistryPaths();
    yarnRegistryOperations = new YarnRegistryViewForProviders(
        registryOperations,
        service_user_name,
        SliderKeys.APP_TYPE,
        instanceName,
        appAttemptID);
    providerService.bindToYarnRegistry(yarnRegistryOperations);
    sliderAMProvider.bindToYarnRegistry(yarnRegistryOperations);

    // Yarn registry
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(YarnRegistryAttributes.YARN_ID, appId.toString());
    serviceRecord.set(YarnRegistryAttributes.YARN_PERSISTENCE,
        PersistencePolicies.APPLICATION);
    serviceRecord.description = "Slider Application Master";

    serviceRecord.addExternalEndpoint(
        RegistryTypeUtils.ipcEndpoint(
            CustomRegistryConstants.AM_IPC_PROTOCOL,
            rpcServiceAddress));
            
    // internal services
    sliderAMProvider.applyInitialRegistryDefinitions(amWebURI,
        agentOpsURI,
        agentStatusURI,
        serviceRecord);

    // provider service dynamic definitions.
    providerService.applyInitialRegistryDefinitions(amWebURI,
        agentOpsURI,
        agentStatusURI,
        serviceRecord);

    // set any provided attributes
    setProvidedServiceRecordAttributes(
        getInstanceDefinition().getAppConfOperations().getComponent(
            SliderKeys.COMPONENT_AM), serviceRecord);

    // register the service's entry
    log.info("Service Record \n{}", serviceRecord);
    yarnRegistryOperations.registerSelf(serviceRecord, true);
    log.info("Registered service under {}; absolute path {}",
        yarnRegistryOperations.getSelfRegistrationPath(),
        yarnRegistryOperations.getAbsoluteSelfRegistrationPath());
    
    boolean isFirstAttempt = 1 == appAttemptID.getAttemptId();
    // delete the children in case there are any and this is an AM startup.
    // just to make sure everything underneath is purged
    if (isFirstAttempt) {
      yarnRegistryOperations.deleteChildren(
          yarnRegistryOperations.getSelfRegistrationPath(),
          true);
    }
  }

  /**
   * TODO: purge this once RM is doing the work
   * @throws IOException
   */
  protected void setupInitialRegistryPaths() throws IOException {
    if (registryOperations instanceof RMRegistryOperationsService) {
      RMRegistryOperationsService rmRegOperations =
          (RMRegistryOperationsService) registryOperations;
      rmRegOperations.initUserRegistryAsync(service_user_name);
    }
  }

  /**
   * Handler for {@link RegisterComponentInstance action}
   * Register/re-register an ephemeral container that is already in the app state
   * @param id the component
   * @param description component description
   * @param type component type
   * @return true if the component is registered
   */
  public boolean registerComponent(ContainerId id, String description,
      String type) throws IOException {
    RoleInstance instance = appState.getOwnedContainer(id);
    if (instance == null) {
      return false;
    }
    // this is where component registrations  go
    log.info("Registering component {}", id);
    String cid = RegistryPathUtils.encodeYarnID(id.toString());
    ServiceRecord container = new ServiceRecord();
    container.set(YarnRegistryAttributes.YARN_ID, cid);
    container.description = description;
    container.set(YarnRegistryAttributes.YARN_PERSISTENCE,
        PersistencePolicies.CONTAINER);
    MapOperations compOps = getInstanceDefinition().getAppConfOperations().
        getComponent(type);
    setProvidedServiceRecordAttributes(compOps, container);
    try {
      yarnRegistryOperations.putComponent(cid, container);
    } catch (IOException e) {
      log.warn("Failed to register container {}/{}: {}",
          id, description, e, e);
      return false;
    }
    return true;
  }

  protected void setProvidedServiceRecordAttributes(MapOperations ops,
                                                  ServiceRecord record) {
    String prefix = RoleKeys.SERVICE_RECORD_ATTRIBUTE_PREFIX;
    for (Map.Entry<String, String> entry : ops.entrySet()) {
      if (entry.getKey().startsWith(
          prefix)) {
        String key = entry.getKey().substring(
            prefix.length() + 1);
        record.set(key, entry.getValue().trim());
      }
    }
  }

  /**
   * Handler for {@link UnregisterComponentInstance}
   * 
   * unregister a component. At the time this message is received,
   * the component may not have been registered
   * @param id the component
   */
  public void unregisterComponent(ContainerId id) {
    log.info("Unregistering component {}", id);
    if (yarnRegistryOperations == null) {
      log.warn("Processing unregister component event before initialization " +
               "completed; init flag ={}", initCompleted);
      return;
    }
    String cid = RegistryPathUtils.encodeYarnID(id.toString());
    try {
      yarnRegistryOperations.deleteComponent(cid);
    } catch (IOException e) {
      log.warn("Failed to delete container {} : {}", id, e, e);
    }
  }

  /**
   * looks for a specific case where a token file is provided as an environment
   * variable, yet the file is not there.
   * 
   * This surfaced (once) in HBase, where its HDFS library was looking for this,
   * and somehow the token was missing. This is a check in the AM so that
   * if the problem re-occurs, the AM can fail with a more meaningful message.
   * 
   */
  private void checkAndWarnForAuthTokenProblems() {
    String fileLocation =
      System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      File tokenFile = new File(fileLocation);
      if (!tokenFile.exists()) {
        log.warn("Token file {} specified in {} not found", tokenFile,
                 UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
      }
    }
  }

  /**
   * Build the configuration directory passed in or of the target FS
   * @return the file
   */
  public File getLocalConfDir() {
    File confdir =
      new File(SliderKeys.PROPAGATED_CONF_DIR_NAME).getAbsoluteFile();
    return confdir;
  }

  /**
   * Get the path to the DFS configuration that is defined in the cluster specification 
   * @return the generated configuration dir
   */
  public String getGeneratedConfDir() {
    return getGlobalInternalOptions().get(
        InternalKeys.INTERNAL_GENERATED_CONF_PATH);
  }

  /**
   * Get the global internal options for the AM
   * @return a map to access the internals
   */
  public MapOperations getGlobalInternalOptions() {
    return getInstanceDefinition()
      .getInternalOperations().
      getGlobalOptions();
  }

  /**
   * Get the filesystem of this cluster
   * @return the FS of the config
   */
  public SliderFileSystem getClusterFS() throws IOException {
    return new SliderFileSystem(getConfig());
  }

  /**
   * Get the AM log
   * @return the log of the AM
   */
  public static Logger getLog() {
    return log;
  }

  /**
   * Get the application state
   * @return the application state
   */
  public AppState getAppState() {
    return appState;
  }

  /**
   * Block until it is signalled that the AM is done
   */
  private void waitForAMCompletionSignal() {
    AMExecutionStateLock.lock();
    try {
      if (!amCompletionFlag.get()) {
        log.debug("blocking until signalled to terminate");
        isAMCompleted.awaitUninterruptibly();
      }
    } finally {
      AMExecutionStateLock.unlock();
    }
  }

  /**
   * Signal that the AM is complete .. queues it in a separate thread
   *
   * @param stopActionRequest request containing shutdown details
   */
  public synchronized void signalAMComplete(ActionStopSlider stopActionRequest) {
    // this is a queued action: schedule it through the queues
    schedule(stopActionRequest);
  }

  /**
   * Signal that the AM is complete
   *
   * @param stopActionRequest request containing shutdown details
   */
  public synchronized void onAMStop(ActionStopSlider stopActionRequest) {

    AMExecutionStateLock.lock();
    try {
      if (amCompletionFlag.compareAndSet(false, true)) {
        // first stop request received
        this.stopAction = stopActionRequest;
        isAMCompleted.signal();
      }
    } finally {
      AMExecutionStateLock.unlock();
    }
  }

  
  /**
   * trigger the YARN cluster termination process
   * @return the exit code
   * @throws Exception if the stop action contained an Exception which implements
   * ExitCodeProvider
   */
  private synchronized int finish() throws Exception {
    Preconditions.checkNotNull(stopAction, "null stop action");
    FinalApplicationStatus appStatus;
    log.info("Triggering shutdown of the AM: {}", stopAction);

    String appMessage = stopAction.getMessage();
    //stop the daemon & grab its exit code
    int exitCode = stopAction.getExitCode();
    Exception exception = stopAction.getEx();

    appStatus = stopAction.getFinalApplicationStatus();
    if (!spawnedProcessExitedBeforeShutdownTriggered) {
      //stopped the forked process but don't worry about its exit code
      int forkedExitCode = stopForkedProcess();
      log.debug("Stopped forked process: exit code={}", forkedExitCode);
    }

    // make sure the AM is actually registered. If not, there's no point
    // trying to unregister it
    if (amRegistrationData == null) {
      log.info("Application attempt not yet registered; skipping unregistration");
      if (exception != null) {
        throw exception;
      }
      return exitCode;
    }

    //stop any launches in progress
    launchService.stop();

    //now release all containers
    releaseAllContainers();

    // When the application completes, it should send a finish application
    // signal to the RM
    log.info("Application completed. Signalling finish to RM");

    try {
      log.info("Unregistering AM status={} message={}", appStatus, appMessage);
      asyncRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
    } catch (InvalidApplicationMasterRequestException e) {
      log.info("Application not found in YARN application list;" +
        " it may have been terminated/YARN shutdown in progress: {}", e, e);
    } catch (YarnException | IOException e) {
      log.info("Failed to unregister application: " + e, e);
    }
    if (exception != null) {
      throw exception;
    }
    return exitCode;
  }

    /**
     * Get diagnostics info about containers
     */
  private String getContainerDiagnosticInfo() {

    return appState.getContainerDiagnosticInfo();
  }

  public Object getProxy(Class protocol, InetSocketAddress addr) {
    return yarnRPC.getProxy(protocol, addr, getConfig());
  }

  /**
   * Start the slider RPC server
   */
  private void startSliderRPCServer(AggregateConf instanceDefinition)
      throws IOException, SliderException {
    verifyIPCAccess();

    sliderIPCService = new SliderIPCService(
        this,
        certificateManager,
        stateForProviders,
        actionQueues,
        metricsAndMonitoring,
        contentCache);

    deployChildService(sliderIPCService);
    SliderClusterProtocolPBImpl protobufRelay =
        new SliderClusterProtocolPBImpl(sliderIPCService);
    BlockingService blockingService = SliderClusterAPI.SliderClusterProtocolPB
        .newReflectiveBlockingService(
            protobufRelay);

    int port = getPortToRequest();
    InetSocketAddress rpcAddress = new InetSocketAddress("0.0.0.0", port);
    rpcService =
        new WorkflowRpcService("SliderRPC",
            RpcBinder.createProtobufServer(rpcAddress, getConfig(),
                secretManager,
            NUM_RPC_HANDLERS,
            blockingService,
            null));
    deployChildService(rpcService);
  }

  /**
   * verify that if the cluster is authed, the ACLs are set.
   * @throws BadConfigException if Authorization is set without any ACL
   */
  private void verifyIPCAccess() throws BadConfigException {
    boolean authorization = getConfig().getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION,
        false);
    String acls = getConfig().get(KEY_PROTOCOL_ACL);
    if (authorization && SliderUtils.isUnset(acls)) {
      throw new BadConfigException("Application has IPC authorization enabled in " +
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION +
          " but no ACLs in " + KEY_PROTOCOL_ACL);
    }
  }


/* =================================================================== */
/* AMRMClientAsync callbacks */
/* =================================================================== */

  /**
   * Callback event when a container is allocated.
   * 
   * The app state is updated with the allocation, and builds up a list
   * of assignments and RM operations. The assignments are 
   * handed off into the pool of service launchers to asynchronously schedule
   * container launch operations.
   * 
   * The operations are run in sequence; they are expected to be 0 or more
   * release operations (to handle over-allocations)
   * 
   * @param allocatedContainers list of containers that are now ready to be
   * given work.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override //AMRMClientAsync
  public void onContainersAllocated(List<Container> allocatedContainers) {
    LOG_YARN.info("onContainersAllocated({})", allocatedContainers.size());
    List<ContainerAssignment> assignments = new ArrayList<>();
    List<AbstractRMOperation> operations = new ArrayList<>();
    
    //app state makes all the decisions
    appState.onContainersAllocated(allocatedContainers, assignments, operations);

    //for each assignment: instantiate that role
    for (ContainerAssignment assignment : assignments) {
      try {
        launchService.launchRole(assignment, getInstanceDefinition(),
            buildContainerCredentials());
      } catch (IOException e) {
        // Can be caused by failure to renew credentials with the remote
        // service. If so, don't launch the application. Container is retained,
        // though YARN will take it away after a timeout.
        log.error("Failed to build credentials to launch container: {}", e, e);

      }
    }

    //for all the operations, exec them
    execute(operations);
    log.info("Diagnostics: {}", getContainerDiagnosticInfo());
  }

  @Override //AMRMClientAsync
  public synchronized void onContainersCompleted(List<ContainerStatus> completedContainers) {
    LOG_YARN.info("onContainersCompleted([{}]", completedContainers.size());
    for (ContainerStatus status : completedContainers) {
      ContainerId containerId = status.getContainerId();
      LOG_YARN.info("Container Completion for" +
                    " containerID={}," +
                    " state={}," +
                    " exitStatus={}," +
                    " diagnostics={}",
                    containerId, status.getState(),
                    status.getExitStatus(),
                    status.getDiagnostics());

      // non complete containers should not be here
      assert (status.getState() == ContainerState.COMPLETE);
      AppState.NodeCompletionResult result = appState.onCompletedNode(status);
      if (result.containerFailed) {
        RoleInstance ri = result.roleInstance;
        log.error("Role instance {} failed ", ri);
      }

      //  known nodes trigger notifications
      if(!result.unknownNode) {
        getProviderService().notifyContainerCompleted(containerId);
        queue(new UnregisterComponentInstance(containerId, 0,
            TimeUnit.MILLISECONDS));
      }
    }

    reviewRequestAndReleaseNodes("onContainersCompleted");
  }

  /**
   * Signal that containers are being upgraded. Containers specified with
   * --containers option and all containers of all roles specified with
   * --components option are merged and upgraded.
   * 
   * @param upgradeContainersRequest
   *          request containing upgrade details
   */
  public synchronized void onUpgradeContainers(
      ActionUpgradeContainers upgradeContainersRequest) throws IOException,
      SliderException {
    LOG_YARN.info("onUpgradeContainers({})",
        upgradeContainersRequest.getMessage());
    Set<String> containers = upgradeContainersRequest.getContainers() == null ? new HashSet<String>()
        : upgradeContainersRequest.getContainers();
    LOG_YARN.info("  Container list provided (total {}) : {}",
        containers.size(), containers);
    Set<String> components = upgradeContainersRequest.getComponents() == null ? new HashSet<String>()
        : upgradeContainersRequest.getComponents();
    LOG_YARN.info("  Component list provided (total {}) : {}",
        components.size(), components);
    // If components are specified as well, then grab all the containers of
    // each of the components (roles)
    if (CollectionUtils.isNotEmpty(components)) {
      Map<ContainerId, RoleInstance> liveContainers = appState.getLiveContainers();
      if (CollectionUtils.isNotEmpty(liveContainers.keySet())) {
        Map<String, Set<String>> roleContainerMap = prepareRoleContainerMap(liveContainers);
        for (String component : components) {
          Set<String> roleContainers = roleContainerMap.get(component);
          if (roleContainers != null) {
            containers.addAll(roleContainers);
          }
        }
      }
    }
    LOG_YARN.info("Final list of containers to be upgraded (total {}) : {}",
        containers.size(), containers);
    if (providerService instanceof AgentProviderService) {
      AgentProviderService agentProviderService = (AgentProviderService) providerService;
      agentProviderService.setInUpgradeMode(true);
      agentProviderService.addUpgradeContainers(containers);
    }
  }

  // create a reverse map of roles -> set of all live containers
  private Map<String, Set<String>> prepareRoleContainerMap(
      Map<ContainerId, RoleInstance> liveContainers) {
    // liveContainers is ensured to be not empty
    Map<String, Set<String>> roleContainerMap = new HashMap<>();
    for (Map.Entry<ContainerId, RoleInstance> liveContainer : liveContainers
        .entrySet()) {
      RoleInstance role = liveContainer.getValue();
      if (roleContainerMap.containsKey(role.role)) {
        roleContainerMap.get(role.role).add(liveContainer.getKey().toString());
      } else {
        Set<String> containers = new HashSet<String>();
        containers.add(liveContainer.getKey().toString());
        roleContainerMap.put(role.role, containers);
      }
    }
    return roleContainerMap;
  }

  /**
   * Implementation of cluster flexing.
   * It should be the only way that anything -even the AM itself on startup-
   * asks for nodes. 
   * @param resources the resource tree
   * @throws SliderException slider problems, including invalid configs
   * @throws IOException IO problems
   */
  public void flexCluster(ConfTree resources)
      throws IOException, SliderException {

    AggregateConf newConf =
        new AggregateConf(appState.getInstanceDefinitionSnapshot());
    newConf.setResources(resources);
    // verify the new definition is valid
    sliderAMProvider.validateInstanceDefinition(newConf);
    providerService.validateInstanceDefinition(newConf);

    appState.updateResourceDefinitions(resources);

    // reset the scheduled windows...the values
    // may have changed
    appState.resetFailureCounts();

    // ask for more containers if needed
    reviewRequestAndReleaseNodes("flexCluster");
  }

  /**
   * Schedule the failure window
   * @param resources the resource tree
   * @throws BadConfigException if the window is out of range
   */
  private void scheduleFailureWindowResets(ConfTree resources) throws
      BadConfigException {
    ResetFailureWindow reset = new ResetFailureWindow();
    ConfTreeOperations ops = new ConfTreeOperations(resources);
    MapOperations globals = ops.getGlobalOptions();
    long seconds = globals.getTimeRange(ResourceKeys.CONTAINER_FAILURE_WINDOW,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_DAYS,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_HOURS,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_WINDOW_MINUTES, 0);
    if (seconds > 0) {
      log.info(
          "Scheduling the failure window reset interval to every {} seconds",
          seconds);
      RenewingAction<ResetFailureWindow> renew = new RenewingAction<>(
          reset, seconds, seconds, TimeUnit.SECONDS, 0);
      actionQueues.renewing("failures", renew);
    } else {
      log.info("Failure window reset interval is not set");
    }
  }

  /**
   * Schedule the escalation action
   * @param internal
   * @throws BadConfigException
   */
  private void scheduleEscalation(ConfTree internal) throws BadConfigException {
    EscalateOutstandingRequests escalate = new EscalateOutstandingRequests();
    ConfTreeOperations ops = new ConfTreeOperations(internal);
    int seconds = ops.getGlobalOptions().getOptionInt(InternalKeys.ESCALATION_CHECK_INTERVAL,
        InternalKeys.DEFAULT_ESCALATION_CHECK_INTERVAL);
    RenewingAction<EscalateOutstandingRequests> renew = new RenewingAction<>(
        escalate, seconds, seconds, TimeUnit.SECONDS, 0);
    actionQueues.renewing("escalation", renew);
  }
  
  /**
   * Look at where the current node state is -and whether it should be changed
   * @param reason reason for operation
   */
  private synchronized void reviewRequestAndReleaseNodes(String reason) {
    log.debug("reviewRequestAndReleaseNodes({})", reason);
    queue(new ReviewAndFlexApplicationSize(reason, 0, TimeUnit.SECONDS));
  }

  /**
   * Handle the event requesting a review ... look at the queue and decide
   * whether to act or not
   * @param action action triggering the event. It may be put
   * back into the queue
   * @throws SliderInternalStateException
   */
  public void handleReviewAndFlexApplicationSize(ReviewAndFlexApplicationSize action)
      throws SliderInternalStateException {

    if ( actionQueues.hasQueuedActionWithAttribute(
        AsyncAction.ATTR_REVIEWS_APP_SIZE | AsyncAction.ATTR_HALTS_APP)) {
      // this operation isn't needed at all -existing duplicate or shutdown due
      return;
    }
    // if there is an action which changes cluster size, wait
    if (actionQueues.hasQueuedActionWithAttribute(
        AsyncAction.ATTR_CHANGES_APP_SIZE)) {
      // place the action at the back of the queue
      actionQueues.put(action);
    }
    
    executeNodeReview(action.name);
  }
  
  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized void executeNodeReview(String reason)
      throws SliderInternalStateException {
    
    log.debug("in executeNodeReview({})", reason);
    if (amCompletionFlag.get()) {
      log.info("Ignoring node review operation: shutdown in progress");
    }
    try {
      List<AbstractRMOperation> allOperations = appState.reviewRequestAndReleaseNodes();
      // tell the provider
      providerRMOperationHandler.execute(allOperations);
      //now apply the operations
      execute(allOperations);
    } catch (TriggerClusterTeardownException e) {
      //App state has decided that it is time to exit
      log.error("Cluster teardown triggered {}", e, e);
      queue(new ActionStopSlider(e));
    }
  }

  /**
   * Escalate operation as triggered by external timer.
   * <p>
   * Get the list of new operations off the AM, then executest them.
   */
  public void escalateOutstandingRequests() {
    List<AbstractRMOperation> operations = appState.escalateOutstandingRequests();
    providerRMOperationHandler.execute(operations);
    execute(operations);
  }


  /**
   * Shutdown operation: release all containers
   */
  private void releaseAllContainers() {
    if (providerService instanceof AgentProviderService) {
      log.info("Setting stopInitiated flag to true");
      AgentProviderService agentProviderService = (AgentProviderService) providerService;
      agentProviderService.setAppStopInitiated(true);
    }
    // Add the sleep here (before releasing containers) so that applications get
    // time to perform graceful shutdown
    try {
      long timeout = getContainerReleaseTimeout();
      if (timeout > 0) {
        Thread.sleep(timeout);
      }
    } catch (InterruptedException e) {
      log.info("Sleep for container release interrupted");
    } finally {
      List<AbstractRMOperation> operations = appState.releaseAllContainers();
      providerRMOperationHandler.execute(operations);
      // now apply the operations
      execute(operations);
    }
  }

  private long getContainerReleaseTimeout() {
    // Get container release timeout in millis or 0 if the property is not set.
    // If non-zero then add the agent heartbeat delay time, since it can take up
    // to that much time for agents to receive the stop command.
    int timeout = getInstanceDefinition().getAppConfOperations()
        .getGlobalOptions()
        .getOptionInt(SliderKeys.APP_CONTAINER_RELEASE_TIMEOUT, 0);
    if (timeout > 0) {
      timeout += SliderKeys.APP_CONTAINER_HEARTBEAT_INTERVAL_SEC;
    }
    // convert to millis
    long timeoutInMillis = timeout * 1000l;
    log.info("Container release timeout in millis = {}", timeoutInMillis);
    return timeoutInMillis;
  }

  /**
   * RM wants to shut down the AM
   */
  @Override //AMRMClientAsync
  public void onShutdownRequest() {
    LOG_YARN.info("Shutdown Request received");
    signalAMComplete(new ActionStopSlider("stop",
        EXIT_SUCCESS,
        FinalApplicationStatus.SUCCEEDED,
        "Shutdown requested from RM"));
  }

  /**
   * Monitored nodes have been changed
   * @param updatedNodes list of updated nodes
   */
  @Override //AMRMClientAsync
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
    LOG_YARN.info("onNodesUpdated({})", updatedNodes.size());
    log.info("Updated nodes {}", updatedNodes);
    // Check if any nodes are lost or revived and update state accordingly

    AppState.NodeUpdatedOutcome outcome = appState.onNodesUpdated(updatedNodes);
    if (!outcome.operations.isEmpty()) {
      execute(outcome.operations);
    }
    // trigger a review if the cluster changed
    if (outcome.clusterChanged) {
      reviewRequestAndReleaseNodes("nodes updated");
    }
  }

  /**
   * heartbeat operation; return the ratio of requested
   * to actual
   * @return progress
   */
  @Override //AMRMClientAsync
  public float getProgress() {
    return appState.getApplicationProgressPercentage();
  }

  @Override //AMRMClientAsync
  public void onError(Throwable e) {
    //callback says it's time to finish
    LOG_YARN.error("AMRMClientAsync.onError() received {}", e, e);
    signalAMComplete(new ActionStopSlider("stop",
        EXIT_EXCEPTION_THROWN,
        FinalApplicationStatus.FAILED,
        "AMRMClientAsync.onError() received " + e));
  }
  
/* =================================================================== */
/* RMOperationHandlerActions */
/* =================================================================== */

 
  @Override
  public void execute(List<AbstractRMOperation> operations) {
    rmOperationHandler.execute(operations);
  }

  @Override
  public void releaseAssignedContainer(ContainerId containerId) {
    rmOperationHandler.releaseAssignedContainer(containerId);
  }

  @Override
  public void addContainerRequest(AMRMClient.ContainerRequest req) {
    rmOperationHandler.addContainerRequest(req);
  }

  @Override
  public int cancelContainerRequests(Priority priority1,
      Priority priority2,
      int count) {
    return rmOperationHandler.cancelContainerRequests(priority1, priority2, count);
  }

  @Override
  public void cancelSingleRequest(AMRMClient.ContainerRequest request) {
    rmOperationHandler.cancelSingleRequest(request);
  }

/* =================================================================== */
/* END */
/* =================================================================== */

  /**
   * Launch the provider service
   *
   * @param instanceDefinition definition of the service
   * @param confDir directory of config data
   * @throws IOException
   * @throws SliderException
   */
  protected synchronized void launchProviderService(AggregateConf instanceDefinition,
                                                    File confDir)
    throws IOException, SliderException {
    Map<String, String> env = new HashMap<>();
    boolean execStarted = providerService.exec(instanceDefinition, confDir, env,
        this);
    if (execStarted) {
      providerService.registerServiceListener(this);
      providerService.start();
    } else {
      // didn't start, so don't register
      providerService.start();
      // and send the started event ourselves
      eventCallbackEvent(null);
    }
  }

  /* =================================================================== */
  /* EventCallback  from the child or ourselves directly */
  /* =================================================================== */

  @Override // ProviderCompleted
  public void eventCallbackEvent(Object parameter) {
    // signalled that the child process is up.
    appState.noteAMLive();
    // now ask for the cluster nodes
    try {
      flexCluster(getInstanceDefinition().getResources());
    } catch (Exception e) {
      // cluster flex failure: log
      log.error("Failed to flex cluster nodes: {}", e, e);
      // then what? exit
      queue(new ActionStopSlider(e));
    }
  }

  /**
   * report container loss. If this isn't already known about, react
   *
   * @param containerId       id of the container which has failed
   * @throws SliderException
   */
  public synchronized void providerLostContainer(
      ContainerId containerId)
      throws SliderException {
    log.info("containerLostContactWithProvider: container {} lost",
        containerId);
    RoleInstance activeContainer = appState.getOwnedContainer(containerId);
    if (activeContainer != null) {
      execute(appState.releaseContainer(containerId));
      // ask for more containers if needed
      log.info("Container released; triggering review");
      reviewRequestAndReleaseNodes("Loss of container");
    } else {
      log.info("Container not in active set - ignoring");
    }
  }

  /* =================================================================== */
  /* ServiceStateChangeListener */
  /* =================================================================== */

  /**
   * Received on listening service termination.
   * @param service the service that has changed.
   */
  @Override //ServiceStateChangeListener
  public void stateChanged(Service service) {
    if (service == providerService && service.isInState(STATE.STOPPED)) {
      //its the current master process in play
      int exitCode = providerService.getExitCode();
      int mappedProcessExitCode = exitCode;

      boolean shouldTriggerFailure = !amCompletionFlag.get()
         && (mappedProcessExitCode != 0);

      if (shouldTriggerFailure) {
        String reason =
            "Spawned process failed with raw " + exitCode + " mapped to " +
            mappedProcessExitCode;
        ActionStopSlider stop = new ActionStopSlider("stop",
            mappedProcessExitCode,
            FinalApplicationStatus.FAILED,
            reason);
        //this wasn't expected: the process finished early
        spawnedProcessExitedBeforeShutdownTriggered = true;
        log.info(
          "Process has exited with exit code {} mapped to {} -triggering termination",
          exitCode,
          mappedProcessExitCode);

        //tell the AM the cluster is complete 
        signalAMComplete(stop);
      } else {
        //we don't care
        log.info(
          "Process has exited with exit code {} mapped to {} -ignoring",
          exitCode,
          mappedProcessExitCode);
      }
    } else {
      super.stateChanged(service);
    }
  }

  /**
   * stop forked process if it the running process var is not null
   * @return the process exit code
   */
  protected synchronized Integer stopForkedProcess() {
    providerService.stop();
    return providerService.getExitCode();
  }

  /**
   *  Async start container request
   * @param container container
   * @param ctx context
   * @param instance node details
   */
  public void startContainer(Container container,
                             ContainerLaunchContext ctx,
                             RoleInstance instance) throws IOException {
    appState.containerStartSubmitted(container, instance);
        
    nmClientAsync.startContainerAsync(container, ctx);
  }

  /**
   * Build the credentials needed for containers. This will include
   * getting new delegation tokens for HDFS if the AM is running
   * with a keytab.
   * @return a buffer of credentials
   * @throws IOException
   */

  private Credentials buildContainerCredentials() throws IOException {
    Credentials credentials = new Credentials(containerCredentials);
    if (securityConfiguration.isKeytabProvided()) {
      CredentialUtils.addSelfRenewableFSDelegationTokens(
          getClusterFS().getFileSystem(),
          credentials);
    }
    return credentials;
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStopped(ContainerId containerId) {
    // do nothing but log: container events from the AM
    // are the source of container halt details to react to
    log.info("onContainerStopped {} ", containerId);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStarted(ContainerId containerId,
      Map<String, ByteBuffer> allServiceResponse) {
    LOG_YARN.info("Started Container {} ", containerId);
    RoleInstance cinfo = appState.onNodeManagerContainerStarted(containerId);
    if (cinfo != null) {
      LOG_YARN.info("Deployed instance of role {} onto {}",
          cinfo.role, containerId);
      //trigger an async container status
      nmClientAsync.getContainerStatusAsync(containerId,
                                            cinfo.container.getNodeId());
      // push out a registration
      queue(new RegisterComponentInstance(containerId, cinfo.role, cinfo.group,
          0, TimeUnit.MILLISECONDS));
      
    } else {
      //this is a hypothetical path not seen. We react by warning
      log.error("Notified of started container that isn't pending {} - releasing",
                containerId);
      //then release it
      asyncRMClient.releaseAssignedContainer(containerId);
    }
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG_YARN.error("Failed to start Container {}", containerId, t);
    appState.onNodeManagerContainerStartFailed(containerId, t);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onContainerStatusReceived(ContainerId containerId,
      ContainerStatus containerStatus) {
    LOG_YARN.debug("Container Status: id={}, status={}", containerId,
        containerStatus);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onGetContainerStatusError(
      ContainerId containerId, Throwable t) {
    LOG_YARN.error("Failed to query the status of Container {}", containerId);
  }

  @Override //  NMClientAsync.CallbackHandler 
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG_YARN.warn("Failed to stop Container {}", containerId);
  }

  public AggregateConf getInstanceDefinition() {
    return appState.getInstanceDefinition();
  }

  /**
   * This is the status, the live model
   */
  public ClusterDescription getClusterDescription() {
    return appState.getClusterStatus();
  }

  public ProviderService getProviderService() {
    return providerService;
  }

  /**
   * Queue an action for immediate execution in the executor thread
   * @param action action to execute
   */
  public void queue(AsyncAction action) {
    actionQueues.put(action);
  }

  /**
   * Schedule an action
   * @param action for delayed execution
   */
  public void schedule(AsyncAction action) {
    actionQueues.schedule(action);
  }


  /**
   * Handle any exception in a thread. If the exception provides an exit
   * code, that is the one that will be used
   * @param thread thread throwing the exception
   * @param exception exception
   */
  public void onExceptionInThread(Thread thread, Throwable exception) {
    log.error("Exception in {}: {}", thread.getName(), exception, exception);
    
    // if there is a teardown in progress, ignore it
    if (amCompletionFlag.get()) {
      log.info("Ignoring exception: shutdown in progress");
    } else {
      int exitCode = EXIT_EXCEPTION_THROWN;
      if (exception instanceof ExitCodeProvider) {
        exitCode = ((ExitCodeProvider) exception).getExitCode();
      }
      signalAMComplete(new ActionStopSlider("stop",
          exitCode,
          FinalApplicationStatus.FAILED,
          exception.toString()));
    }
  }

  /**
   * Start the chaos monkey
   * @return true if it started
   */
  private boolean maybeStartMonkey() {
    MapOperations internals = getGlobalInternalOptions();

    Boolean enabled =
        internals.getOptionBool(InternalKeys.CHAOS_MONKEY_ENABLED,
            InternalKeys.DEFAULT_CHAOS_MONKEY_ENABLED);
    if (!enabled) {
      log.debug("Chaos monkey disabled");
      return false;
    }
    
    long monkeyInterval = internals.getTimeRange(
        InternalKeys.CHAOS_MONKEY_INTERVAL,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_DAYS,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_HOURS,
        InternalKeys.DEFAULT_CHAOS_MONKEY_INTERVAL_MINUTES,
        0);
    if (monkeyInterval == 0) {
      log.debug(
          "Chaos monkey not configured with a time interval...not enabling");
      return false;
    }

    long monkeyDelay = internals.getTimeRange(
        InternalKeys.CHAOS_MONKEY_DELAY,
        0,
        0,
        0,
        (int)monkeyInterval);
    
    log.info("Adding Chaos Monkey scheduled every {} seconds ({} hours -delay {}",
        monkeyInterval, monkeyInterval/(60*60), monkeyDelay);
    monkey = new ChaosMonkeyService(metrics, actionQueues);
    initAndAddService(monkey);
    
    // configure the targets
    
    // launch failure: special case with explicit failure triggered now
    int amLaunchFailProbability = internals.getOptionInt(
        InternalKeys.CHAOS_MONKEY_PROBABILITY_AM_LAUNCH_FAILURE,
        0);
    if (amLaunchFailProbability> 0 && monkey.chaosCheck(amLaunchFailProbability)) {
      log.info("Chaos Monkey has triggered AM Launch failure");
      // trigger a failure
      ActionStopSlider stop = new ActionStopSlider("stop",
          0, TimeUnit.SECONDS,
          LauncherExitCodes.EXIT_FALSE,
          FinalApplicationStatus.FAILED,
          E_TRIGGERED_LAUNCH_FAILURE);
      queue(stop);
    }
    
    int amKillProbability = internals.getOptionInt(
        InternalKeys.CHAOS_MONKEY_PROBABILITY_AM_FAILURE,
        InternalKeys.DEFAULT_CHAOS_MONKEY_PROBABILITY_AM_FAILURE);
    monkey.addTarget("AM killer",
        new ChaosKillAM(actionQueues, -1), amKillProbability);
    int containerKillProbability = internals.getOptionInt(
        InternalKeys.CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE,
        InternalKeys.DEFAULT_CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE);
    monkey.addTarget("Container killer",
        new ChaosKillContainer(appState, actionQueues, rmOperationHandler),
        containerKillProbability);
    
    // and schedule it
    if (monkey.schedule(monkeyDelay, monkeyInterval, TimeUnit.SECONDS)) {
      log.info("Chaos Monkey is running");
      return true;
    } else {
      log.info("Chaos monkey not started");
      return false;
    }
  }
  
  /**
   * This is the main entry point for the service launcher.
   * @param args command line arguments.
   */
  public static void main(String[] args) {

    //turn the args to a list
    List<String> argsList = Arrays.asList(args);
    //create a new list, as the ArrayList type doesn't push() on an insert
    List<String> extendedArgs = new ArrayList<String>(argsList);
    //insert the service name
    extendedArgs.add(0, SERVICE_CLASSNAME);
    //now have the service launcher do its work
    ServiceLauncher.serviceMain(extendedArgs);
  }

}
