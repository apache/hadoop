/*
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

package org.apache.slider.client;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.KerberosDiags;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;
import org.apache.slider.api.SliderClusterProtocol;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.types.ContainerInformation;
import org.apache.slider.api.types.NodeInformationList;
import org.apache.slider.client.ipc.SliderClusterOperations;
import org.apache.slider.common.Constants;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.params.AbstractActionArgs;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.params.ActionAMSuicideArgs;
import org.apache.slider.common.params.ActionClientArgs;
import org.apache.slider.common.params.ActionDependencyArgs;
import org.apache.slider.common.params.ActionDiagnosticArgs;
import org.apache.slider.common.params.ActionExistsArgs;
import org.apache.slider.common.params.ActionFlexArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionKDiagArgs;
import org.apache.slider.common.params.ActionKeytabArgs;
import org.apache.slider.common.params.ActionKillContainerArgs;
import org.apache.slider.common.params.ActionListArgs;
import org.apache.slider.common.params.ActionLookupArgs;
import org.apache.slider.common.params.ActionNodesArgs;
import org.apache.slider.common.params.ActionRegistryArgs;
import org.apache.slider.common.params.ActionResolveArgs;
import org.apache.slider.common.params.ActionResourceArgs;
import org.apache.slider.common.params.ActionStatusArgs;
import org.apache.slider.common.params.ActionThawArgs;
import org.apache.slider.common.params.ActionTokensArgs;
import org.apache.slider.common.params.ActionUpgradeArgs;
import org.apache.slider.common.params.Arguments;
import org.apache.slider.common.params.ClientArgs;
import org.apache.slider.common.params.CommonArgs;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.Duration;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.common.tools.SliderVersionInfo;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.NotFoundException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.exceptions.UsageException;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.launch.CredentialUtils;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.core.launch.SerializedApplicationReport;
import org.apache.slider.core.main.RunService;
import org.apache.slider.core.persist.ApplicationReportSerDeser;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.slider.core.registry.SliderRegistryUtils;
import org.apache.slider.core.registry.YarnAppListClient;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsOutputter;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.core.registry.retrieve.RegistryRetriever;
import org.apache.slider.core.zk.BlockingZKWatcher;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.providers.AbstractClientProvider;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.rpc.RpcBinder;
import org.apache.slider.server.services.utility.AbstractSliderLaunchedService;
import org.apache.slider.util.ServiceApiUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.registry.client.binding.RegistryUtils.*;
import static org.apache.slider.common.Constants.HADOOP_JAAS_DEBUG;
import static org.apache.slider.common.params.SliderActions.*;
import static org.apache.slider.common.tools.SliderUtils.*;

/**
 * Client service for Slider
 */

public class SliderClient extends AbstractSliderLaunchedService implements RunService,
    SliderExitCodes, SliderKeys, ErrorStrings, SliderClientAPI {
  private static final Logger log = LoggerFactory.getLogger(SliderClient.class);
  public static final String E_MUST_BE_A_VALID_JSON_FILE
      = "Invalid configuration. Must be a valid json file.";
  public static final String E_INVALID_INSTALL_LOCATION
      = "A valid install location must be provided for the client.";
  public static final String E_UNABLE_TO_READ_SUPPLIED_PACKAGE_FILE
      = "Unable to read supplied package file";
  public static final String E_INVALID_APPLICATION_PACKAGE_LOCATION
      = "A valid application package location required.";
  public static final String E_INVALID_INSTALL_PATH = "Install path is not a valid directory";
  public static final String E_INSTALL_PATH_DOES_NOT_EXIST = "Install path does not exist";
  public static final String E_INVALID_APPLICATION_TYPE_NAME
      = "A valid application type name is required (e.g. HBASE).";
  public static final String E_USE_REPLACEPKG_TO_OVERWRITE = "Use --replacepkg to overwrite.";
  public static final String E_PACKAGE_DOES_NOT_EXIST = "Package does not exist";
  public static final String E_NO_ZOOKEEPER_QUORUM = "No Zookeeper quorum defined";
  public static final String E_NO_RESOURCE_MANAGER = "No valid Resource Manager address provided";
  public static final String E_PACKAGE_EXISTS = "Package exists";
  private static PrintStream clientOutputStream = System.out;
  private static final JsonSerDeser<Application> jsonSerDeser =
      new JsonSerDeser<Application>(Application.class,
          PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

  // value should not be changed without updating string find in slider.py
  private static final String PASSWORD_PROMPT = "Enter password for";

  private ClientArgs serviceArgs;
  public ApplicationId applicationId;

  private String deployedClusterName;
  /**
   * Cluster operations against the deployed cluster -will be null
   * if no bonding has yet taken place
   */
  private SliderClusterOperations sliderClusterOperations;

  protected SliderFileSystem sliderFileSystem;

  /**
   * Yarn client service
   */
  private SliderYarnClientImpl yarnClient;
  private YarnAppListClient yarnAppListClient;

  /**
   * The YARN registry service
   */
  @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
  private RegistryOperations registryOperations;

  private static EnumSet<YarnApplicationState> terminatedStates = EnumSet
      .of(YarnApplicationState.FINISHED, YarnApplicationState.FAILED,
          YarnApplicationState.KILLED);
  /**
   * Constructor
   */
  public SliderClient() {
    super("Slider Client");
    new HdfsConfiguration();
    new YarnConfiguration();
  }

  /**
   * This is called <i>Before serviceInit is called</i>
   * @param config the initial configuration build up by the
   * service launcher.
   * @param args argument list list of arguments passed to the command line
   * after any launcher-specific commands have been stripped.
   * @return the post-binding configuration to pass to the <code>init()</code>
   * operation.
   * @throws Exception
   */
  @Override
  public Configuration bindArgs(Configuration config, String... args) throws Exception {
    config = super.bindArgs(config, args);
    serviceArgs = new ClientArgs(args);
    serviceArgs.parse();
    // yarn-ify
    YarnConfiguration yarnConfiguration = new YarnConfiguration(config);
    return patchConfiguration(yarnConfiguration);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    Configuration clientConf = loadSliderClientXML();
    ConfigHelper.mergeConfigurations(conf, clientConf, SLIDER_CLIENT_XML, true);
    serviceArgs.applyDefinitions(conf);
    serviceArgs.applyFileSystemBinding(conf);
    AbstractActionArgs coreAction = serviceArgs.getCoreAction();
    // init security with our conf
    if (!coreAction.disableSecureLogin() && isHadoopClusterSecure(conf)) {
      forceLogin();
      initProcessSecurity(conf);
    }
    if (coreAction.getHadoopServicesRequired()) {
      initHadoopBinding();
    }
    super.serviceInit(conf);
  }

  /**
   * Launched service execution. This runs {@link #exec()}
   * then catches some exceptions and converts them to exit codes
   * @return an exit code
   * @throws Throwable
   */
  @Override
  public int runService() throws Throwable {
    try {
      return exec();
    } catch (FileNotFoundException | PathNotFoundException nfe) {
      throw new NotFoundException(nfe, nfe.toString());
    }
  }

  /**
   * Execute the command line
   * @return an exit code
   * @throws Throwable on a failure
   */
  public int exec() throws Throwable {

    // choose the action
    String action = serviceArgs.getAction();
    if (isUnset(action)) {
      throw new SliderException(EXIT_USAGE, serviceArgs.usage());
    }

    int exitCode = EXIT_SUCCESS;
    String clusterName = serviceArgs.getClusterName();
    // actions

    switch (action) {
      case ACTION_AM_SUICIDE:
        exitCode = actionAmSuicide(clusterName,
            serviceArgs.getActionAMSuicideArgs());
        break;

      case ACTION_BUILD:
        exitCode = actionBuild(getApplicationFromArgs(clusterName,
            serviceArgs.getActionBuildArgs()));
        break;

      case ACTION_CLIENT:
        exitCode = actionClient(serviceArgs.getActionClientArgs());
        break;

      case ACTION_CREATE:
        actionCreate(getApplicationFromArgs(clusterName,
            serviceArgs.getActionCreateArgs()));
        break;

      case ACTION_DEPENDENCY:
        exitCode = actionDependency(serviceArgs.getActionDependencyArgs());
        break;

      case ACTION_DESTROY:
        actionDestroy(clusterName);
        break;

      case ACTION_DIAGNOSTICS:
        exitCode = actionDiagnostic(serviceArgs.getActionDiagnosticArgs());
        break;
      
      case ACTION_EXISTS:
        exitCode = actionExists(clusterName,
            serviceArgs.getActionExistsArgs());
        break;
      
      case ACTION_FLEX:
        actionFlex(clusterName, serviceArgs.getActionFlexArgs());
        break;
      
      case ACTION_STOP:
        actionStop(clusterName, serviceArgs.getActionFreezeArgs());
        break;
      
      case ACTION_HELP:
        log.info(serviceArgs.usage());
        break;

      case ACTION_KDIAG:
        exitCode = actionKDiag(serviceArgs.getActionKDiagArgs());
        break;

      case ACTION_KILL_CONTAINER:
        exitCode = actionKillContainer(clusterName,
            serviceArgs.getActionKillContainerArgs());
        break;

      case ACTION_KEYTAB:
        exitCode = actionKeytab(serviceArgs.getActionKeytabArgs());
        break;

      case ACTION_LIST:
        exitCode = actionList(clusterName, serviceArgs.getActionListArgs());
        break;

      case ACTION_LOOKUP:
        exitCode = actionLookup(serviceArgs.getActionLookupArgs());
        break;

      case ACTION_NODES:
        exitCode = actionNodes("", serviceArgs.getActionNodesArgs());
        break;

      case ACTION_REGISTRY:
        exitCode = actionRegistry(serviceArgs.getActionRegistryArgs());
        break;
      
      case ACTION_RESOLVE:
        exitCode = actionResolve(serviceArgs.getActionResolveArgs());
        break;

      case ACTION_RESOURCE:
        exitCode = actionResource(serviceArgs.getActionResourceArgs());
        break;

      case ACTION_STATUS:
        exitCode = actionStatus(clusterName, serviceArgs.getActionStatusArgs());
        break;

      case ACTION_START:
        exitCode = actionStart(clusterName, serviceArgs.getActionThawArgs());
        break;

      case ACTION_TOKENS:
        exitCode = actionTokens(serviceArgs.getActionTokenArgs());
        break;

      case ACTION_UPDATE:
        exitCode = actionUpdate(clusterName, serviceArgs.getActionUpdateArgs());
        break;

      case ACTION_UPGRADE:
        exitCode = actionUpgrade(clusterName, serviceArgs.getActionUpgradeArgs());
        break;

      case ACTION_VERSION:
        exitCode = actionVersion();
        break;
      
      default:
        throw new SliderException(EXIT_UNIMPLEMENTED,
            "Unimplemented: " + action);
    }
   
    return exitCode;
  }

  /**
   * Perform everything needed to init the hadoop binding.
   * This assumes that the service is already  in inited or started state
   * @throws IOException
   * @throws SliderException
   */
  protected void initHadoopBinding() throws IOException, SliderException {
    // validate the client
    validateSliderClientEnvironment(null);
    //create the YARN client
    yarnClient = new SliderYarnClientImpl();
    yarnClient.init(getConfig());
    if (getServiceState() == STATE.STARTED) {
      yarnClient.start();
    }
    addService(yarnClient);
    yarnAppListClient =
        new YarnAppListClient(yarnClient, getUsername(), getConfig());
    // create the filesystem
    sliderFileSystem = new SliderFileSystem(getConfig());
  }

  /**
   * Delete the zookeeper node associated with the calling user and the cluster
   * TODO: YARN registry operations
   **/
  @VisibleForTesting
  public boolean deleteZookeeperNode(String clusterName) throws YarnException, IOException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    Exception e = null;
    try {
      ZKIntegration client = getZkClient(clusterName, user);
      if (client != null) {
        if (client.exists(zkPath)) {
          log.info("Deleting zookeeper path {}", zkPath);
        }
        client.deleteRecursive(zkPath);
        return true;
      }
    } catch (InterruptedException | BadConfigException | KeeperException ex) {
      e = ex;
    }
    if (e != null) {
      log.warn("Unable to recursively delete zk node {}", zkPath, e);
    }

    return false;
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   *
   * @param clusterName slider application name
   * @param nameOnly should the name only be created (i.e. don't create ZK node)
   * @return the path, using the policy implemented in
   *   {@link ZKIntegration#mkClusterPath(String, String)}
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public String createZookeeperNode(String clusterName, Boolean nameOnly) throws YarnException, IOException {
    try {
      return createZookeeperNodeInner(clusterName, nameOnly);
    } catch (KeeperException.NodeExistsException e) {
      return null;
    } catch (KeeperException e) {
      return null;
    } catch (InterruptedException e) {
      throw new InterruptedIOException(e.toString());
    }
  }

  /**
   * Create the zookeeper node associated with the calling user and the cluster
   * -throwing exceptions on any failure
   * @param clusterName cluster name
   * @param nameOnly create the path, not the node
   * @return the path, using the policy implemented in
   *   {@link ZKIntegration#mkClusterPath(String, String)}
   * @throws YarnException
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  @VisibleForTesting
  public String createZookeeperNodeInner(String clusterName, Boolean nameOnly)
      throws YarnException, IOException, KeeperException, InterruptedException {
    String user = getUsername();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    if (nameOnly) {
      return zkPath;
    }
    ZKIntegration client = getZkClient(clusterName, user);
    if (client != null) {
      // set up the permissions. This must be done differently on a secure cluster from an insecure
      // one
      List<ACL> zkperms = new ArrayList<>();
      if (UserGroupInformation.isSecurityEnabled()) {
        zkperms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
        zkperms.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
      } else {
        zkperms.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));
      }
      client.createPath(zkPath, "",
          zkperms,
          CreateMode.PERSISTENT);
      return zkPath;
    } else {
      return null;
    }
  }

  /**
   * Gets a zookeeper client, returns null if it cannot connect to zookeeper
   **/
  protected ZKIntegration getZkClient(String clusterName, String user) throws YarnException {
    String registryQuorum = lookupZKQuorum();
    ZKIntegration client = null;
    try {
      BlockingZKWatcher watcher = new BlockingZKWatcher();
      client = ZKIntegration.newInstance(registryQuorum, user, clusterName, true, false, watcher,
          ZKIntegration.SESSION_TIMEOUT);
      boolean fromCache = client.init();
      if (!fromCache) {
        watcher.waitForZKConnection(2 * 1000);
      }
    } catch (InterruptedException e) {
      client = null;
      log.warn("Interrupted - unable to connect to zookeeper quorum {}",
          registryQuorum, e);
    } catch (IOException e) {
      log.warn("Unable to connect to zookeeper quorum {}", registryQuorum, e);
    }
    return client;
  }

  /**
   * Keep this signature for backward compatibility with
   * force=true by default.
   */
  @Override
  public int actionDestroy(String appName)
      throws YarnException, IOException {
    validateClusterName(appName);
    verifyNoLiveApp(appName, "Destroy");
    Path appDir = sliderFileSystem.buildClusterDirPath(appName);
    FileSystem fs = sliderFileSystem.getFileSystem();
    if (fs.exists(appDir)) {
      if (fs.delete(appDir, true)) {
        log.info("Successfully deleted application dir for " + appName);
      } else {
        String message =
            "Failed to delete application + " + appName + " at:  " + appDir;
        log.info(message);
        throw new YarnException(message);
      }
    }
    if (!deleteZookeeperNode(appName)) {
      String message =
          "Failed to cleanup cleanup application " + appName + " in zookeeper";
      log.warn(message);
      throw new YarnException(message);
    }

    //TODO clean registry?
    String registryPath = SliderRegistryUtils.registryPathForInstance(
        appName);
    try {
      getRegistryOperations().delete(registryPath, true);
    } catch (IOException e) {
      log.warn("Error deleting registry entry {}: {} ", registryPath, e, e);
    } catch (SliderException e) {
      log.warn("Error binding to registry {} ", e, e);
    }

    log.info("Destroyed cluster {}", appName);
    return EXIT_SUCCESS;
  }

  
  @Override
  public int actionAmSuicide(String clustername,
      ActionAMSuicideArgs args) throws YarnException, IOException {
    SliderClusterOperations clusterOperations =
      createClusterOperations(clustername);
    clusterOperations.amSuicide(args.message, args.exitcode, args.waittime);
    return EXIT_SUCCESS;
  }

  private Application getApplicationFromArgs(String clusterName,
      AbstractClusterBuildingActionArgs args) throws IOException {
    File file = args.getAppDef();
    Path filePath = new Path(file.getAbsolutePath());
    log.info("Loading app definition from: " + filePath);
    Application application =
        jsonSerDeser.load(FileSystem.getLocal(getConfig()), filePath);
    if(args.lifetime > 0) {
      application.setLifetime(args.lifetime);
    }
    application.setName(clusterName);
    return application;
  }

  public int actionBuild(Application application) throws YarnException,
      IOException {
    Path appDir = checkAppNotExistOnHdfs(application);
    ServiceApiUtil.validateAndResolveApplication(application,
        sliderFileSystem, getConfig());
    persistApp(appDir, application);
    deployedClusterName = application.getName();
    return EXIT_SUCCESS;
  }

  public ApplicationId actionCreate(Application application)
      throws IOException, YarnException {
    String appName = application.getName();
    validateClusterName(appName);
    ServiceApiUtil.validateAndResolveApplication(application,
        sliderFileSystem, getConfig());
    verifyNoLiveApp(appName, "Create");
    Path appDir = checkAppNotExistOnHdfs(application);

    ApplicationId appId = submitApp(application);
    application.setId(appId.toString());
    // write app definition on to hdfs
    persistApp(appDir, application);
    return appId;
    //TODO deal with registry
  }

  private ApplicationId submitApp(Application app)
      throws IOException, YarnException {
    String appName = app.getName();
    Configuration conf = getConfig();
    Path appRootDir = sliderFileSystem.buildClusterDirPath(app.getName());
    deployedClusterName = appName;

    YarnClientApplication yarnApp =  yarnClient.createApplication();
    ApplicationSubmissionContext submissionContext =
        yarnApp.getApplicationSubmissionContext();
    applicationId = submissionContext.getApplicationId();
    submissionContext.setKeepContainersAcrossApplicationAttempts(true);
    if (app.getLifetime() > 0) {
      Map<ApplicationTimeoutType, Long> appTimeout = new HashMap<>();
      appTimeout.put(ApplicationTimeoutType.LIFETIME, app.getLifetime());
      submissionContext.setApplicationTimeouts(appTimeout);
    }
    submissionContext.setMaxAppAttempts(conf.getInt(KEY_AM_RESTART_LIMIT, 2));

    Map<String, LocalResource> localResources = new HashMap<>();

    // copy local slideram-log4j.properties to hdfs and add to localResources
    boolean hasSliderAMLog4j =
        addAMLog4jResource(appName, conf, localResources);
    // copy jars to hdfs and add to localResources
    addJarResource(appName, localResources);
    // add keytab if in secure env
    addKeytabResourceIfSecure(sliderFileSystem, localResources, conf, appName);
    printLocalResources(localResources);

    //TODO SliderAMClientProvider#copyEnvVars
    //TODO localResource putEnv

    Map<String, String> env = addAMEnv(conf);

    // create AM CLI
    String cmdStr =
        buildCommandLine(appName, conf, appRootDir, hasSliderAMLog4j);

    //TODO set log aggregation context
    //TODO set retry window
    submissionContext.setResource(Resource.newInstance(
        conf.getLong(KEY_AM_RESOURCE_MEM, DEFAULT_KEY_AM_RESOURCE_MEM), 1));
    submissionContext.setQueue(conf.get(KEY_YARN_QUEUE, app.getQueue()));
    submissionContext.setApplicationName(appName);
    submissionContext.setApplicationType(SliderKeys.APP_TYPE);
    Set<String> appTags =
        AbstractClientProvider.createApplicationTags(appName, null, null);
    if (!appTags.isEmpty()) {
      submissionContext.setApplicationTags(appTags);
    }
    ContainerLaunchContext amLaunchContext =
        Records.newRecord(ContainerLaunchContext.class);
    amLaunchContext.setCommands(Collections.singletonList(cmdStr));
    amLaunchContext.setEnvironment(env);
    amLaunchContext.setLocalResources(localResources);
    addCredentialsIfSecure(conf, amLaunchContext);
    submissionContext.setAMContainerSpec(amLaunchContext);
    submitApplication(submissionContext);
    return submissionContext.getApplicationId();
  }

  @VisibleForTesting
  public ApplicationId submitApplication(ApplicationSubmissionContext context)
      throws IOException, YarnException {
    return yarnClient.submitApplication(context);
  }

  private void printLocalResources(Map<String, LocalResource> map) {
    log.info("Added LocalResource for localization: ");
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, LocalResource> entry : map.entrySet()) {
      builder.append(entry.getKey()).append(" -> ")
          .append(entry.getValue().getResource().getFile())
          .append(System.lineSeparator());
    }
    log.info(builder.toString());
  }

  private void addCredentialsIfSecure(Configuration conf,
      ContainerLaunchContext amLaunchContext) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      // pick up oozie credentials
      Credentials credentials =
          CredentialUtils.loadTokensFromEnvironment(System.getenv(), conf);
      if (credentials == null) {
        // nothing from oozie, so build up directly
        credentials = new Credentials(
            UserGroupInformation.getCurrentUser().getCredentials());
        CredentialUtils.addRMRenewableFSDelegationTokens(conf,
            sliderFileSystem.getFileSystem(), credentials);
      } else {
        log.info("Using externally supplied credentials to launch AM");
      }
      amLaunchContext.setTokens(CredentialUtils.marshallCredentials(credentials));
    }
  }

  private String buildCommandLine(String appName, Configuration conf,
      Path appRootDir, boolean hasSliderAMLog4j) throws BadConfigException {
    JavaCommandLineBuilder CLI = new JavaCommandLineBuilder();
    CLI.forceIPv4().headless();
    //TODO CLI.setJVMHeap
    //TODO CLI.addJVMOPTS
    if (hasSliderAMLog4j) {
      CLI.sysprop(SYSPROP_LOG4J_CONFIGURATION, LOG4J_SERVER_PROP_FILENAME);
      CLI.sysprop(SYSPROP_LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    }
    CLI.add(SliderAppMaster.SERVICE_CLASSNAME);
    CLI.add(ACTION_CREATE, appName);
    //TODO debugAM CLI.add(Arguments.ARG_DEBUG)
    CLI.add(Arguments.ARG_CLUSTER_URI, appRootDir.toUri());
//    InetSocketAddress rmSchedulerAddress = getRmSchedulerAddress(conf);
//    String rmAddr = NetUtils.getHostPortString(rmSchedulerAddress);
//    CLI.add(Arguments.ARG_RM_ADDR, rmAddr);
    // pass the registry binding
    CLI.addConfOptionToCLI(conf, RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    CLI.addMandatoryConfOption(conf, RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    if(isHadoopClusterSecure(conf)) {
      //TODO Is this required ??
      // if the cluster is secure, make sure that
      // the relevant security settings go over
      CLI.addConfOption(conf, DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY);
    }
//    // copy over any/all YARN RM client values, in case the server-side XML conf file
//    // has the 0.0.0.0 address
//    CLI.addConfOptions(conf, YarnConfiguration.RM_ADDRESS,
//        YarnConfiguration.RM_CLUSTER_ID, YarnConfiguration.RM_HOSTNAME,
//        YarnConfiguration.RM_PRINCIPAL);

    // write out the path output
    CLI.addOutAndErrFiles(STDOUT_AM, STDERR_AM);
    String cmdStr = CLI.build();
    log.info("Completed setting up app master command: {}", cmdStr);
    return cmdStr;
  }

  private Map<String, String> addAMEnv(Configuration conf)
      throws IOException {
    Map<String, String> env = new HashMap<>();
    ClasspathConstructor classpath =
        buildClasspath(SliderKeys.SUBMITTED_CONF_DIR, "lib",
            sliderFileSystem, getUsingMiniMRCluster());
    env.put("CLASSPATH", classpath.buildClasspath());
    env.put("LANG", "en_US.UTF-8");
    env.put("LC_ALL", "en_US.UTF-8");
    env.put("LANGUAGE", "en_US.UTF-8");
    String jaas = System.getenv(HADOOP_JAAS_DEBUG);
    if (jaas != null) {
      env.put(HADOOP_JAAS_DEBUG, jaas);
    }
    if (!UserGroupInformation.isSecurityEnabled()) {
      String userName = UserGroupInformation.getCurrentUser().getUserName();
      log.info("Run as user " + userName);
      // HADOOP_USER_NAME env is used by UserGroupInformation when log in
      // This env makes AM run as this user
      env.put("HADOOP_USER_NAME", userName);
    }
    env.putAll(getAmLaunchEnv(conf));
    log.info("AM env: \n{}", stringifyMap(env));
    return env;
  }

  private Path addJarResource(String appName,
      Map<String, LocalResource> localResources)
      throws IOException, SliderException {
    Path libPath = sliderFileSystem.buildClusterDirPath(appName);
    ProviderUtils
        .addProviderJar(localResources, SliderAppMaster.class, SLIDER_JAR,
            sliderFileSystem, libPath, "lib", false);
    Path dependencyLibTarGzip = sliderFileSystem.getDependencyTarGzip();
    if (sliderFileSystem.isFile(dependencyLibTarGzip)) {
      log.info("Loading lib tar from " + sliderFileSystem.getFileSystem()
          .getScheme() + ": "  + dependencyLibTarGzip);
      SliderUtils.putAmTarGzipAndUpdate(localResources, sliderFileSystem);
    } else {
      String[] libs = SliderUtils.getLibDirs();
      log.info("Loading dependencies from local file system: " + Arrays
          .toString(libs));
      for (String libDirProp : libs) {
        ProviderUtils
            .addAllDependencyJars(localResources, sliderFileSystem, libPath,
                "lib", libDirProp);
      }
    }
    return libPath;
  }

  private boolean addAMLog4jResource(String appName, Configuration conf,
      Map<String, LocalResource> localResources)
      throws IOException, BadClusterStateException {
    boolean hasSliderAMLog4j = false;
    String hadoopConfDir =
        System.getenv(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    if (hadoopConfDir != null) {
      File localFile =
          new File(hadoopConfDir, SliderKeys.LOG4J_SERVER_PROP_FILENAME);
      if (localFile.exists()) {
        Path localFilePath = createLocalPath(localFile);
        Path appDirPath = sliderFileSystem.buildClusterDirPath(appName);
        Path remoteConfPath =
            new Path(appDirPath, SliderKeys.SUBMITTED_CONF_DIR);
        Path remoteFilePath =
            new Path(remoteConfPath, SliderKeys.LOG4J_SERVER_PROP_FILENAME);
        copy(conf, localFilePath, remoteFilePath);
        LocalResource localResource = sliderFileSystem
            .createAmResource(remoteConfPath, LocalResourceType.FILE);
        localResources.put(localFilePath.getName(), localResource);
        hasSliderAMLog4j = true;
      }
    }
    return hasSliderAMLog4j;
  }

  private Path checkAppNotExistOnHdfs(Application application)
      throws IOException, SliderException {
    Path appDir = sliderFileSystem.buildClusterDirPath(application.getName());
    sliderFileSystem.verifyDirectoryNonexistent(
        new Path(appDir, application.getName() + ".json"));
    return appDir;
  }

  private Path checkAppExistOnHdfs(String appName)
      throws IOException, SliderException {
    Path appDir = sliderFileSystem.buildClusterDirPath(appName);
    sliderFileSystem.verifyPathExists(
        new Path(appDir, appName + ".json"));
    return appDir;
  }

  private void persistApp(Path appDir, Application application)
      throws IOException, SliderException {
    FsPermission appDirPermission = new FsPermission("750");
    sliderFileSystem.createWithPermissions(appDir, appDirPermission);
    Path appJson = new Path(appDir, application.getName() + ".json");
    jsonSerDeser
        .save(sliderFileSystem.getFileSystem(), appJson, application, true);
    log.info(
        "Persisted application " + application.getName() + " at " + appJson);
  }

  private void addKeytabResourceIfSecure(SliderFileSystem fileSystem,
      Map<String, LocalResource> localResource, Configuration conf,
      String appName) throws IOException, BadConfigException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    String keytabPreInstalledOnHost =
        conf.get(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
    if (StringUtils.isEmpty(keytabPreInstalledOnHost)) {
      String amKeytabName =
          conf.get(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
      String keytabDir = conf.get(SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR);
      Path keytabPath =
          fileSystem.buildKeytabPath(keytabDir, amKeytabName, appName);
      if (fileSystem.getFileSystem().exists(keytabPath)) {
        LocalResource keytabRes =
            fileSystem.createAmResource(keytabPath, LocalResourceType.FILE);
        localResource
            .put(SliderKeys.KEYTAB_DIR + "/" + amKeytabName, keytabRes);
        log.info("Adding AM keytab on hdfs: " + keytabPath);
      } else {
        log.warn("No keytab file was found at {}.", keytabPath);
        if (conf.getBoolean(KEY_AM_LOGIN_KEYTAB_REQUIRED, false)) {
          throw new BadConfigException("No keytab file was found at %s.",
              keytabPath);
        } else {
          log.warn("The AM will be "
              + "started without a kerberos authenticated identity. "
              + "The application is therefore not guaranteed to remain "
              + "operational beyond 24 hours.");
        }
      }
    }
  }

  @Override
  public int actionUpgrade(String clustername, ActionUpgradeArgs upgradeArgs)
      throws YarnException, IOException {
  //TODO
    return 0;
  }

  @Override
  public int actionKeytab(ActionKeytabArgs keytabInfo)
      throws YarnException, IOException {
    if (keytabInfo.install) {
      return actionInstallKeytab(keytabInfo);
    } else if (keytabInfo.delete) {
      return actionDeleteKeytab(keytabInfo);
    } else if (keytabInfo.list) {
      return actionListKeytab(keytabInfo);
    } else {
      throw new BadCommandArgumentsException(
          "Keytab option specified not found.\n"
          + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));
    }
  }

  private int actionListKeytab(ActionKeytabArgs keytabInfo) throws IOException {
    String folder = keytabInfo.folder != null ? keytabInfo.folder : StringUtils.EMPTY;
    Path keytabPath = sliderFileSystem.buildKeytabInstallationDirPath(folder);
    RemoteIterator<LocatedFileStatus> files =
        sliderFileSystem.getFileSystem().listFiles(keytabPath, true);
    log.info("Keytabs:");
    while (files.hasNext()) {
      log.info("\t" + files.next().getPath().toString());
    }

    return EXIT_SUCCESS;
  }

  private int actionDeleteKeytab(ActionKeytabArgs keytabInfo)
      throws BadCommandArgumentsException, IOException {
    if (StringUtils.isEmpty(keytabInfo.folder)) {
      throw new BadCommandArgumentsException(
          "A valid destination keytab sub-folder name is required (e.g. 'security').\n"
          + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));
    }

    if (StringUtils.isEmpty(keytabInfo.keytab)) {
      throw new BadCommandArgumentsException("A keytab name is required.");
    }

    Path pkgPath = sliderFileSystem.buildKeytabInstallationDirPath(keytabInfo.folder);

    Path fileInFs = new Path(pkgPath, keytabInfo.keytab );
    log.info("Deleting keytab {}", fileInFs);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    require(sfs.exists(fileInFs), "No keytab to delete found at %s",
        fileInFs.toUri());
    sfs.delete(fileInFs, false);

    return EXIT_SUCCESS;
  }

  private int actionInstallKeytab(ActionKeytabArgs keytabInfo)
      throws BadCommandArgumentsException, IOException {
    Path srcFile = null;
    require(isSet(keytabInfo.folder),
        "A valid destination keytab sub-folder name is required (e.g. 'security').\n"
        + CommonArgs.usage(serviceArgs, ACTION_KEYTAB));

    requireArgumentSet(Arguments.ARG_KEYTAB, keytabInfo.keytab);
    File keytabFile = new File(keytabInfo.keytab);
    require(keytabFile.isFile(),
        "Unable to access supplied keytab file at %s", keytabFile.getAbsolutePath());
    srcFile = new Path(keytabFile.toURI());

    Path pkgPath = sliderFileSystem.buildKeytabInstallationDirPath(keytabInfo.folder);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    sfs.mkdirs(pkgPath);
    sfs.setPermission(pkgPath, new FsPermission(
        FsAction.ALL, FsAction.NONE, FsAction.NONE));

    Path fileInFs = new Path(pkgPath, srcFile.getName());
    log.info("Installing keytab {} at {} and overwrite is {}.",
        srcFile, fileInFs, keytabInfo.overwrite);
    require(!(sfs.exists(fileInFs) && !keytabInfo.overwrite),
        "Keytab exists at %s. Use --overwrite to overwrite.", fileInFs.toUri());

    sfs.copyFromLocalFile(false, keytabInfo.overwrite, srcFile, fileInFs);
    sfs.setPermission(fileInFs,
        new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

    return EXIT_SUCCESS;
  }

  @Override
  public int actionResource(ActionResourceArgs resourceInfo)
      throws YarnException, IOException {
    if (resourceInfo.help) {
      actionHelp(ACTION_RESOURCE);
      return EXIT_SUCCESS;
    } else if (resourceInfo.install) {
      return actionInstallResource(resourceInfo);
    } else if (resourceInfo.delete) {
      return actionDeleteResource(resourceInfo);
    } else if (resourceInfo.list) {
      return actionListResource(resourceInfo);
    } else {
      throw new BadCommandArgumentsException(
          "Resource option specified not found.\n"
              + CommonArgs.usage(serviceArgs, ACTION_RESOURCE));
    }
  }

  private int actionListResource(ActionResourceArgs resourceInfo) throws IOException {
    String folder = resourceInfo.folder != null ? resourceInfo.folder : StringUtils.EMPTY;
    Path path = sliderFileSystem.buildResourcePath(folder);
    RemoteIterator<LocatedFileStatus> files =
        sliderFileSystem.getFileSystem().listFiles(path, true);
    log.info("Resources:");
    while (files.hasNext()) {
      log.info("\t" + files.next().getPath().toString());
    }

    return EXIT_SUCCESS;
  }

  private int actionDeleteResource(ActionResourceArgs resourceInfo)
      throws BadCommandArgumentsException, IOException {
    if (StringUtils.isEmpty(resourceInfo.resource)) {
      throw new BadCommandArgumentsException("A file name is required.");
    }

    Path fileInFs;
    if (resourceInfo.folder == null) {
      fileInFs = sliderFileSystem.buildResourcePath(resourceInfo.resource);
    } else {
      fileInFs = sliderFileSystem.buildResourcePath(resourceInfo.folder,
          resourceInfo.resource);
    }

    log.info("Deleting resource {}", fileInFs);
    FileSystem sfs = sliderFileSystem.getFileSystem();
    require(sfs.exists(fileInFs), "No resource to delete found at %s", fileInFs.toUri());
    sfs.delete(fileInFs, true);

    return EXIT_SUCCESS;
  }

  private int actionInstallResource(ActionResourceArgs resourceInfo)
      throws BadCommandArgumentsException, IOException {
    Path srcFile = null;
    String folder = resourceInfo.folder != null ? resourceInfo.folder : StringUtils.EMPTY;

    requireArgumentSet(Arguments.ARG_RESOURCE, resourceInfo.resource);
    File file = new File(resourceInfo.resource);
    require(file.isFile() || file.isDirectory(),
        "Unable to access supplied file at %s", file.getAbsolutePath());

    File[] files;
    if (file.isDirectory()) {
      files = file.listFiles();
    } else {
      files = new File[] { file };
    }

    Path pkgPath = sliderFileSystem.buildResourcePath(folder);
    FileSystem sfs = sliderFileSystem.getFileSystem();

    if (!sfs.exists(pkgPath)) {
      sfs.mkdirs(pkgPath);
      sfs.setPermission(pkgPath, new FsPermission(
          FsAction.ALL, FsAction.NONE, FsAction.NONE));
    } else {
      require(sfs.isDirectory(pkgPath), "Specified folder %s exists and is " +
          "not a directory", folder);
    }

    if (files != null) {
      for (File f : files) {
        srcFile = new Path(f.toURI());

        Path fileInFs = new Path(pkgPath, srcFile.getName());
        log.info("Installing file {} at {} and overwrite is {}.",
            srcFile, fileInFs, resourceInfo.overwrite);
        require(!(sfs.exists(fileInFs) && !resourceInfo.overwrite),
            "File exists at %s. Use --overwrite to overwrite.", fileInFs.toUri());

        sfs.copyFromLocalFile(false, resourceInfo.overwrite, srcFile, fileInFs);
        sfs.setPermission(fileInFs,
            new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));
      }
    }

    return EXIT_SUCCESS;
  }

  @Override
  public int actionClient(ActionClientArgs clientInfo) throws
      YarnException,
      IOException {
    if (clientInfo.install) {
      // TODO implement client install
      throw new UnsupportedOperationException("Client install not yet " +
          "supported");
    } else {
      throw new BadCommandArgumentsException(
          "Only install, keystore, and truststore commands are supported for the client.\n"
          + CommonArgs.usage(serviceArgs, ACTION_CLIENT));

    }
  }

  @Override
  public int actionUpdate(String clustername,
      AbstractClusterBuildingActionArgs buildInfo) throws
      YarnException, IOException {
    if (buildInfo.lifetime > 0) {
      updateLifetime(clustername, buildInfo.lifetime);
    } else {
      //TODO upgrade
    }
    return EXIT_SUCCESS;
  }

  public String updateLifetime(String appName, long lifetime)
      throws YarnException, IOException {
    EnumSet<YarnApplicationState> appStates = EnumSet.range(
        YarnApplicationState.NEW, YarnApplicationState.RUNNING);
    ApplicationReport report = findInstance(appName, appStates);
    if (report == null) {
      throw new YarnException("Application not found for " + appName);
    }
    ApplicationId appId = report.getApplicationId();
    log.info("Updating lifetime of an application: appName = " + appName
        + ", appId = " + appId+ ", lifetime = " + lifetime);
    Map<ApplicationTimeoutType, String> map = new HashMap<>();
    String newTimeout =
        Times.formatISO8601(System.currentTimeMillis() + lifetime * 1000);
    map.put(ApplicationTimeoutType.LIFETIME, newTimeout);
    UpdateApplicationTimeoutsRequest request =
        UpdateApplicationTimeoutsRequest.newInstance(appId, map);
    yarnClient.updateApplicationTimeouts(request);
    log.info("Successfully updated lifetime for an application: appName = "
        + appName + ", appId = " + appId
        + ". New expiry time in ISO8601 format is " + newTimeout);
    return newTimeout;
  }

  protected Map<String, String> getAmLaunchEnv(Configuration config) {
    String sliderAmLaunchEnv = config.get(KEY_AM_LAUNCH_ENV);
    log.debug("{} = {}", KEY_AM_LAUNCH_ENV, sliderAmLaunchEnv);
    // Multiple env variables can be specified with a comma (,) separator
    String[] envs = StringUtils.isEmpty(sliderAmLaunchEnv) ? null
        : sliderAmLaunchEnv.split(",");
    if (ArrayUtils.isEmpty(envs)) {
      return Collections.emptyMap();
    }
    Map<String, String> amLaunchEnv = new HashMap<>();
    for (String env : envs) {
      if (StringUtils.isNotEmpty(env)) {
        // Each env name/value is separated by equals sign (=)
        String[] tokens = env.split("=");
        if (tokens != null && tokens.length == 2) {
          String envKey = tokens[0];
          String envValue = tokens[1];
          for (Map.Entry<String, String> placeholder : generatePlaceholderKeyValueMap(
              env).entrySet()) {
            if (StringUtils.isNotEmpty(placeholder.getValue())) {
              envValue = envValue.replaceAll(
                  Pattern.quote(placeholder.getKey()), placeholder.getValue());
            }
          }
          if (Shell.WINDOWS) {
            envValue = "%" + envKey + "%;" + envValue;
          } else {
            envValue = "$" + envKey + ":" + envValue;
          }
          log.info("Setting AM launch env {}={}", envKey, envValue);
          amLaunchEnv.put(envKey, envValue);
        }
      }
    }
    return amLaunchEnv;
  }

  protected Map<String, String> generatePlaceholderKeyValueMap(String env) {
    String PLACEHOLDER_PATTERN = "\\$\\{[^{]+\\}";
    Pattern placeholderPattern = Pattern.compile(PLACEHOLDER_PATTERN);
    Matcher placeholderMatcher = placeholderPattern.matcher(env);
    Map<String, String> placeholderKeyValueMap = new HashMap<>();
    if (placeholderMatcher.find()) {
      String placeholderKey = placeholderMatcher.group();
      String systemKey = placeholderKey
          .substring(2, placeholderKey.length() - 1).toUpperCase(Locale.ENGLISH)
          .replaceAll("\\.", "_");
      String placeholderValue = getSystemEnv(systemKey);
      log.debug("Placeholder {}={}", placeholderKey, placeholderValue);
      placeholderKeyValueMap.put(placeholderKey, placeholderValue);
    }
    return placeholderKeyValueMap;
  }

  /**
   * verify that a live cluster isn't there
   * @param clustername cluster name
   * @param action
   * @throws SliderException with exit code EXIT_CLUSTER_LIVE
   * if a cluster of that name is either live or starting up.
   */
  public void verifyNoLiveApp(String clustername, String action) throws
                                                       IOException,
                                                       YarnException {
    List<ApplicationReport> existing = findAllLiveInstances(clustername);

    if (!existing.isEmpty()) {
      throw new SliderException(EXIT_APPLICATION_IN_USE,
          action +" failed for "
                              + clustername
                              + ": "
                              + E_CLUSTER_RUNNING + " :" +
                              existing.get(0));
    }
  }

  public String getUsername() throws IOException {
    return RegistryUtils.currentUser();
  }

  /**
   * Get the name of any deployed cluster
   * @return the cluster name
   */
  public String getDeployedClusterName() {
    return deployedClusterName;
  }

  /**
   * ask if the client is using a mini MR cluster
   * @return true if they are
   */
  private boolean getUsingMiniMRCluster() {
    return getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER,
        false);
  }


  /**
   * List Slider instances belonging to a specific user with a specific app
   * name and within a set of app states.
   * @param user user: "" means all users, null means "default"
   * @param appName name of the application set as a tag
   * @param appStates a set of states the applications should be in
   * @return a possibly empty list of Slider AMs
   */
  public List<ApplicationReport> listSliderInstances(String user,
      String appName, EnumSet<YarnApplicationState> appStates)
      throws YarnException, IOException {
    return yarnAppListClient.listInstances(user, appName, appStates);
  }

  /**
   * A basic list action to list live instances
   * @param clustername cluster name
   * @return success if the listing was considered successful
   * @throws IOException
   * @throws YarnException
   */
  public int actionList(String clustername) throws IOException, YarnException {
    ActionListArgs args = new ActionListArgs();
    args.live = true;
    return actionList(clustername, args);
  }

  /**
   * Implement the list action.
   * @param clustername List out specific instance name
   * @param args Action list arguments
   * @return 0 if one or more entries were listed
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @Override
  public int actionList(String clustername, ActionListArgs args)
      throws IOException, YarnException {
    Set<ApplicationReport> appInstances = getApplicationList(clustername, args);
    if (!appInstances.isEmpty()) {
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  /**
   * Retrieve a list of application instances satisfying the query criteria.
   * 
   * @param clustername
   *          List out specific instance name (set null for all)
   * @param args
   *          Action list arguments
   * @return the list of application names which satisfies the list criteria
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException
   *           if a specific instance was named but it was not found
   */
  public Set<ApplicationReport> getApplicationList(String clustername,
      ActionListArgs args) throws IOException, YarnException {
    if (args.help) {
      actionHelp(ACTION_LIST);
      // the above call throws an exception so the return is not really required
      return Collections.emptySet();
    }
    boolean live = args.live;
    String state = args.state;
    boolean listContainers = args.containers;
    boolean verbose = args.verbose;
    String version = args.version;
    Set<String> components = args.components;

    if (live && !state.isEmpty()) {
      throw new BadCommandArgumentsException(
          Arguments.ARG_LIVE + " and " + Arguments.ARG_STATE + " are exclusive");
    }
    if (listContainers && isUnset(clustername)) {
      throw new BadCommandArgumentsException(
          "Should specify an application instance with "
              + Arguments.ARG_CONTAINERS);
    }
    // specifying both --version and --components with --containers is okay
    if (StringUtils.isNotEmpty(version) && !listContainers) {
      throw new BadCommandArgumentsException(Arguments.ARG_VERSION
          + " can be specified only with " + Arguments.ARG_CONTAINERS);
    }
    if (!components.isEmpty() && !listContainers) {
      throw new BadCommandArgumentsException(Arguments.ARG_COMPONENTS
          + " can be specified only with " + Arguments.ARG_CONTAINERS);
    }

    // flag to indicate only services in a specific state are to be listed
    boolean listOnlyInState = live || !state.isEmpty();

    YarnApplicationState min, max;
    if (live) {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.RUNNING;
    } else if (!state.isEmpty()) {
      YarnApplicationState stateVal = extractYarnApplicationState(state);
      min = max = stateVal;
    } else {
      min = YarnApplicationState.NEW;
      max = YarnApplicationState.KILLED;
    }
    // get the complete list of persistent instances
    Map<String, Path> persistentInstances = sliderFileSystem.listPersistentInstances();

    if (persistentInstances.isEmpty() && isUnset(clustername)) {
      // an empty listing is a success if no cluster was named
      log.debug("No application instances found");
      return Collections.emptySet();
    }

    // and those the RM knows about
    EnumSet<YarnApplicationState> appStates = EnumSet.range(min, max);
    List<ApplicationReport> instances = listSliderInstances(null, clustername,
        appStates);
    sortApplicationsByMostRecent(instances);
    Map<String, ApplicationReport> reportMap =
        buildApplicationReportMap(instances, min, max);
    log.debug("Persisted {} deployed {} filtered[{}-{}] & de-duped to {}",
        persistentInstances.size(),
        instances.size(),
        min, max,
        reportMap.size() );

    List<ContainerInformation> containers = null;
    if (isSet(clustername)) {
      // only one instance is expected
      // resolve the persistent value
      Path persistent = persistentInstances.get(clustername);
      if (persistent == null) {
        throw unknownClusterException(clustername);
      }
      // create a new map with only that instance in it.
      // this restricts the output of results to this instance
      persistentInstances = new HashMap<>();
      persistentInstances.put(clustername, persistent);
      if (listContainers) {
        containers = getContainers(clustername);
      }
    }
    
    // at this point there is either the entire list or a stripped down instance
    Set<ApplicationReport> listedInstances = new HashSet<ApplicationReport>();
    for (String name : persistentInstances.keySet()) {
      ApplicationReport report = reportMap.get(name);
      if (!listOnlyInState || report != null) {
        // list the details if all were requested, or the filtering contained
        // a report
        listedInstances.add(report);
        // containers will be non-null when only one instance is requested
        String details = instanceDetailsToString(name, report,
            containers, version, components, verbose);
        print(details);
      }
    }
    
    return listedInstances;
  }

  public List<ContainerInformation> getContainers(String name)
      throws YarnException, IOException {
    SliderClusterOperations clusterOps = new SliderClusterOperations(
        bondToCluster(name));
    try {
      return clusterOps.getContainers();
    } catch (NoSuchNodeException e) {
      throw new BadClusterStateException(
          "Containers not found for application instance %s", name);
    }
  }


  /**
   * Extract the state of a Yarn application --state argument
   * @param state state argument
   * @return the application state
   * @throws BadCommandArgumentsException if the argument did not match
   * any known state
   */
  private YarnApplicationState extractYarnApplicationState(String state) throws
      BadCommandArgumentsException {
    YarnApplicationState stateVal;
    try {
      stateVal = YarnApplicationState.valueOf(state.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException("Unknown state: " + state);

    }
    return stateVal;
  }
  
  /**
   * Is an application active: accepted or running
   * @param report the application report
   * @return true if it is running or scheduled to run.
   */
  public boolean isApplicationActive(ApplicationReport report) {
    return report.getYarnApplicationState() == YarnApplicationState.RUNNING
                || report.getYarnApplicationState() == YarnApplicationState.ACCEPTED;
  }

  /**
   * Implement the islive action: probe for a cluster of the given name existing
   * @return exit code
   */

  @Override
  @VisibleForTesting
  public int actionFlex(String appName, ActionFlexArgs args)
      throws YarnException, IOException {
    Map<String, Long> componentCounts = new HashMap<>(args.getComponentMap()
        .size());
    for (Entry<String, String> entry : args.getComponentMap().entrySet()) {
      long numberOfContainers = Long.parseLong(entry.getValue());
      componentCounts.put(entry.getKey(), numberOfContainers);
    }
    // throw usage exception if no changes proposed
    if (componentCounts.size() == 0) {
      actionHelp(ACTION_FLEX);
    }
    flex(appName, componentCounts);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionExists(String name, boolean checkLive) throws YarnException, IOException {
    ActionExistsArgs args = new ActionExistsArgs();
    args.live = checkLive;
    return actionExists(name, args);
  }

  public int actionExists(String name, ActionExistsArgs args) throws YarnException, IOException {
    validateClusterName(name);
    boolean checkLive = args.live;
    log.debug("actionExists({}, {}, {})", name, checkLive, args.state);

    //initial probe for a cluster in the filesystem
    Path clusterDirectory = sliderFileSystem.buildClusterDirPath(name);
    if (!sliderFileSystem.getFileSystem().exists(clusterDirectory)) {
      throw unknownClusterException(name);
    }
    String state = args.state;
    if (!checkLive && isUnset(state)) {
      log.info("Application {} exists", name);
      return EXIT_SUCCESS;
    }

    //test for liveness/state
    boolean inDesiredState = false;
    ApplicationReport instance;
    instance = findInstance(name);
    if (instance == null) {
      log.info("Application {} not running", name);
      return EXIT_FALSE;
    }
    if (checkLive) {
      // the app exists, check that it is not in any terminated state
      YarnApplicationState appstate = instance.getYarnApplicationState();
      log.debug(" current app state = {}", appstate);
      inDesiredState =
            appstate.ordinal() < YarnApplicationState.FINISHED.ordinal();
    } else {
      // scan for instance in single --state state
      state = state.toUpperCase(Locale.ENGLISH);
      YarnApplicationState desiredState = extractYarnApplicationState(state);
      List<ApplicationReport> userInstances = yarnClient
          .listDeployedInstances("", EnumSet.of(desiredState), name);
      ApplicationReport foundInstance =
          yarnClient.findAppInInstanceList(userInstances, name, desiredState);
      if (foundInstance != null) {
        // found in selected state: success
        inDesiredState = true;
        // mark this as the instance to report
        instance = foundInstance;
      }
    }

    OnDemandReportStringifier report =
        new OnDemandReportStringifier(instance);
    if (!inDesiredState) {
      //cluster in the list of apps but not running
      log.info("Application {} found but is in wrong state {}", name,
          instance.getYarnApplicationState());
      log.debug("State {}", report);
      return EXIT_FALSE;
    } else {
      log.debug("Application instance is in desired state");
      log.info("Application {} is {}\n{}", name,
          instance.getYarnApplicationState(), report);
      return EXIT_SUCCESS;
    }
  }


  @Override
  public int actionKillContainer(String name,
      ActionKillContainerArgs args) throws YarnException, IOException {
    String id = args.id;
    if (isUnset(id)) {
      throw new BadCommandArgumentsException("Missing container id");
    }
    log.info("killingContainer {}:{}", name, id);
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(name));
    try {
      clusterOps.killContainer(id);
    } catch (NoSuchNodeException e) {
      throw new BadClusterStateException("Container %s not found in cluster %s",
                                         id, name);
    }
    return EXIT_SUCCESS;
  }

  /**
   * Find an instance of an application belonging to the current user.
   * @param appname application name
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname)
      throws YarnException, IOException {
    return findInstance(appname, null);
  }
  
  /**
   * Find an instance of an application belonging to the current user and in
   * specific app states.
   * @param appname application name
   * @param appStates app states in which the application should be in
   * @return the app report or null if none is found
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  public ApplicationReport findInstance(String appname,
      EnumSet<YarnApplicationState> appStates)
      throws YarnException, IOException {
    return yarnAppListClient.findInstance(appname, appStates);
  }

  /**
   * find all live instances of a specific app -if there is >1 in the cluster,
   * this returns them all. State should be running or less
   * @param appname application name
   * @return the list of all matching application instances
   */
  private List<ApplicationReport> findAllLiveInstances(String appname)
    throws YarnException, IOException {
    
    return yarnAppListClient.findAllLiveInstances(appname);
  }

  /**
   * Connect to a Slider AM
   * @param app application report providing the details on the application
   * @return an instance
   * @throws YarnException
   * @throws IOException
   */
  private SliderClusterProtocol connect(ApplicationReport app)
      throws YarnException, IOException {

    try {
      return RpcBinder.getProxy(getConfig(),
                                yarnClient.getRmClient(),
                                app,
                                Constants.CONNECT_TIMEOUT,
                                Constants.RPC_TIMEOUT);
    } catch (InterruptedException e) {
      throw new SliderException(SliderExitCodes.EXIT_TIMED_OUT,
                              e,
                              "Interrupted waiting for communications with the Slider AM");
    }
  }

  @Override
  @VisibleForTesting
  public int actionStatus(String clustername, ActionStatusArgs statusArgs)
      throws YarnException, IOException {
    if (statusArgs.lifetime) {
      queryAndPrintLifetime(clustername);
      return EXIT_SUCCESS;
    }

    Application application = getApplication(clustername);
    String outfile = statusArgs.getOutput();
    if (outfile == null) {
      log.info(application.toString());
    } else {
      jsonSerDeser.save(application, new File(statusArgs.getOutput()));
    }
    return EXIT_SUCCESS;
  }

  @Override
  public Application actionStatus(String clustername)
      throws YarnException, IOException {
    return getApplication(clustername);
  }

  private void queryAndPrintLifetime(String appName)
      throws YarnException, IOException {
    ApplicationReport appReport = findInstance(appName);
    if (appReport == null) {
      throw new YarnException("No application found for " + appName);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter timeoutStr =
        new PrintWriter(new OutputStreamWriter(baos, Charset.forName("UTF-8")));
    try {
      ApplicationTimeout lifetime = appReport.getApplicationTimeouts()
          .get(ApplicationTimeoutType.LIFETIME);
      if (lifetime.getRemainingTime() == -1L) {
        timeoutStr.append(appName + " has no lifetime configured.");
      } else {
        timeoutStr.append("\t" + ApplicationTimeoutType.LIFETIME);
        timeoutStr.print(" expires at : " + lifetime.getExpiryTime());
        timeoutStr.println(
            ".\tRemaining Time : " + lifetime.getRemainingTime() + " seconds");
      }
      System.out.println(baos.toString("UTF-8"));
    } finally {
      timeoutStr.close();
    }
  }

  @Override
  public int actionVersion() {
    SliderVersionInfo.loadAndPrintVersionInfo(log);
    return EXIT_SUCCESS;
  }

  @Override
  public int actionStop(String appName, ActionFreezeArgs freezeArgs)
      throws YarnException, IOException {
    validateClusterName(appName);
    ApplicationReport app = findInstance(appName);
    if (app == null) {
      throw new ApplicationNotFoundException(
          "Application " + appName + " doesn't exist in RM.");
    }

    if (terminatedStates.contains(app.getYarnApplicationState())) {
      log.info("Application {} is already in a terminated state {}", appName,
          app.getYarnApplicationState());
      return EXIT_SUCCESS;
    }

    try {
      SliderClusterProtocol appMaster = connect(app);
      Messages.StopClusterRequestProto r =
          Messages.StopClusterRequestProto.newBuilder()
              .setMessage(freezeArgs.message).build();
      appMaster.stopCluster(r);
      log.info("Application " + appName + " is being gracefully stopped...");
      long startTime = System.currentTimeMillis();
      int pollCount = 0;
      while (true) {
        Thread.sleep(200);
        ApplicationReport report =
            yarnClient.getApplicationReport(app.getApplicationId());
        if (terminatedStates.contains(report.getYarnApplicationState())) {
          log.info("Application " + appName + " is stopped.");
          break;
        }
        // kill after 10 seconds.
        if ((System.currentTimeMillis() - startTime) > 10000) {
          log.info("Stop operation timeout stopping, forcefully kill the app "
              + appName);
          yarnClient
              .killApplication(app.getApplicationId(), freezeArgs.message);
          break;
        }
        if (++pollCount % 10 == 0) {
          log.info("Waiting for application " + appName + " to be stopped.");
        }
      }
    } catch (IOException | YarnException | InterruptedException e) {
      log.info("Failed to stop " + appName
          + " gracefully, forcefully kill the app.");
      yarnClient.killApplication(app.getApplicationId(), freezeArgs.message);
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionStart(String appName, ActionThawArgs thaw)
      throws YarnException, IOException {
    validateClusterName(appName);
    Path appDir = checkAppExistOnHdfs(appName);
    Application application = ServiceApiUtil.loadApplication(sliderFileSystem,
        appName);
    ServiceApiUtil.validateAndResolveApplication(application,
        sliderFileSystem, getConfig());
    // see if it is actually running and bail out;
    verifyNoLiveApp(appName, "Thaw");
    ApplicationId appId = submitApp(application);
    application.setId(appId.toString());
    // write app definition on to hdfs
    persistApp(appDir, application);
    return 0;
  }

  public Map<String, Long> flex(String appName, Map<String, Long>
      componentCounts) throws YarnException, IOException {
    validateClusterName(appName);
    Application persistedApp = ServiceApiUtil.loadApplication(sliderFileSystem,
        appName);
    Map<String, Long> original = new HashMap<>(componentCounts.size());
    for (Component persistedComp : persistedApp.getComponents()) {
      String name = persistedComp.getName();
      if (componentCounts.containsKey(persistedComp.getName())) {
        original.put(name, persistedComp.getNumberOfContainers());
        persistedComp.setNumberOfContainers(componentCounts.get(name));
      }
    }
    if (original.size() < componentCounts.size()) {
      componentCounts.keySet().removeAll(original.keySet());
      throw new YarnException("Components " + componentCounts.keySet()
          + " do not exist in app definition.");
    }
    jsonSerDeser
        .save(sliderFileSystem.getFileSystem(), ServiceApiUtil.getAppJsonPath(
            sliderFileSystem, appName), persistedApp, true);
    log.info("Updated app definition file for components " + componentCounts
        .keySet());

    ApplicationReport instance = findInstance(appName);
    if (instance != null) {
      log.info("Flexing running app " + appName);
      SliderClusterProtocol appMaster = connect(instance);
      SliderClusterOperations clusterOps =
          new SliderClusterOperations(appMaster);
      clusterOps.flex(componentCounts);
      for (Entry<String, Long> componentCount : componentCounts.entrySet()) {
        log.info(
            "Application name = " + appName + ", Component name = " +
                componentCount.getKey() + ", number of containers updated " +
                "from " + original.get(componentCount.getKey()) + " to " +
                componentCount.getValue());
      }
    } else {
      String message = "Application " + appName + "does not exist in RM. ";
      throw new YarnException(message);
    }
    return original;
  }

  /**
   * Connect to a live cluster and get its current state
   *
   * @param appName the cluster name
   * @return its description
   */
  @VisibleForTesting
  public Application getApplication(String appName)
      throws YarnException, IOException {
    validateClusterName(appName);
    SliderClusterOperations clusterOperations =
        createClusterOperations(appName);
    return clusterOperations.getApplication();
  }

  /**
   * Bond to a running cluster
   * @param clustername cluster name
   * @return the AM RPC client
   * @throws SliderException if the cluster is unkown
   */
  private SliderClusterProtocol bondToCluster(String clustername) throws
                                                                  YarnException,
                                                                  IOException {
    if (clustername == null) {
      throw unknownClusterException("(undefined)");
    }
    ApplicationReport instance = findInstance(clustername,
        SliderUtils.getAllLiveAppStates());
    if (null == instance) {
      throw unknownClusterException(clustername);
    }
    return connect(instance);
  }

  /**
   * Create a cluster operations instance against a given cluster
   * @param clustername cluster name
   * @return a bonded cluster operations instance
   * @throws YarnException YARN issues
   * @throws IOException IO problems
   */
  private SliderClusterOperations createClusterOperations(String clustername) throws
                                                                            YarnException,
                                                                            IOException {
    SliderClusterProtocol sliderAM = bondToCluster(clustername);
    return new SliderClusterOperations(sliderAM);
  }

  /**
   * Generate an exception for an unknown cluster
   * @param clustername cluster name
   * @return an exception with text and a relevant exit code
   */
  public UnknownApplicationInstanceException unknownClusterException(String clustername) {
    return UnknownApplicationInstanceException.unknownInstance(clustername);
  }

  @Override
  public String toString() {
    return "Slider Client in state " + getServiceState()
           + " and Slider Application Instance " + deployedClusterName;
  }

  /**
   * Get all YARN applications
   * @return a possibly empty list
   * @throws YarnException
   * @throws IOException
   */
  @VisibleForTesting
  public List<ApplicationReport> getApplications()
      throws YarnException, IOException {
    return yarnClient.getApplications();
  }

  @Override
  public int actionResolve(ActionResolveArgs args)
      throws YarnException, IOException {
    // as this is an API entry point, validate
    // the arguments
    args.validate();
    String path = SliderRegistryUtils.resolvePath(args.path);
    ServiceRecordMarshal serviceRecordMarshal = new ServiceRecordMarshal();
    try {
      if (args.list) {
        File destDir = args.destdir;
        if (destDir != null && !destDir.exists() && !destDir.mkdirs()) {
          throw new IOException("Failed to create directory: " + destDir);
        }


        Map<String, ServiceRecord> recordMap;
        Map<String, RegistryPathStatus> znodes;
        try {
          znodes = statChildren(registryOperations, path);
          recordMap = extractServiceRecords(registryOperations,
              path,
              znodes.values());
        } catch (PathNotFoundException e) {
          // treat the root directory as if if is always there
        
          if ("/".equals(path)) {
            znodes = new HashMap<>(0);
            recordMap = new HashMap<>(0);
          } else {
            throw e;
          }
        }
        // subtract all records from the znodes map to get pure directories
        log.info("Entries: {}", znodes.size());

        for (String name : znodes.keySet()) {
          println("  " + name);
        }
        println("");

        log.info("Service records: {}", recordMap.size());
        for (Entry<String, ServiceRecord> recordEntry : recordMap.entrySet()) {
          String name = recordEntry.getKey();
          ServiceRecord instance = recordEntry.getValue();
          String json = serviceRecordMarshal.toJson(instance);
          if (destDir == null) {
            println(name);
            println(json);
          } else {
            String filename = RegistryPathUtils.lastPathEntry(name) + ".json";
            File jsonFile = new File(destDir, filename);
            write(jsonFile, serviceRecordMarshal.toBytes(instance));
          }
        }
      } else  {
        // resolve single entry
        ServiceRecord instance = resolve(path);
        File outFile = args.out;
        if (args.destdir != null) {
          outFile = new File(args.destdir, RegistryPathUtils.lastPathEntry(path));
        }
        if (outFile != null) {
          write(outFile, serviceRecordMarshal.toBytes(instance));
        } else {
          println(serviceRecordMarshal.toJson(instance));
        }
      }
    } catch (PathNotFoundException | NoRecordException e) {
      // no record at this path
      throw new NotFoundException(e, path);
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionRegistry(ActionRegistryArgs registryArgs) throws
      YarnException,
      IOException {
    // as this is also a test entry point, validate
    // the arguments
    registryArgs.validate();
    try {
      if (registryArgs.list) {
        actionRegistryList(registryArgs);
      } else if (registryArgs.listConf) {
        // list the configurations
        actionRegistryListConfigsYarn(registryArgs);
      } else if (registryArgs.listExports) {
        // list the exports
        actionRegistryListExports(registryArgs);
      } else if (isSet(registryArgs.getConf)) {
        // get a configuration
        PublishedConfiguration publishedConfiguration =
            actionRegistryGetConfig(registryArgs);
        outputConfig(publishedConfiguration, registryArgs);
      } else if (isSet(registryArgs.getExport)) {
        // get a export group
        PublishedExports publishedExports =
            actionRegistryGetExport(registryArgs);
        outputExport(publishedExports, registryArgs);
      } else {
        // it's an unknown command
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
//      JDK7
    } catch (FileNotFoundException e) {
      log.info("{}", e.toString());
      log.debug("{}", e, e);
      return EXIT_NOT_FOUND;
    } catch (PathNotFoundException e) {
      log.info("{}", e.toString());
      log.debug("{}", e, e);
      return EXIT_NOT_FOUND;
    }
    return EXIT_SUCCESS;
  }

  /**
   * Registry operation
   *
   * @param registryArgs registry Arguments
   * @return the instances (for tests)
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  @VisibleForTesting
  public Collection<ServiceRecord> actionRegistryList(
      ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    String serviceType = registryArgs.serviceType;
    String name = registryArgs.name;
    RegistryOperations operations = getRegistryOperations();
    Collection<ServiceRecord> serviceRecords;
    if (StringUtils.isEmpty(name)) {
      String path = serviceclassPath(currentUser(), serviceType);

      try {
        Map<String, ServiceRecord> recordMap =
            listServiceRecords(operations, path);
        if (recordMap.isEmpty()) {
          throw new UnknownApplicationInstanceException(
              "No applications registered under " + path);
        }
        serviceRecords = recordMap.values();
      } catch (PathNotFoundException e) {
        throw new NotFoundException(path, e);
      }
    } else {
      ServiceRecord instance = lookupServiceRecord(registryArgs);
      serviceRecords = new ArrayList<>(1);
      serviceRecords.add(instance);
    }

    for (ServiceRecord serviceRecord : serviceRecords) {
      logInstance(serviceRecord, registryArgs.verbose);
    }
    return serviceRecords;
  }

  @Override
  public int actionDiagnostic(ActionDiagnosticArgs diagnosticArgs) {
    try {
      if (diagnosticArgs.client) {
        actionDiagnosticClient(diagnosticArgs);
      } else if (diagnosticArgs.application) {
        // TODO print configs of application - get from AM
      } else if (diagnosticArgs.yarn) {
        // This method prints yarn nodes info and yarn configs.
        // We can just use yarn node CLI instead which is much more richful
        // for yarn configs, this method reads local config which is only client
        // config not cluster configs.
//        actionDiagnosticYarn(diagnosticArgs);
      } else if (diagnosticArgs.credentials) {
        // actionDiagnosticCredentials internall only runs a bare 'klist' command...
        // IMHO, the user can just run klist on their own with extra options supported, don't
        // actually see the point of this method.
//        actionDiagnosticCredentials();
      } else if (diagnosticArgs.all) {
        actionDiagnosticAll(diagnosticArgs);
      } else if (diagnosticArgs.level) {
        // agent is removed
      } else {
        // it's an unknown option
        log.info(CommonArgs.usage(serviceArgs, ACTION_DIAGNOSTICS));
        return EXIT_USAGE;
      }
    } catch (Exception e) {
      log.error(e.toString());
      return EXIT_FALSE;
    }
    return EXIT_SUCCESS;
  }

  private void actionDiagnosticAll(ActionDiagnosticArgs diagnosticArgs)
      throws IOException, YarnException {
    // assign application name from param to each sub diagnostic function
    actionDiagnosticClient(diagnosticArgs);
    // actionDiagnosticSlider only prints the agent location on hdfs,
    // which is invalid now.
    // actionDiagnosticCredentials only runs 'klist' command, IMHO, the user
    // can just run klist on its own with extra options supported, don't
    // actually see the point of this method.
  }

  private void actionDiagnosticClient(ActionDiagnosticArgs diagnosticArgs)
      throws SliderException, IOException {
    try {
      String currentCommandPath = getCurrentCommandPath();
      SliderVersionInfo.loadAndPrintVersionInfo(log);
      String clientConfigPath = getClientConfigPath();
      String jdkInfo = getJDKInfo();
      println("The slider command path: %s", currentCommandPath);
      println("The slider-client.xml used by current running command path: %s",
          clientConfigPath);
      println(jdkInfo);

      // security info
      Configuration config = getConfig();
      if (isHadoopClusterSecure(config)) {
        println("Hadoop Cluster is secure");
        println("Login user is %s", UserGroupInformation.getLoginUser());
        println("Current user is %s", UserGroupInformation.getCurrentUser());

      } else {
        println("Hadoop Cluster is insecure");
      }

      // verbose?
      if (diagnosticArgs.verbose) {
        // do the environment
        Map<String, String> env = getSystemEnv();
        Set<String> envList = ConfigHelper.sortedConfigKeys(env.entrySet());
        StringBuilder builder = new StringBuilder("Environment variables:\n");
        for (String key : envList) {
          builder.append(key).append("=").append(env.get(key)).append("\n");
        }
        println(builder.toString());

        // Java properties
        builder = new StringBuilder("JVM Properties\n");
        Map<String, String> props =
            sortedMap(toMap(System.getProperties()));
        for (Entry<String, String> entry : props.entrySet()) {
          builder.append(entry.getKey()).append("=")
                 .append(entry.getValue()).append("\n");
        }
        
        println(builder.toString());

        // then the config
        println("Slider client configuration:\n" + ConfigHelper.dumpConfigToString(config));
      }

      validateSliderClientEnvironment(log);
    } catch (SliderException | IOException e) {
      log.error(e.toString());
      throw e;
    }

  }

  /**
   * Kerberos Diagnostics
   * @param args CLI arguments
   * @return exit code
   * @throws SliderException
   * @throws IOException
   */
  @SuppressWarnings("IOResourceOpenedButNotSafelyClosed")
  private int actionKDiag(ActionKDiagArgs args)
    throws Exception {
    PrintStream out;
    boolean closeStream = false;
    if (args.out != null) {
      out = new PrintStream(args.out, "UTF-8");
      closeStream = true;
    } else {
      out = System.err;
    }
    try {
      KerberosDiags kdiags = new KerberosDiags(getConfig(),
          out,
          args.services,
          args.keytab,
          args.principal,
          args.keylen,
          args.secure);
      kdiags.execute();
    } catch (KerberosDiags.KerberosDiagsFailure e) {
      log.error(e.toString());
      log.debug(e.toString(), e);
      throw e;
    } catch (Exception e) {
      log.error("Kerberos Diagnostics", e);
      throw e;
    } finally {
      if (closeStream) {
        out.flush();
        out.close();
      }
    }
    return 0;
  }

  /**
   * Log a service record instance
   * @param instance record
   * @param verbose verbose logging of all external endpoints
   */
  private void logInstance(ServiceRecord instance,
      boolean verbose) {
    if (!verbose) {
      log.info("{}", instance.get(YarnRegistryAttributes.YARN_ID, ""));
    } else {
      log.info("{}: ", instance.get(YarnRegistryAttributes.YARN_ID, ""));
      logEndpoints(instance);
    }
  }

  /**
   * Log the external endpoints of a service record
   * @param instance service record instance
   */
  private void logEndpoints(ServiceRecord instance) {
    List<Endpoint> endpoints = instance.external;
    for (Endpoint endpoint : endpoints) {
      log.info(endpoint.toString());
    }
  }

 /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListConfigsYarn(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {

    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    PublishedConfigSet configurations =
        retriever.getConfigurations(!registryArgs.internal);
    PrintStream out = null;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(registryArgs.out, "UTF-8");
      } else {
        out = System.out;
      }
      for (String configName : configurations.keys()) {
        if (!registryArgs.verbose) {
          out.println(configName);
        } else {
          PublishedConfiguration published = configurations.get(configName);
          out.printf("%s: %s%n", configName, published.description);
        }
      }
    } finally {
      if (registryArgs.out != null && out != null) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * list exports available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   */
  public void actionRegistryListExports(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    PublishedExportsSet exports =
        retriever.getExports(!registryArgs.internal);
    PrintStream out = null;
    boolean streaming = false;
    try {
      if (registryArgs.out != null) {
        out = new PrintStream(registryArgs.out, "UTF-8");
        streaming = true;
        log.debug("Saving output to {}", registryArgs.out);
      } else {
        out = System.out;
      }
      log.debug("Number of exports: {}", exports.keys().size());
      for (String exportName : exports.keys()) {
        if (streaming) {
          log.debug(exportName);
        }
        if (!registryArgs.verbose) {
          out.println(exportName);
        } else {
          PublishedExports published = exports.get(exportName);
          out.printf("%s: %s%n", exportName, published.description);
        }
      }
    } finally {
      if (streaming) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * list configs available for an instance
   *
   * @param registryArgs registry Arguments
   * @throws YarnException YARN problems
   * @throws IOException Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedConfiguration actionRegistryGetConfig(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    return ClientUtils.getConfigFromRegistry(getRegistryOperations(),
        getConfig(), registryArgs.getConf, registryArgs.name, registryArgs.user,
        !registryArgs.internal);
  }

  /**
   * get a specific export group
   *
   * @param registryArgs registry Arguments
   *
   * @throws YarnException         YARN problems
   * @throws IOException           Network or other problems
   * @throws FileNotFoundException if the config is not found
   */
  @VisibleForTesting
  public PublishedExports actionRegistryGetExport(ActionRegistryArgs registryArgs)
      throws YarnException, IOException {
    ServiceRecord instance = lookupServiceRecord(registryArgs);

    RegistryRetriever retriever = new RegistryRetriever(getConfig(), instance);
    boolean external = !registryArgs.internal;
    PublishedExportsSet exports = retriever.getExports(external);

    PublishedExports published = retriever.retrieveExports(exports,
                                                           registryArgs.getExport,
                                                           external);
    return published;
  }

  /**
   * write out the config. If a destination is provided and that dir is a
   * directory, the entry is written to it with the name provided + extension,
   * else it is printed to standard out.
   * @param published published config
   * @param registryArgs registry Arguments
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputConfig(PublishedConfiguration published,
      ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getConf;
    String format = registryArgs.format;
    String output = ClientUtils.saveOrReturnConfig(published,
        registryArgs.format, registryArgs.out, entry + "." + format);
    if (output != null) {
      print(output);
    }
  }

  /**
   * write out the config
   * @param published
   * @param registryArgs
   * @throws BadCommandArgumentsException
   * @throws IOException
   */
  private void outputExport(PublishedExports published,
                            ActionRegistryArgs registryArgs) throws
      BadCommandArgumentsException,
      IOException {
    // decide whether or not to print
    String entry = registryArgs.getExport;
    String format = ConfigFormat.JSON.toString();
    ConfigFormat configFormat = ConfigFormat.resolve(format);
    if (configFormat == null || configFormat != ConfigFormat.JSON) {
      throw new BadCommandArgumentsException(
          "Unknown/Unsupported format %s . Only JSON is supported.", format);
    }

    PublishedExportsOutputter outputter =
        PublishedExportsOutputter.createOutputter(configFormat,
                                                  published);
    boolean print = registryArgs.out == null;
    if (!print) {
      File destFile;
      destFile = registryArgs.out;
      if (destFile.isDirectory()) {
        // creating it under a directory
        destFile = new File(destFile, entry + "." + format);
      }
      log.info("Destination path: {}", destFile);
      outputter.save(destFile);
    } else {
      print(outputter.asString());
    }
  }

  /**
   * Look up an instance
   * @return instance data
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  private ServiceRecord lookupServiceRecord(ActionRegistryArgs registryArgs) throws
      SliderException,
      IOException {
    return ClientUtils.lookupServiceRecord(getRegistryOperations(),
        registryArgs.user, registryArgs.serviceType, registryArgs.name);
  }

  /**
   * 
   * Look up an instance
   * @param path path
   * @return instance data
   * @throws NotFoundException no path/no service record
   * at the end of the path
   * @throws SliderException other failures
   * @throws IOException IO problems or wrapped exceptions
   */
  public ServiceRecord resolve(String path)
      throws IOException, SliderException {
    return ClientUtils.resolve(getRegistryOperations(), path);
  }

  /**
   * List instances in the registry for the current user
   * @return a list of slider registry instances
   * @throws IOException Any IO problem ... including no path in the registry
   * to slider service classes for this user
   * @throws SliderException other failures
   */

  public Map<String, ServiceRecord> listRegistryInstances()
      throws IOException, SliderException {
    Map<String, ServiceRecord> recordMap = listServiceRecords(
        getRegistryOperations(),
        serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
    return recordMap;
  }
  
  /**
   * List instances in the registry
   * @return the instance IDs
   * @throws IOException
   * @throws YarnException
   */
  public List<String> listRegisteredSliderInstances() throws
      IOException,
      YarnException {
    try {
      Map<String, ServiceRecord> recordMap = listServiceRecords(
          getRegistryOperations(),
          serviceclassPath(currentUser(), SliderKeys.APP_TYPE));
      return new ArrayList<>(recordMap.keySet());
    } catch (PathNotFoundException e) {
      log.debug("No registry path for slider instances for current user: {}", e, e);
      // no entries: return an empty list
      return new ArrayList<>(0);
    } catch (IOException | YarnException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Start the registry if it is not there yet
   * @return the registry service
   * @throws SliderException
   * @throws IOException
   */
  private synchronized RegistryOperations maybeStartYarnRegistry()
      throws SliderException, IOException {

    if (registryOperations == null) {
      registryOperations = startRegistryOperationsService();
    }
    return registryOperations;
  }

  @Override
  public RegistryOperations getRegistryOperations()
      throws SliderException, IOException {
    return maybeStartYarnRegistry();
  }

  /**
   * Output to standard out/stderr (implementation specific detail)
   * @param src source
   */
  private static void print(CharSequence src) {
    clientOutputStream.print(src);
  }

  /**
   * Output to standard out/stderr with a newline after
   * @param message message
   */
  private static void println(String message) {
    clientOutputStream.println(message);
  }
  /**
   * Output to standard out/stderr with a newline after, formatted
   * @param message message
   * @param args arguments for string formatting
   */
  private static void println(String message, Object ... args) {
    clientOutputStream.println(String.format(message, args));
  }

  /**
   * Implement the lookup action.
   * @param args Action arguments
   * @return 0 if the entry was found
   * @throws IOException
   * @throws YarnException
   * @throws UnknownApplicationInstanceException if a specific instance
   * was named but it was not found
   */
  @VisibleForTesting
  public int actionLookup(ActionLookupArgs args)
      throws IOException, YarnException {
    try {
      ApplicationId id = ConverterUtils.toApplicationId(args.id);
      ApplicationReport report = yarnClient.getApplicationReport(id);
      SerializedApplicationReport sar = new SerializedApplicationReport(report);
      ApplicationReportSerDeser serDeser = new ApplicationReportSerDeser();
      if (args.outputFile != null) {
        serDeser.save(sar, args.outputFile);
      } else {
        println(serDeser.toJson(sar));
      }
    } catch (IllegalArgumentException e) {
      throw new BadCommandArgumentsException(e, "%s : %s", args, e);
    } catch (ApplicationAttemptNotFoundException | ApplicationNotFoundException notFound) {
      throw new NotFoundException(notFound, notFound.toString());
    }
    return EXIT_SUCCESS;
  }

  @Override
  public int actionDependency(ActionDependencyArgs args) throws IOException,
      YarnException {
    String currentUser = getUsername();
    log.info("Running command as user {}", currentUser);
    
    String version = getSliderVersion();
    Path dependencyLibTarGzip = sliderFileSystem.getDependencyTarGzip();
    
    // Check if dependency has already been uploaded, in which case log
    // appropriately and exit success (unless overwrite has been requested)
    if (sliderFileSystem.isFile(dependencyLibTarGzip) && !args.overwrite) {
      println(String.format(
          "Dependency libs are already uploaded to %s. Use %s "
              + "if you want to re-upload", dependencyLibTarGzip.toUri(),
          Arguments.ARG_OVERWRITE));
      return EXIT_SUCCESS;
    }
    
    String[] libDirs = SliderUtils.getLibDirs();
    if (libDirs.length > 0) {
      File tempLibTarGzipFile = File.createTempFile(
          SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_NAME + "_",
          SliderKeys.SLIDER_DEPENDENCY_TAR_GZ_FILE_EXT);
      // copy all jars
      tarGzipFolder(libDirs, tempLibTarGzipFile, createJarFilter());

      log.info("Uploading dependency for AM (version {}) from {} to {}",
          version, tempLibTarGzipFile.toURI(), dependencyLibTarGzip.toUri());
      sliderFileSystem.copyLocalFileToHdfs(tempLibTarGzipFile,
          dependencyLibTarGzip, new FsPermission(
              SliderKeys.SLIDER_DEPENDENCY_DIR_PERMISSIONS));
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  private int actionHelp(String actionName) throws YarnException, IOException {
    throw new UsageException(CommonArgs.usage(serviceArgs, actionName));
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   *
   * @param args argument list
   * @return a possibly empty list of nodes in the cluster
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  @Override
  public NodeInformationList listYarnClusterNodes(ActionNodesArgs args)
    throws YarnException, IOException {
    return yarnClient.listNodes(args.label, args.healthy);
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   *
   * @param args argument list
   * @return a possibly empty list of nodes in the cluster
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  public NodeInformationList listInstanceNodes(String instance, ActionNodesArgs args)
    throws YarnException, IOException {
    // TODO
    log.info("listInstanceNodes {}", instance);
    SliderClusterOperations clusterOps =
      new SliderClusterOperations(bondToCluster(instance));
    return clusterOps.getLiveNodes();
  }

  /**
   * List the nodes in the cluster, possibly filtering by node state or label.
   * Prints them to stdout unless the args names a file instead.
   * @param args argument list
   * @throws IOException IO problems
   * @throws YarnException YARN problems
   */
  public int actionNodes(String instance, ActionNodesArgs args) throws YarnException, IOException {

    args.instance = instance;
    NodeInformationList nodes;
    if (SliderUtils.isUnset(instance)) {
      nodes = listYarnClusterNodes(args);
    } else {
      nodes = listInstanceNodes(instance, args);
    }
    log.debug("Node listing for {} has {} nodes", args, nodes.size());
    JsonSerDeser<NodeInformationList> serDeser = NodeInformationList.createSerializer();
    if (args.outputFile != null) {
      serDeser.save(nodes, args.outputFile);
    } else {
      println(serDeser.toJson(nodes));
    }
    return 0;
  }

  /**
   * Save/list tokens. This is for testing oozie integration
   * @param args commands
   * @return status
   */
  private int actionTokens(ActionTokensArgs args)
      throws IOException, YarnException {
    return new TokensOperation().actionTokens(args,
        sliderFileSystem.getFileSystem(),
        getConfig(),
        yarnClient);
  }

  @VisibleForTesting
  public ApplicationReport monitorAppToRunning(Duration duration)
      throws YarnException, IOException {
    return yarnClient.monitorAppToState(applicationId, YarnApplicationState
        .RUNNING, duration);
  }
}


