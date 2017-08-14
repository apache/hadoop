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

package org.apache.hadoop.yarn.service.client;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.ComponentCountProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.FlexComponentsRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusRequestProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.GetStatusResponseProto;
import org.apache.hadoop.yarn.proto.ClientAMProtocol.StopRequestProto;
import org.apache.hadoop.yarn.service.ClientAMProtocol;
import org.apache.hadoop.yarn.service.ServiceMaster;
import org.apache.hadoop.yarn.service.client.params.ActionDependencyArgs;
import org.apache.hadoop.yarn.service.client.params.ActionFlexArgs;
import org.apache.hadoop.yarn.service.client.params.Arguments;
import org.apache.hadoop.yarn.service.client.params.ClientArgs;
import org.apache.hadoop.yarn.service.client.params.CommonArgs;
import org.apache.hadoop.yarn.service.conf.SliderExitCodes;
import org.apache.hadoop.yarn.service.conf.SliderKeys;
import org.apache.hadoop.yarn.service.conf.SliderXmlConfKeys;
import org.apache.hadoop.yarn.service.provider.AbstractClientProvider;
import org.apache.hadoop.yarn.service.provider.ProviderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.Times;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.common.params.AbstractClusterBuildingActionArgs;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UsageException;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.slider.core.launch.JavaCommandLineBuilder;
import org.apache.slider.core.registry.SliderRegistryUtils;
import org.apache.slider.core.zk.ZKIntegration;
import org.apache.slider.core.zk.ZookeeperUtils;
import org.apache.hadoop.yarn.service.utils.ServiceApiUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.*;
import static org.apache.hadoop.yarn.service.client.params.SliderActions.ACTION_CREATE;
import static org.apache.hadoop.yarn.service.client.params.SliderActions.ACTION_FLEX;
import static org.apache.slider.common.Constants.HADOOP_JAAS_DEBUG;
import static org.apache.slider.common.tools.SliderUtils.*;

public class ServiceClient extends CompositeService
    implements SliderExitCodes, SliderKeys {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceClient.class);
  private SliderFileSystem fs;
  private YarnClient yarnClient;
  // Avoid looking up applicationId from fs all the time.
  private Map<String, ApplicationId> cachedAppIds = new ConcurrentHashMap<>();
  private RegistryOperations registryClient;
  private CuratorFramework curatorClient;
  private YarnRPC rpc;

  private static EnumSet<YarnApplicationState> terminatedStates =
      EnumSet.of(FINISHED, FAILED, KILLED);
  private static EnumSet<YarnApplicationState> liveStates =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, RUNNING);

  public ServiceClient() {
    super(ServiceClient.class.getName());
  }

  @Override protected void serviceInit(Configuration configuration)
      throws Exception {
    fs = new SliderFileSystem(configuration);
    yarnClient = YarnClient.createYarnClient();
    rpc = YarnRPC.create(configuration);
    addService(yarnClient);
    super.serviceInit(configuration);
  }

  @Override
  protected void serviceStop() throws Exception {
    if (registryClient != null) {
      registryClient.stop();
    }
    super.serviceStop();
  }

  private Application loadAppJsonFromLocalFS(
      AbstractClusterBuildingActionArgs args) throws IOException {
    File file = args.getAppDef();
    Path filePath = new Path(file.getAbsolutePath());
    LOG.info("Loading app json from: " + filePath);
    Application application = ServiceApiUtil.jsonSerDeser
        .load(FileSystem.getLocal(getConfig()), filePath);
    if (args.lifetime > 0) {
      application.setLifetime(args.lifetime);
    }
    application.setName(args.getClusterName());
    return application;
  }

  public int actionBuild(AbstractClusterBuildingActionArgs args)
      throws IOException, YarnException {
    return actionBuild(loadAppJsonFromLocalFS(args));
  }

  public int actionBuild(Application application)
      throws YarnException, IOException {
    Path appDir = checkAppNotExistOnHdfs(application);
    ServiceApiUtil.validateAndResolveApplication(application, fs, getConfig());
    createDirAndPersistApp(appDir, application);
    return EXIT_SUCCESS;
  }

  public int actionCreate(AbstractClusterBuildingActionArgs args)
      throws IOException, YarnException {
    actionCreate(loadAppJsonFromLocalFS(args));
    return EXIT_SUCCESS;
  }

  public ApplicationId actionCreate(Application application)
      throws IOException, YarnException {
    String appName = application.getName();
    validateClusterName(appName);
    ServiceApiUtil.validateAndResolveApplication(application, fs, getConfig());
    verifyNoLiveAppInRM(appName, "create");
    Path appDir = checkAppNotExistOnHdfs(application);

    // Write the definition first and then submit - AM will read the definition
    createDirAndPersistApp(appDir, application);
    ApplicationId appId = submitApp(application);
    cachedAppIds.put(appName, appId);
    application.setId(appId.toString());
    // update app definition with appId
    persistAppDef(appDir, application);
    return appId;
  }

  // Called by ServiceCLI
  protected int actionFlexByCLI(ClientArgs args)
      throws YarnException, IOException {
    ActionFlexArgs flexArgs = args.getActionFlexArgs();
    Map<String, Long> componentCounts =
        new HashMap<>(flexArgs.getComponentMap().size());
    Application persistedApp =
        ServiceApiUtil.loadApplication(fs, flexArgs.getClusterName());
    if (!StringUtils.isEmpty(persistedApp.getId())) {
      cachedAppIds.put(persistedApp.getName(),
          ApplicationId.fromString(persistedApp.getId()));
    }
    for (Map.Entry<String, String> entry : flexArgs.getComponentMap()
        .entrySet()) {
      String compName = entry.getKey();
      ServiceApiUtil.validateCompName(compName);
      Component component = persistedApp.getComponent(compName);
      if (component == null) {
        throw new IllegalArgumentException(entry.getKey() + " does not exist !");
      }
      long numberOfContainers =
          parseNumberOfContainers(component, entry.getValue());
      componentCounts.put(compName, numberOfContainers);
    }
    // throw usage exception if no changes proposed
    if (componentCounts.size() == 0) {
      actionHelp(ACTION_FLEX, args);
    }
    flexComponents(args.getClusterName(), componentCounts, persistedApp);
    return EXIT_SUCCESS;
  }

  // Parse the number of containers requested by user, e.g.
  // +5 means add 5 additional containers
  // -5 means reduce 5 containers, if it goes to negative, sets it to 0
  // 5 means sets it to 5 containers.
  private long parseNumberOfContainers(Component component, String newNumber) {

    long orig = component.getNumberOfContainers();
    if (newNumber.startsWith("+")) {
      return orig + Long.parseLong(newNumber.substring(1));
    } else if (newNumber.startsWith("-")) {
      long ret = orig - Long.parseLong(newNumber.substring(1));
      if (ret < 0) {
        LOG.warn(MessageFormat.format(
            "[COMPONENT {}]: component count goes to negative ({}{} = {}), reset it to 0.",
            component.getName(), orig, newNumber, ret));
        ret = 0;
      }
      return ret;
    } else {
      return Long.parseLong(newNumber);
    }
  }

  // Called by Rest Service
  public Map<String, Long> flexByRestService(String appName,
      Map<String, Long> componentCounts) throws YarnException, IOException {
    // load app definition
    Application persistedApp = ServiceApiUtil.loadApplication(fs, appName);
    cachedAppIds.put(persistedApp.getName(),
        ApplicationId.fromString(persistedApp.getId()));
    return flexComponents(appName, componentCounts, persistedApp);
  }

  private Map<String, Long> flexComponents(String appName,
      Map<String, Long> componentCounts, Application persistedApp)
      throws YarnException, IOException {
    validateClusterName(appName);

    Map<String, Long> original = new HashMap<>(componentCounts.size());

    ComponentCountProto.Builder countBuilder = ComponentCountProto.newBuilder();
    FlexComponentsRequestProto.Builder requestBuilder =
        FlexComponentsRequestProto.newBuilder();

    for (Component persistedComp : persistedApp.getComponents()) {
      String name = persistedComp.getName();
      if (componentCounts.containsKey(persistedComp.getName())) {
        original.put(name, persistedComp.getNumberOfContainers());
        persistedComp.setNumberOfContainers(componentCounts.get(name));

        // build the request
        countBuilder.setName(persistedComp.getName())
            .setNumberOfContainers(persistedComp.getNumberOfContainers());
        requestBuilder.addComponents(countBuilder.build());
      }
    }
    if (original.size() < componentCounts.size()) {
      componentCounts.keySet().removeAll(original.keySet());
      throw new YarnException("Components " + componentCounts.keySet()
          + " do not exist in app definition.");
    }
    ServiceApiUtil.jsonSerDeser
        .save(fs.getFileSystem(), ServiceApiUtil.getAppJsonPath(fs, appName),
            persistedApp, true);
    ClientAMProtocol proxy = connectToAM(appName);
    proxy.flexComponents(requestBuilder.build());
    for (Map.Entry<String, Long> entry : original.entrySet()) {
      LOG.info("[COMPONENT {}]: number of containers changed from {} to {}",
          entry.getKey(), entry.getValue(),
          componentCounts.get(entry.getKey()));
    }
    return original;
  }

  public int actionStop(String appName) throws YarnException, IOException {
    validateClusterName(appName);
    getAppIdFromPersistedApp(appName);
    ApplicationId currentAppId = cachedAppIds.get(appName);
    ApplicationReport report = yarnClient.getApplicationReport(currentAppId);
    if (terminatedStates.contains(report.getYarnApplicationState())) {
      LOG.info("Application {} is already in a terminated state {}", appName,
          report.getYarnApplicationState());
      return EXIT_SUCCESS;
    }
    LOG.info("Stopping application {}, with appId = {}", appName, currentAppId);
    try {
      // try to stop the app gracefully.
      ClientAMProtocol proxy = connectToAM(appName);
      StopRequestProto request = StopRequestProto.newBuilder().build();
      proxy.stop(request);
      LOG.info("Application " + appName + " is being gracefully stopped...");

      // Wait until the app is killed.
      long startTime = System.currentTimeMillis();
      int pollCount = 0;
      while (true) {
        Thread.sleep(1000);
        report = yarnClient.getApplicationReport(currentAppId);
        if (terminatedStates.contains(report.getYarnApplicationState())) {
          LOG.info("Application " + appName + " is stopped.");
          break;
        }
        // Forcefully kill after 10 seconds.
        if ((System.currentTimeMillis() - startTime) > 10000) {
          LOG.info("Stop operation timeout stopping, forcefully kill the app "
              + appName);
          yarnClient.killApplication(currentAppId,
              "Forcefully kill the app by user");
          break;
        }
        if (++pollCount % 10 == 0) {
          LOG.info("Waiting for application " + appName + " to be stopped.");
        }
      }
    } catch (IOException | YarnException | InterruptedException e) {
      LOG.info("Failed to stop " + appName
          + " gracefully, forcefully kill the app.");
      yarnClient.killApplication(currentAppId, "Forcefully kill the app");
    }
    return EXIT_SUCCESS;
  }

  public int actionDestroy(String appName) throws Exception {
    validateClusterName(appName);
    verifyNoLiveAppInRM(appName, "Destroy");
    Path appDir = fs.buildClusterDirPath(appName);
    FileSystem fileSystem = fs.getFileSystem();
    // remove from the appId cache
    cachedAppIds.remove(appName);
    if (fileSystem.exists(appDir)) {
      if (fileSystem.delete(appDir, true)) {
        LOG.info("Successfully deleted application dir for " + appName + ": "
            + appDir);
      } else {
        String message =
            "Failed to delete application + " + appName + " at:  " + appDir;
        LOG.info(message);
        throw new YarnException(message);
      }
    }
    deleteZKNode(appName);
    String registryPath = SliderRegistryUtils.registryPathForInstance(appName);
    try {
      getRegistryClient().delete(registryPath, true);
    } catch (IOException e) {
      LOG.warn("Error deleting registry entry {}", registryPath, e);
    }
    LOG.info("Destroyed cluster {}", appName);
    return EXIT_SUCCESS;
  }

  private synchronized RegistryOperations getRegistryClient()
      throws SliderException, IOException {

    if (registryClient == null) {
      registryClient =
          RegistryOperationsFactory.createInstance("ServiceClient", getConfig());
      registryClient.init(getConfig());
      registryClient.start();
    }
    return registryClient;
  }

  private void deleteZKNode(String clusterName) throws Exception {
    CuratorFramework curatorFramework = getCuratorClient();
    String user = RegistryUtils.currentUser();
    String zkPath = ZKIntegration.mkClusterPath(user, clusterName);
    if (curatorFramework.checkExists().forPath(zkPath) != null) {
      curatorFramework.delete().deletingChildrenIfNeeded().forPath(zkPath);
      LOG.info("Deleted zookeeper path: " + zkPath);
    }
  }

  private synchronized CuratorFramework getCuratorClient()
      throws BadConfigException {
    String registryQuorum =
        getConfig().get(RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    // though if neither is set: trouble
    if (SliderUtils.isUnset(registryQuorum)) {
      throw new BadConfigException(
          "No Zookeeper quorum provided in the" + " configuration property "
              + RegistryConstants.KEY_REGISTRY_ZK_QUORUM);
    }
    ZookeeperUtils.splitToHostsAndPortsStrictly(registryQuorum);

    if (curatorClient == null) {
      curatorClient =
          CuratorFrameworkFactory.builder().connectString(registryQuorum)
              .sessionTimeoutMs(10000).retryPolicy(new RetryNTimes(10, 2000))
              .build();
      curatorClient.start();
    }
    return curatorClient;
  }

  private int actionHelp(String actionName, CommonArgs args)
      throws YarnException, IOException {
    throw new UsageException(CommonArgs.usage(args, actionName));
  }

  private void verifyNoLiveAppInRM(String appname, String action)
      throws IOException, YarnException {
    Set<String> types = new HashSet<>(1);
    types.add(SliderKeys.APP_TYPE);
    Set<String> tags = null;
    if (appname != null) {
      tags = Collections.singleton(SliderUtils.createNameTag(appname));
    }
    GetApplicationsRequest request = GetApplicationsRequest.newInstance();
    request.setApplicationTypes(types);
    request.setApplicationTags(tags);
    request.setApplicationStates(liveStates);
    List<ApplicationReport> reports = yarnClient.getApplications(request);
    if (!reports.isEmpty()) {
      throw new YarnException(
          "Failed to " + action + " application, as " + appname
              + " already exists.");
    }
  }

  private ApplicationId submitApp(Application app)
      throws IOException, YarnException {
    String appName = app.getName();
    Configuration conf = getConfig();
    Path appRootDir = fs.buildClusterDirPath(app.getName());

    YarnClientApplication yarnApp = yarnClient.createApplication();
    ApplicationSubmissionContext submissionContext =
        yarnApp.getApplicationSubmissionContext();
    ServiceApiUtil.validateCompResourceSize(
        yarnApp.getNewApplicationResponse().getMaximumResourceCapability(),
        app);

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
    addKeytabResourceIfSecure(fs, localResources, conf, appName);
    if (LOG.isDebugEnabled()) {
      printLocalResources(localResources);
    }
    Map<String, String> env = addAMEnv(conf);

    // create AM CLI
    String cmdStr =
        buildCommandLine(appName, conf, appRootDir, hasSliderAMLog4j);

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
    submissionContext.setAMContainerSpec(amLaunchContext);
    yarnClient.submitApplication(submissionContext);
    return submissionContext.getApplicationId();
  }

  private void printLocalResources(Map<String, LocalResource> map) {
    LOG.debug("Added LocalResource for localization: ");
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, LocalResource> entry : map.entrySet()) {
      builder.append(entry.getKey()).append(" -> ")
          .append(entry.getValue().getResource().getFile())
          .append(System.lineSeparator());
    }
    LOG.debug(builder.toString());
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
    CLI.add(ServiceMaster.class.getCanonicalName());
    CLI.add(ACTION_CREATE, appName);
    //TODO debugAM CLI.add(Arguments.ARG_DEBUG)
    CLI.add(Arguments.ARG_CLUSTER_URI, new Path(appRootDir, appName + ".json"));
    // pass the registry binding
    CLI.addConfOptionToCLI(conf, RegistryConstants.KEY_REGISTRY_ZK_ROOT,
        RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    CLI.addMandatoryConfOption(conf, RegistryConstants.KEY_REGISTRY_ZK_QUORUM);

    // write out the path output
    CLI.addOutAndErrFiles(STDOUT_AM, STDERR_AM);
    String cmdStr = CLI.build();
    LOG.info("AM launch command: {}", cmdStr);
    return cmdStr;
  }

  private Map<String, String> addAMEnv(Configuration conf) throws IOException {
    Map<String, String> env = new HashMap<>();
    ClasspathConstructor classpath =
        buildClasspath(SliderKeys.SUBMITTED_CONF_DIR, "lib", fs, getConfig()
            .getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false));
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
      LOG.info("Run as user " + userName);
      // HADOOP_USER_NAME env is used by UserGroupInformation when log in
      // This env makes AM run as this user
      env.put("HADOOP_USER_NAME", userName);
    }
    LOG.info("AM env: \n{}", stringifyMap(env));
    return env;
  }

  protected Path addJarResource(String appName,
      Map<String, LocalResource> localResources)
      throws IOException, SliderException {
    Path libPath = fs.buildClusterDirPath(appName);
    ProviderUtils
        .addProviderJar(localResources, ServiceMaster.class, SLIDER_JAR, fs,
            libPath, "lib", false);
    Path dependencyLibTarGzip = fs.getDependencyTarGzip();
    if (fs.isFile(dependencyLibTarGzip)) {
      LOG.info("Loading lib tar from " + fs.getFileSystem().getScheme() + ": "
          + dependencyLibTarGzip);
      SliderUtils.putAmTarGzipAndUpdate(localResources, fs);
    } else {
      String[] libs = SliderUtils.getLibDirs();
      for (String libDirProp : libs) {
        ProviderUtils.addAllDependencyJars(localResources, fs, libPath, "lib",
            libDirProp);
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
        Path appDirPath = fs.buildClusterDirPath(appName);
        Path remoteConfPath =
            new Path(appDirPath, SliderKeys.SUBMITTED_CONF_DIR);
        Path remoteFilePath =
            new Path(remoteConfPath, SliderKeys.LOG4J_SERVER_PROP_FILENAME);
        copy(conf, localFilePath, remoteFilePath);
        LocalResource localResource =
            fs.createAmResource(remoteConfPath, LocalResourceType.FILE);
        localResources.put(localFilePath.getName(), localResource);
        hasSliderAMLog4j = true;
      }
    }
    return hasSliderAMLog4j;
  }

  public int actionStart(String appName) throws YarnException, IOException {
    validateClusterName(appName);
    Path appDir = checkAppExistOnHdfs(appName);
    Application application = ServiceApiUtil.loadApplication(fs, appName);
    ServiceApiUtil.validateAndResolveApplication(application, fs, getConfig());
    // see if it is actually running and bail out;
    verifyNoLiveAppInRM(appName, "thaw");
    ApplicationId appId = submitApp(application);
    application.setId(appId.toString());
    // write app definition on to hdfs
    createDirAndPersistApp(appDir, application);
    return 0;
  }

  private Path checkAppNotExistOnHdfs(Application application)
      throws IOException, SliderException {
    Path appDir = fs.buildClusterDirPath(application.getName());
    fs.verifyDirectoryNonexistent(
        new Path(appDir, application.getName() + ".json"));
    return appDir;
  }

  private Path checkAppExistOnHdfs(String appName)
      throws IOException, SliderException {
    Path appDir = fs.buildClusterDirPath(appName);
    fs.verifyPathExists(new Path(appDir, appName + ".json"));
    return appDir;
  }

  private void createDirAndPersistApp(Path appDir, Application application)
      throws IOException, SliderException {
    FsPermission appDirPermission = new FsPermission("750");
    fs.createWithPermissions(appDir, appDirPermission);
    persistAppDef(appDir, application);
  }

  private void persistAppDef(Path appDir, Application application)
      throws IOException {
    Path appJson = new Path(appDir, application.getName() + ".json");
    ServiceApiUtil.jsonSerDeser
        .save(fs.getFileSystem(), appJson, application, true);
    LOG.info(
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
        LOG.info("Adding AM keytab on hdfs: " + keytabPath);
      } else {
        LOG.warn("No keytab file was found at {}.", keytabPath);
        if (conf.getBoolean(KEY_AM_LOGIN_KEYTAB_REQUIRED, false)) {
          throw new BadConfigException("No keytab file was found at %s.",
              keytabPath);
        } else {
          LOG.warn("The AM will be "
              + "started without a kerberos authenticated identity. "
              + "The application is therefore not guaranteed to remain "
              + "operational beyond 24 hours.");
        }
      }
    }
  }

  public String updateLifetime(String appName, long lifetime)
      throws YarnException, IOException {
    getAppIdFromPersistedApp(appName);
    ApplicationId currentAppId = cachedAppIds.get(appName);
    ApplicationReport report = yarnClient.getApplicationReport(currentAppId);
    if (report == null) {
      throw new YarnException("Application not found for " + appName);
    }
    ApplicationId appId = report.getApplicationId();
    LOG.info("Updating lifetime of an application: appName = " + appName
        + ", appId = " + appId + ", lifetime = " + lifetime);
    Map<ApplicationTimeoutType, String> map = new HashMap<>();
    String newTimeout =
        Times.formatISO8601(System.currentTimeMillis() + lifetime * 1000);
    map.put(ApplicationTimeoutType.LIFETIME, newTimeout);
    UpdateApplicationTimeoutsRequest request =
        UpdateApplicationTimeoutsRequest.newInstance(appId, map);
    yarnClient.updateApplicationTimeouts(request);
    LOG.info(
        "Successfully updated lifetime for an application: appName = " + appName
            + ", appId = " + appId + ". New expiry time in ISO8601 format is "
            + newTimeout);
    return newTimeout;
  }

  public Application getStatus(String appName)
      throws IOException, YarnException {
    ClientAMProtocol proxy = connectToAM(appName);
    GetStatusResponseProto response =
        proxy.getStatus(GetStatusRequestProto.newBuilder().build());
    return ServiceApiUtil.jsonSerDeser.fromJson(response.getStatus());

  }

  public YarnClient getYarnClient() {
    return this.yarnClient;
  }

  public int actionDependency(ActionDependencyArgs args)
      throws IOException, YarnException {
    String currentUser = RegistryUtils.currentUser();
    LOG.info("Running command as user {}", currentUser);

    Path dependencyLibTarGzip = fs.getDependencyTarGzip();

    // Check if dependency has already been uploaded, in which case log
    // appropriately and exit success (unless overwrite has been requested)
    if (fs.isFile(dependencyLibTarGzip) && !args.overwrite) {
      System.out.println(String.format(
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

      LOG.info("Uploading dependency for AM (version {}) from {} to {}",
          VersionInfo.getBuildVersion(), tempLibTarGzipFile.toURI(),
          dependencyLibTarGzip.toUri());
      fs.copyLocalFileToHdfs(tempLibTarGzipFile, dependencyLibTarGzip,
          new FsPermission(SliderKeys.SLIDER_DEPENDENCY_DIR_PERMISSIONS));
      return EXIT_SUCCESS;
    } else {
      return EXIT_FALSE;
    }
  }

  protected ClientAMProtocol connectToAM(String appName)
      throws IOException, YarnException {
    ApplicationId currentAppId = getAppIdFromPersistedApp(appName);
    // Wait until app becomes running.
    long startTime = System.currentTimeMillis();
    int pollCount = 0;
    ApplicationReport appReport = null;
    while (true) {
      appReport = yarnClient.getApplicationReport(currentAppId);
      YarnApplicationState state = appReport.getYarnApplicationState();
      if (state == RUNNING) {
        break;
      }
      if (terminatedStates.contains(state)) {
        throw new YarnException(
            "Failed to getStatus " + currentAppId + ": " + appReport
                .getDiagnostics());
      }
      long elapsedMillis = System.currentTimeMillis() - startTime;
      // if over 5 min, quit
      if (elapsedMillis >= 300000) {
        throw new YarnException(
            "Timed out while waiting for application " + currentAppId
                + " to be running");
      }

      if (++pollCount % 10 == 0) {
        LOG.info(
            "Waiting for application {} to be running, current state is {}",
            currentAppId, state);
      }
      try {
        Thread.sleep(3000);
      } catch (InterruptedException ie) {
        String msg =
            "Interrupted while waiting for application " + currentAppId
                + " to be running.";
        throw new YarnException(msg, ie);
      }
    }

    // Make the connection
    InetSocketAddress address = NetUtils
        .createSocketAddrForHost(appReport.getHost(), appReport.getRpcPort());
    return ClientAMProxy.createProxy(getConfig(), ClientAMProtocol.class,
        UserGroupInformation.getCurrentUser(), rpc, address);
  }

  private synchronized ApplicationId getAppIdFromPersistedApp(String appName)
      throws IOException, YarnException {
    if (cachedAppIds.containsKey(appName)) {
      return cachedAppIds.get(appName);
    }
    Application persistedApp = ServiceApiUtil.loadApplication(fs, appName);
    if (persistedApp == null) {
      throw new YarnException("Application " + appName
          + " doesn't exist on hdfs. Please check if the app exists in RM");
    }
    ApplicationId currentAppId = ApplicationId.fromString(persistedApp.getId());
    cachedAppIds.put(appName, currentAppId);
    return currentAppId;
  }
}
