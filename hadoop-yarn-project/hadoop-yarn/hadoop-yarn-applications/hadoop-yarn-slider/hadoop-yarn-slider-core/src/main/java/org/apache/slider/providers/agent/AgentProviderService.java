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

package org.apache.slider.providers.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.ConfigUtils;
import org.apache.slider.core.registry.docstore.ExportEntry;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.MonitorDetail;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.providers.agent.application.metadata.AbstractComponent;
import org.apache.slider.providers.agent.application.metadata.Application;
import org.apache.slider.providers.agent.application.metadata.CommandOrder;
import org.apache.slider.providers.agent.application.metadata.CommandScript;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.apache.slider.providers.agent.application.metadata.ComponentCommand;
import org.apache.slider.providers.agent.application.metadata.ComponentExport;
import org.apache.slider.providers.agent.application.metadata.ComponentsInAddonPackage;
import org.apache.slider.providers.agent.application.metadata.ConfigFile;
import org.apache.slider.providers.agent.application.metadata.DefaultConfig;
import org.apache.slider.providers.agent.application.metadata.DockerContainer;
import org.apache.slider.providers.agent.application.metadata.Export;
import org.apache.slider.providers.agent.application.metadata.ExportGroup;
import org.apache.slider.providers.agent.application.metadata.Metainfo;
import org.apache.slider.providers.agent.application.metadata.OSPackage;
import org.apache.slider.providers.agent.application.metadata.OSSpecific;
import org.apache.slider.providers.agent.application.metadata.Package;
import org.apache.slider.providers.agent.application.metadata.PropertyInfo;
import org.apache.slider.server.appmaster.actions.ProviderReportedContainerLoss;
import org.apache.slider.server.appmaster.actions.RegisterComponentInstance;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.agent.AgentCommandType;
import org.apache.slider.server.appmaster.web.rest.agent.AgentRestOperations;
import org.apache.slider.server.appmaster.web.rest.agent.CommandReport;
import org.apache.slider.server.appmaster.web.rest.agent.ComponentStatus;
import org.apache.slider.server.appmaster.web.rest.agent.ExecutionCommand;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeat;
import org.apache.slider.server.appmaster.web.rest.agent.HeartBeatResponse;
import org.apache.slider.server.appmaster.web.rest.agent.Register;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse;
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus;
import org.apache.slider.server.appmaster.web.rest.agent.StatusCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.apache.slider.api.RoleKeys.ROLE_PREFIX;
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_AGENTS;

/**
 * This class implements the server-side logic for application deployment through Slider application package
 */
public class AgentProviderService extends AbstractProviderService implements
    ProviderCore,
    AgentKeys,
    SliderKeys, AgentRestOperations {


  protected static final Logger log =
      LoggerFactory.getLogger(AgentProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static final String LABEL_MAKER = "___";
  private static final String CONTAINER_ID = "container_id";
  private static final String GLOBAL_CONFIG_TAG = "global";
  private static final String COMPONENT_TAG = "component";
  private static final String APPLICATION_TAG = "application";
  private static final String COMPONENT_DATA_TAG = "ComponentInstanceData";
  private static final String SHARED_PORT_TAG = "SHARED";
  private static final String PER_CONTAINER_TAG = "{PER_CONTAINER}";
  private static final int MAX_LOG_ENTRIES = 40;
  private static final int DEFAULT_HEARTBEAT_MONITOR_INTERVAL = 60 * 1000;

  private final Object syncLock = new Object();
  private final ComponentTagProvider tags = new ComponentTagProvider();
  private int heartbeatMonitorInterval = 0;
  private AgentClientProvider clientProvider;
  private AtomicInteger taskId = new AtomicInteger(0);
  private volatile Map<String, MetainfoHolder> metaInfoMap = new HashMap<>();
  private SliderFileSystem fileSystem = null;
  private Map<String, DefaultConfig> defaultConfigs = null;
  private ComponentCommandOrder commandOrder = new ComponentCommandOrder();
  private HeartbeatMonitor monitor;
  private Boolean canAnyMasterPublish = null;
  private AgentLaunchParameter agentLaunchParameter = null;
  private String clusterName = null;
  private boolean isInUpgradeMode;
  private Set<String> upgradeContainers = new HashSet<String>();
  private boolean appStopInitiated;

  private final Map<String, ComponentInstanceState> componentStatuses =
      new ConcurrentHashMap<String, ComponentInstanceState>();
  private final Map<String, Map<String, String>> componentInstanceData =
      new ConcurrentHashMap<String, Map<String, String>>();
  private final Map<String, Map<String, List<ExportEntry>>> exportGroups =
      new ConcurrentHashMap<String, Map<String, List<ExportEntry>>>();
  private final Map<String, Map<String, String>> allocatedPorts =
      new ConcurrentHashMap<String, Map<String, String>>();
  private final Map<String, Metainfo> packageMetainfo = 
      new ConcurrentHashMap<String, Metainfo>();

  private final Map<String, ExportEntry> logFolderExports =
      Collections.synchronizedMap(new LinkedHashMap<String, ExportEntry>(MAX_LOG_ENTRIES, 0.75f, false) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > MAX_LOG_ENTRIES;
        }
      });
  private final Map<String, ExportEntry> workFolderExports =
      Collections.synchronizedMap(new LinkedHashMap<String, ExportEntry>(MAX_LOG_ENTRIES, 0.75f, false) {
        protected boolean removeEldestEntry(Map.Entry eldest) {
          return size() > MAX_LOG_ENTRIES;
        }
      });
  private final Map<String, Set<String>> containerExportsMap =
      new HashMap<String, Set<String>>();

  private static class MetainfoHolder {
    Metainfo metaInfo;
    private Map<String, DefaultConfig> defaultConfigs = null;

    public MetainfoHolder(Metainfo metaInfo,
        Map<String, DefaultConfig> defaultConfigs) {
      this.metaInfo = metaInfo;
      this.defaultConfigs = defaultConfigs;
    }
  }

  /**
   * Create an instance of AgentProviderService
   */
  public AgentProviderService() {
    super("AgentProviderService");
    setAgentRestOperations(this);
    setHeartbeatMonitorInterval(DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
  }

  @Override
  public String getHumanName() {
    return "Slider Agent";
  }

  @Override
  public List<ProviderRole> getRoles() {
    return AgentRoles.getRoles();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    clientProvider = new AgentClientProvider(conf);
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition)
      throws
      SliderException {
    clientProvider.validateInstanceDefinition(instanceDefinition, null);

    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();

    Set<String> names = resources.getComponentNames();
    names.remove(COMPONENT_AM);
    for (String name : names) {
      Component componentDef = getApplicationComponent(name);
      if (componentDef == null) {
        // component member is validated elsewhere, so we don't need to throw
        // an exception here
        continue;
      }

      MapOperations componentConfig = resources.getMandatoryComponent(name);
      int count =
          componentConfig.getMandatoryOptionInt(ResourceKeys.COMPONENT_INSTANCES);
      int definedMinCount = componentDef.getMinInstanceCountInt();
      int definedMaxCount = componentDef.getMaxInstanceCountInt();
      if (count < definedMinCount || count > definedMaxCount) {
        throw new BadConfigException("Component %s, %s value %d out of range. "
                                     + "Expected minimum is %d and maximum is %d",
                                     name,
                                     ResourceKeys.COMPONENT_INSTANCES,
                                     count,
                                     definedMinCount,
                                     definedMaxCount);
      }
    }
  }

  // Reads the metainfo.xml in the application package and loads it
  private void buildMetainfo(AggregateConf instanceDefinition,
                             SliderFileSystem fileSystem,
                             String roleGroup)
      throws IOException, SliderException {
    String mapKey = instanceDefinition.getAppConfOperations()
        .getComponentOpt(roleGroup, ROLE_PREFIX, DEFAULT_METAINFO_MAP_KEY);
    String appDef = SliderUtils.getApplicationDefinitionPath(
        instanceDefinition.getAppConfOperations(), roleGroup);
    MapOperations component = null;
    if (roleGroup != null) {
      component = instanceDefinition.getAppConfOperations().getComponent(roleGroup);
    }

    MetainfoHolder metaInfoHolder = metaInfoMap.get(mapKey);
    if (metaInfoHolder == null) {
      synchronized (syncLock) {
        if (this.fileSystem == null) {
          this.fileSystem = fileSystem;
        }
        metaInfoHolder = metaInfoMap.get(mapKey);
        if (metaInfoHolder == null) {
          readAndSetHeartbeatMonitoringInterval(instanceDefinition);
          initializeAgentDebugCommands(instanceDefinition);

          Metainfo metaInfo = getApplicationMetainfo(fileSystem, appDef, false);
          log.info("Master package metainfo: {}", metaInfo.toString());
          if (metaInfo == null || metaInfo.getApplication() == null) {
            log.error("metainfo.xml is unavailable or malformed at {}.", appDef);
            throw new SliderException(
                "metainfo.xml is required in app package.");
          }
          List<CommandOrder> commandOrders = metaInfo.getApplication()
              .getCommandOrders();
          if (!DEFAULT_METAINFO_MAP_KEY.equals(mapKey)) {
            for (Component comp : metaInfo.getApplication().getComponents()) {
              comp.setName(mapKey + comp.getName());
              log.info("Modifying external metainfo component name to {}",
                  comp.getName());
            }
            for (CommandOrder co : commandOrders) {
              log.info("Adding prefix {} to command order {}",
                  mapKey, co);
              co.setCommand(mapKey + co.getCommand());
              co.setRequires(mapKey + co.getRequires());
            }
          }
          log.debug("Merging command orders {} for {}", commandOrders,
              roleGroup);
          commandOrder.mergeCommandOrders(commandOrders,
              instanceDefinition.getResourceOperations());
          Map<String, DefaultConfig> defaultConfigs =
              initializeDefaultConfigs(fileSystem, appDef, metaInfo);
          metaInfoMap.put(mapKey, new MetainfoHolder(metaInfo, defaultConfigs));
          monitor = new HeartbeatMonitor(this, getHeartbeatMonitorInterval());
          monitor.start();

          // build a map from component to metainfo
          String addonAppDefString = instanceDefinition.getAppConfOperations()
              .getGlobalOptions().getOption(ADDONS, null);
          if (component != null) {
            addonAppDefString = component.getOption(ADDONS, addonAppDefString);
          }
          log.debug("All addon appdefs: {}", addonAppDefString);
          if (addonAppDefString != null) {
            Scanner scanner = new Scanner(addonAppDefString).useDelimiter(",");
            while (scanner.hasNext()) {
              String addonAppDef = scanner.next();
              String addonAppDefPath = instanceDefinition
                  .getAppConfOperations().getGlobalOptions().get(addonAppDef);
              if (component != null) {
                addonAppDefPath = component.getOption(addonAppDef, addonAppDefPath);
              }
              log.debug("Addon package {} is stored at: {}", addonAppDef
                  + addonAppDefPath);
              Metainfo addonMetaInfo = getApplicationMetainfo(fileSystem,
                  addonAppDefPath, true);
              addonMetaInfo.validate();
              packageMetainfo.put(addonMetaInfo.getApplicationPackage()
                  .getName(), addonMetaInfo);
            }
            log.info("Metainfo map for master and addon: {}",
                packageMetainfo.toString());
          }
        }
      }
    }
  }

  @Override
  public void initializeApplicationConfiguration(
      AggregateConf instanceDefinition, SliderFileSystem fileSystem,
      String roleGroup)
      throws IOException, SliderException {
    buildMetainfo(instanceDefinition, fileSystem, roleGroup);
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
                                          AggregateConf instanceDefinition,
                                          Container container,
                                          ProviderRole providerRole,
                                          SliderFileSystem fileSystem,
                                          Path generatedConfPath,
                                          MapOperations resourceComponent,
                                          MapOperations appComponent,
                                          Path containerTmpDirPath) throws
      IOException,
      SliderException {
    
    String roleName = providerRole.name;
    String roleGroup = providerRole.group;
    String appDef = SliderUtils.getApplicationDefinitionPath(instanceDefinition
        .getAppConfOperations(), roleGroup);

    initializeApplicationConfiguration(instanceDefinition, fileSystem, roleGroup);

    log.info("Build launch context for Agent");
    log.debug(instanceDefinition.toString());
    
    //if we are launching docker based app on yarn, then we need to pass docker image
    if (isYarnDockerContainer(roleGroup)) {
      launcher.setYarnDockerMode(true);
      launcher.setDockerImage(getConfigFromMetaInfo(roleGroup, "image"));
      launcher.setDockerNetwork(getConfigFromMetaInfo(roleGroup, "network"));
      launcher.setRunPrivilegedContainer(getConfigFromMetaInfo(roleGroup, "runPriviledgedContainer"));
      launcher
          .setYarnContainerMountPoints(getConfigFromMetaInfoWithAppConfigOverriding(
              roleGroup, "yarn.container.mount.points"));
    }

    // Set the environment
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent,
        providerUtils.getStandardTokenMap(getAmState().getAppConfSnapshot(),
            getAmState().getInternalsSnapshot(), roleName, roleGroup,
            getClusterName())));

    String workDir = ApplicationConstants.Environment.PWD.$();
    launcher.setEnv("AGENT_WORK_ROOT", workDir);
    log.info("AGENT_WORK_ROOT set to {}", workDir);
    String logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    launcher.setEnv("AGENT_LOG_ROOT", logDir);
    log.info("AGENT_LOG_ROOT set to {}", logDir);
    if (System.getenv(HADOOP_USER_NAME) != null) {
      launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));
    }
    // for 2-Way SSL
    launcher.setEnv(SLIDER_PASSPHRASE, instanceDefinition.getPassphrase());
    //add english env
    launcher.setEnv("LANG", "en_US.UTF-8");
    launcher.setEnv("LC_ALL", "en_US.UTF-8");
    launcher.setEnv("LANGUAGE", "en_US.UTF-8");

    //local resources

    // TODO: Should agent need to support App Home
    String scriptPath = new File(AGENT_MAIN_SCRIPT_ROOT, AGENT_MAIN_SCRIPT).getPath();
    String appHome = instanceDefinition.getAppConfOperations().
        getGlobalOptions().get(PACKAGE_PATH);
    if (SliderUtils.isSet(appHome)) {
      scriptPath = new File(appHome, AGENT_MAIN_SCRIPT).getPath();
    }

    // set PYTHONPATH
    List<String> pythonPaths = new ArrayList<String>();
    pythonPaths.add(AGENT_MAIN_SCRIPT_ROOT);
    pythonPaths.add(AGENT_JINJA2_ROOT);
    String pythonPath = StringUtils.join(File.pathSeparator, pythonPaths);
    launcher.setEnv(PYTHONPATH, pythonPath);
    log.info("PYTHONPATH set to {}", pythonPath);

    Path agentImagePath = null;
    String agentImage = instanceDefinition.getInternalOperations().
        get(InternalKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    if (SliderUtils.isUnset(agentImage)) {
      agentImagePath =
          new Path(new Path(new Path(instanceDefinition.getInternalOperations().get(InternalKeys.INTERNAL_TMP_DIR),
                                     container.getId().getApplicationAttemptId().getApplicationId().toString()),
                            PROVIDER_AGENT),
                   AGENT_TAR);
    } else {
       agentImagePath = new Path(agentImage);
    }

    if (fileSystem.getFileSystem().exists(agentImagePath)) {
      LocalResource agentImageRes = fileSystem.createAmResource(agentImagePath, LocalResourceType.ARCHIVE);
      launcher.addLocalResource(AGENT_INSTALL_DIR, agentImageRes);
    } else {
      String msg =
          String.format("Required agent image slider-agent.tar.gz is unavailable at %s", agentImagePath.toString());
      MapOperations compOps = appComponent;
      boolean relaxVerificationForTest = compOps != null ? Boolean.valueOf(compOps.
          getOptionBool(TEST_RELAX_VERIFICATION, false)) : false;
      log.error(msg);

      if (!relaxVerificationForTest) {
        throw new SliderException(SliderExitCodes.EXIT_DEPLOYMENT_FAILED, msg);
      }
    }

    log.info("Using {} for agent.", scriptPath);
    LocalResource appDefRes = fileSystem.createAmResource(
        fileSystem.getFileSystem().resolvePath(new Path(appDef)),
        LocalResourceType.ARCHIVE);
    launcher.addLocalResource(APP_DEFINITION_DIR, appDefRes);

    for (Package pkg : getMetaInfo(roleGroup).getApplication().getPackages()) {
      Path pkgPath = fileSystem.buildResourcePath(pkg.getName());
      if (!fileSystem.isFile(pkgPath)) {
        pkgPath = fileSystem.buildResourcePath(getClusterName(),
            pkg.getName());
      }
      if (!fileSystem.isFile(pkgPath)) {
        throw new IOException("Package doesn't exist as a resource: " +
            pkg.getName());
      }
      log.info("Adding resource {}", pkg.getName());
      LocalResourceType type = LocalResourceType.FILE;
      if ("archive".equals(pkg.getType())) {
        type = LocalResourceType.ARCHIVE;
      }
      LocalResource packageResource = fileSystem.createAmResource(
          pkgPath, type);
      launcher.addLocalResource(APP_PACKAGES_DIR, packageResource);
    }

    String agentConf = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AGENT_CONF, "");
    if (SliderUtils.isSet(agentConf)) {
      LocalResource agentConfRes = fileSystem.createAmResource(fileSystem
                                                                   .getFileSystem().resolvePath(new Path(agentConf)),
                                                               LocalResourceType.FILE);
      launcher.addLocalResource(AGENT_CONFIG_FILE, agentConfRes);
    }

    String agentVer = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AGENT_VERSION, null);
    if (agentVer != null) {
      LocalResource agentVerRes = fileSystem.createAmResource(
          fileSystem.getFileSystem().resolvePath(new Path(agentVer)),
          LocalResourceType.FILE);
      launcher.addLocalResource(AGENT_VERSION_FILE, agentVerRes);
    }

    if (SliderUtils.isHadoopClusterSecure(getConfig())) {
      providerUtils.localizeServiceKeytabs(launcher, instanceDefinition,
          fileSystem, getClusterName());
    }

    MapOperations amComponent = instanceDefinition.
        getAppConfOperations().getComponent(COMPONENT_AM);
    if (providerUtils.hasTwoWaySSLEnabled(amComponent)) {
      providerUtils.localizeContainerSSLResources(launcher, container,
          fileSystem, getClusterName());
    }

    if (providerUtils.areStoresRequested(appComponent)) {
      providerUtils.localizeContainerSecurityStores(launcher, container,
          roleName, fileSystem, instanceDefinition, appComponent,
          getClusterName());
    }

    //add the configuration resources
    launcher.addLocalResources(fileSystem.submitDirectory(
        generatedConfPath,
        PROPAGATED_CONF_DIR_NAME));

    if (appComponent.getOptionBool(AM_CONFIG_GENERATION, false)) {
      // build and localize configuration files
      Map<String, Map<String, String>> configurations =
          buildCommandConfigurations(instanceDefinition.getAppConfOperations(),
              instanceDefinition.getInternalOperations(),
              container.getId().toString(), roleName, roleGroup);
      for (ConfigFile configFile : getMetaInfo(roleGroup)
          .getComponentConfigFiles(roleGroup)) {
        localizeConfigFile(launcher, roleName, roleGroup, configFile,
            configurations, launcher.getEnv(), fileSystem);
      }
    }

    String label = getContainerLabel(container, roleName, roleGroup);
    CommandLineBuilder operation = new CommandLineBuilder();

    String pythonExec = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().getOption(SliderXmlConfKeys.PYTHON_EXECUTABLE_PATH,
                                      PYTHON_EXE);

    operation.add(pythonExec);

    operation.add(scriptPath);
    operation.add(ARG_LABEL, label);
    operation.add(ARG_ZOOKEEPER_QUORUM);
    operation.add(getClusterOptionPropertyValue(OptionKeys.ZOOKEEPER_QUORUM));
    operation.add(ARG_ZOOKEEPER_REGISTRY_PATH);
    operation.add(getZkRegistryPath());

    String debugCmd = agentLaunchParameter.getNextLaunchParameter(roleGroup);
    if (SliderUtils.isSet(debugCmd)) {
      operation.add(ARG_DEBUG);
      operation.add(debugCmd);
    }

    operation.add("> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + AGENT_OUT_FILE + " 2>&1");

    launcher.addCommand(operation.build());

    // localize addon package
    String addonAppDefString = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().getOption(ADDONS, null);
    log.debug("All addon appdefs: {}", addonAppDefString);
    if (addonAppDefString != null) {
      Scanner scanner = new Scanner(addonAppDefString).useDelimiter(",");
      while (scanner.hasNext()) {
        String addonAppDef = scanner.next();
        String addonAppDefPath = instanceDefinition
            .getAppConfOperations().getGlobalOptions().get(addonAppDef);
        log.debug("Addon package {} is stored at: {}", addonAppDef, addonAppDefPath);
        LocalResource addonPkgRes = fileSystem.createAmResource(
            fileSystem.getFileSystem().resolvePath(new Path(addonAppDefPath)),
            LocalResourceType.ARCHIVE);
        launcher.addLocalResource(ADDON_DEFINITION_DIR + "/" + addonAppDef, addonPkgRes);
      }
      log.debug("Metainfo map for master and addon: {}",
          packageMetainfo.toString());
    }    

    // Additional files to localize in addition to the application def
    String appResourcesString = instanceDefinition.getAppConfOperations()
        .getGlobalOptions().getOption(APP_RESOURCES, null);
    log.info("Configuration value for extra resources to localize: {}", appResourcesString);
    if (null != appResourcesString) {
      try (Scanner scanner = new Scanner(appResourcesString).useDelimiter(",")) {
        while (scanner.hasNext()) {
          String resource = scanner.next();
          Path resourcePath = new Path(resource);
          LocalResource extraResource = fileSystem.createAmResource(
              fileSystem.getFileSystem().resolvePath(resourcePath),
              LocalResourceType.FILE);
          String destination = APP_RESOURCES_DIR + "/" + resourcePath.getName();
          log.info("Localizing {} to {}", resourcePath, destination);
          // TODO Can we try harder to avoid collisions?
          launcher.addLocalResource(destination, extraResource);
        }
      }
    }

    // initialize addon pkg states for all componentInstanceStatus
    Map<String, State> pkgStatuses = new TreeMap<>();
    for (Metainfo appPkg : packageMetainfo.values()) {
      // check each component of that addon to see if they apply to this
      // component 'role'
      for (ComponentsInAddonPackage comp : appPkg.getApplicationPackage()
          .getComponents()) {
        log.debug("Current component: {} component in metainfo: {}", roleName,
            comp.getName());
        if (comp.getName().equals(roleGroup)
            || comp.getName().equals(ADDON_FOR_ALL_COMPONENTS)) {
          pkgStatuses.put(appPkg.getApplicationPackage().getName(), State.INIT);
        }
      }
    }
    log.debug("For component: {} pkg status map: {}", roleName,
        pkgStatuses.toString());
    
    // initialize the component instance state
    getComponentStatuses().put(label,
                               new ComponentInstanceState(
                                   roleGroup,
                                   container.getId(),
                                   getClusterInfoPropertyValue(OptionKeys.APPLICATION_NAME),
                                   pkgStatuses));
  }

  @VisibleForTesting
  protected void localizeConfigFile(ContainerLauncher launcher,
                                     String roleName, String roleGroup,
                                     ConfigFile configFile,
                                     Map<String, Map<String, String>> configs,
                                     MapOperations env,
                                     SliderFileSystem fileSystem)
      throws IOException {
    ConfigFormat configFormat = ConfigFormat.resolve(configFile.getType());
    providerUtils.localizeConfigFile(launcher, roleName, roleGroup,
        configFile.getDictionaryName(), configFormat, configFile.getFileName(),
        configs, env, fileSystem, getClusterName());
  }

  /**
   * build the zookeeper registry path.
   * 
   * @return the path the service registered at
   * @throws NullPointerException if the service has not yet registered
   */
  private String getZkRegistryPath() {
    Preconditions.checkNotNull(yarnRegistry, "Yarn registry not bound");
    String path = yarnRegistry.getAbsoluteSelfRegistrationPath();
    Preconditions.checkNotNull(path, "Service record path not defined");
    return path;
  }

  @Override
  public void rebuildContainerDetails(List<Container> liveContainers,
                                      String applicationId, Map<Integer, ProviderRole> providerRoleMap) {
    for (Container container : liveContainers) {
      // get the role name and label
      ProviderRole role = providerRoleMap.get(ContainerPriority
                                                  .extractRole(container));
      if (role != null) {
        String roleName = role.name;
        String roleGroup = role.group;
        String label = getContainerLabel(container, roleName, roleGroup);
        log.info("Rebuilding in-memory: container {} in role {} in cluster {}",
                 container.getId(), roleName, applicationId);
        getComponentStatuses().put(label,
            new ComponentInstanceState(roleGroup, container.getId(),
                                       applicationId));
      } else {
        log.warn("Role not found for container {} in cluster {}",
                 container.getId(), applicationId);
      }
    }
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  /**
   * Handle registration calls from the agents
   *
   * @param registration registration entry
   *
   * @return response
   */
  @Override
  public RegistrationResponse handleRegistration(Register registration) {
    log.info("Handling registration: {}", registration);
    RegistrationResponse response = new RegistrationResponse();
    String label = registration.getLabel();
    String pkg = registration.getPkg();
    State agentState = registration.getActualState();
    String appVersion = registration.getAppVersion();

    log.info("label: {} pkg: {}", label, pkg);

    if (getComponentStatuses().containsKey(label)) {
      response.setResponseStatus(RegistrationStatus.OK);
      ComponentInstanceState componentStatus = getComponentStatuses().get(label);
      componentStatus.heartbeat(System.currentTimeMillis());
      updateComponentStatusWithAgentState(componentStatus, agentState);

      String roleName = getRoleName(label);
      String roleGroup = getRoleGroup(label);
      String containerId = getContainerId(label);

      if (SliderUtils.isSet(registration.getTags())) {
        tags.recordAssignedTag(roleName, containerId, registration.getTags());
      } else {
        response.setTags(tags.getTag(roleName, containerId));
      }

      String hostFqdn = registration.getPublicHostname();
      Map<String, String> ports = registration.getAllocatedPorts();
      if (ports != null && !ports.isEmpty()) {
        processAllocatedPorts(hostFqdn, roleName, roleGroup, containerId, ports);
      }

      Map<String, String> folders = registration.getLogFolders();
      if (folders != null && !folders.isEmpty()) {
        publishFolderPaths(folders, containerId, roleName, hostFqdn);
      }

      // Set app version if empty. It gets unset during upgrade - why?
      checkAndSetContainerAppVersion(containerId, appVersion);
    } else {
      response.setResponseStatus(RegistrationStatus.FAILED);
      response.setLog("Label not recognized.");
      log.warn("Received registration request from unknown label {}", label);
    }
    log.info("Registration response: {}", response);
    return response;
  }

  // Checks if app version is empty. Sets it to the version as reported by the
  // container during registration phase.
  private void checkAndSetContainerAppVersion(String containerId,
      String appVersion) {
    StateAccessForProviders amState = getAmState();
    try {
      RoleInstance role = amState.getOwnedContainer(containerId);
      if (role != null) {
        String currentAppVersion = role.appVersion;
        log.debug("Container = {}, app version current = {} new = {}",
            containerId, currentAppVersion, appVersion);
        if (currentAppVersion == null
            || currentAppVersion.equals(APP_VERSION_UNKNOWN)) {
          amState.getOwnedContainer(containerId).appVersion = appVersion;
        }
      }
    } catch (NoSuchNodeException e) {
      // ignore - there is nothing to do if we don't find a container
      log.warn("Owned container {} not found - {}", containerId, e);
    }
  }

  /**
   * Handle heartbeat response from agents
   *
   * @param heartBeat incoming heartbeat from Agent
   *
   * @return response to send back
   */
  @Override
  public HeartBeatResponse handleHeartBeat(HeartBeat heartBeat) {
    log.debug("Handling heartbeat: {}", heartBeat);
    HeartBeatResponse response = new HeartBeatResponse();
    long id = heartBeat.getResponseId();
    response.setResponseId(id + 1L);

    String label = heartBeat.getHostname();
    String pkg = heartBeat.getPackage();

    log.debug("package received: " + pkg);
    
    String roleName = getRoleName(label);
    String roleGroup = getRoleGroup(label);
    String containerId = getContainerId(label);
    boolean doUpgrade = false;
    if (isInUpgradeMode && upgradeContainers.contains(containerId)) {
      doUpgrade = true;
    }

    CommandScript cmdScript = getScriptPathForMasterPackage(roleGroup);
    List<ComponentCommand> commands = getApplicationComponent(roleGroup).getCommands();

    if (!isDockerContainer(roleGroup) && !isYarnDockerContainer(roleGroup)
        && (cmdScript == null || cmdScript.getScript() == null)
        && commands.size() == 0) {
      log.error(
          "role.script is unavailable for {}. Commands will not be sent.",
          roleName);
      return response;
    }

    String scriptPath = null;
    long timeout = 600L;
    if (cmdScript != null) {
      scriptPath = cmdScript.getScript();
      timeout = cmdScript.getTimeout();
    }

    if (timeout == 0L) {
      timeout = 600L;
    }

    if (!getComponentStatuses().containsKey(label)) {
      // container is completed but still heart-beating, send terminate signal
      log.info(
          "Sending terminate signal to completed container (still heartbeating): {}",
          label);
      response.setTerminateAgent(true);
      return response;
    }

    List<ComponentStatus> statuses = heartBeat.getComponentStatus();
    if (statuses != null && !statuses.isEmpty()) {
      log.info("status from agent: " + statuses.toString());
      for(ComponentStatus status : statuses){
        providerUtils.updateServiceRecord(getAmState(), yarnRegistry,
            containerId, roleName, Collections.singletonList(status.getIp()),
            status.getHostname());
      }
    }

    Boolean isMaster = isMaster(roleGroup);
    ComponentInstanceState componentStatus = getComponentStatuses().get(label);
    componentStatus.heartbeat(System.currentTimeMillis());
    if (doUpgrade) {
      switch (componentStatus.getState()) {
      case STARTED:
        componentStatus.setTargetState(State.UPGRADED);
        break;
      case UPGRADED:
        componentStatus.setTargetState(State.STOPPED);
        break;
      case STOPPED:
        componentStatus.setTargetState(State.TERMINATING);
        break;
      default:
        break;
      }
      log.info("Current state = {} target state {}",
          componentStatus.getState(), componentStatus.getTargetState());
    }

    if (appStopInitiated && !componentStatus.isStopInitiated()) {
      log.info("Stop initiated for label {}", label);
      componentStatus.setTargetState(State.STOPPED);
      componentStatus.setStopInitiated(true);
    }

    publishConfigAndExportGroups(heartBeat, componentStatus, roleGroup);
    CommandResult result = null;
    List<CommandReport> reports = heartBeat.getReports();
    if (SliderUtils.isNotEmpty(reports)) {
      CommandReport report = reports.get(0);
      Map<String, String> ports = report.getAllocatedPorts();
      if (SliderUtils.isNotEmpty(ports)) {
        processAllocatedPorts(heartBeat.getFqdn(), roleName, roleGroup, containerId, ports);
      }
      result = CommandResult.getCommandResult(report.getStatus());
      Command command = Command.getCommand(report.getRoleCommand());
      componentStatus.applyCommandResult(result, command, pkg);
      log.info("Component operation. Status: {}; new container state: {};"
          + " new component state: {}", result,
          componentStatus.getContainerState(), componentStatus.getState());

      if (command == Command.INSTALL && SliderUtils.isNotEmpty(report.getFolders())) {
        publishFolderPaths(report.getFolders(), containerId, roleName, heartBeat.getFqdn());
      }
    }

    int waitForCount = getAmState().getInstanceDefinitionSnapshot().
        getAppConfOperations().getComponentOptInt(roleGroup, WAIT_HEARTBEAT, 0);

    if (id < waitForCount) {
      log.info("Waiting until heartbeat count {}. Current val: {}", waitForCount, id);
      getComponentStatuses().put(label, componentStatus);
      return response;
    }

    Command command = componentStatus.getNextCommand(doUpgrade);
    try {
      if (Command.NOP != command) {
        log.debug("For comp {} pkg {} issuing {}", roleName,
            componentStatus.getNextPkgToInstall(), command.toString());
        if (command == Command.INSTALL) {
          log.info("Installing {} on {}.", roleName, containerId);
          if (isDockerContainer(roleGroup) || isYarnDockerContainer(roleGroup)){
            addInstallDockerCommand(roleName, roleGroup, containerId,
                response, null, timeout);
          } else if (scriptPath != null) {
            addInstallCommand(roleName, roleGroup, containerId, response,
                scriptPath, null, timeout, null);
          } else {
            // commands
            ComponentCommand installCmd = null;
            for (ComponentCommand compCmd : commands) {
              if (compCmd.getName().equals("INSTALL")) {
                installCmd = compCmd;
              }
            }
            addInstallCommand(roleName, roleGroup, containerId, response, null,
                installCmd, timeout, null);
          }
          componentStatus.commandIssued(command);
        } else if (command == Command.INSTALL_ADDON) {
          String nextPkgToInstall = componentStatus.getNextPkgToInstall();
          // retrieve scriptPath or command of that package for the component
          for (ComponentsInAddonPackage comp : packageMetainfo
              .get(nextPkgToInstall).getApplicationPackage().getComponents()) {
            // given nextPkgToInstall and roleName is determined, the if below
            // should only execute once per heartbeat
            log.debug("Addon component: {} pkg: {} script: {}", comp.getName(),
                nextPkgToInstall, comp.getCommandScript().getScript());
            if (comp.getName().equals(roleGroup)
                || comp.getName().equals(ADDON_FOR_ALL_COMPONENTS)) {
              scriptPath = comp.getCommandScript().getScript();
              if (scriptPath != null) {
                addInstallCommand(roleName, roleGroup, containerId, response,
                    scriptPath, null, timeout, nextPkgToInstall);
              } else {
                ComponentCommand installCmd = null;
                for (ComponentCommand compCmd : comp.getCommands()) {
                  if (compCmd.getName().equals("INSTALL")) {
                    installCmd = compCmd;
                  }
                }
                addInstallCommand(roleName, roleGroup, containerId, response,
                    null, installCmd, timeout, nextPkgToInstall);
              }
            }
          }
          componentStatus.commandIssued(command);
        } else if (command == Command.START) {
          // check against dependencies
          boolean canExecute = commandOrder.canExecute(roleGroup, command, getComponentStatuses().values());
          if (canExecute) {
            log.info("Starting {} on {}.", roleName, containerId);
            if (isDockerContainer(roleGroup) || isYarnDockerContainer(roleGroup)){
              addStartDockerCommand(roleName, roleGroup, containerId,
                  response, null, timeout, false);
            } else if (scriptPath != null) {
              addStartCommand(roleName,
                              roleGroup,
                              containerId,
                              response,
                              scriptPath,
                              null,
                              null,
                              timeout,
                              isMarkedAutoRestart(roleGroup));
            } else {
              ComponentCommand startCmd = null;
              for (ComponentCommand compCmd : commands) {
                if (compCmd.getName().equals("START")) {
                  startCmd = compCmd;
                }
              }
              ComponentCommand stopCmd = null;
              for (ComponentCommand compCmd : commands) {
                if (compCmd.getName().equals("STOP")) {
                  stopCmd = compCmd;
                }
              }
              addStartCommand(roleName, roleGroup, containerId, response, null,
                  startCmd, stopCmd, timeout, false);
            }
            componentStatus.commandIssued(command);
          } else {
            log.info("Start of {} on {} delayed as dependencies have not started.", roleName, containerId);
          }
        } else if (command == Command.UPGRADE) {
          addUpgradeCommand(roleName, roleGroup, containerId, response,
              scriptPath, timeout);
          componentStatus.commandIssued(command, true);
        } else if (command == Command.STOP) {
          log.info("Stop command being sent to container with id {}",
              containerId);
          addStopCommand(roleName, roleGroup, containerId, response, scriptPath,
              timeout, doUpgrade);
          componentStatus.commandIssued(command);
        } else if (command == Command.TERMINATE) {
          log.info("A formal terminate command is being sent to container {}"
              + " in state {}", label, componentStatus.getState());
          response.setTerminateAgent(true);
        }
      }

      // if there is no outstanding command then retrieve config
      if (isMaster && componentStatus.getState() == State.STARTED
          && command == Command.NOP) {
        if (!componentStatus.getConfigReported()) {
          log.info("Requesting applied config for {} on {}.", roleName, containerId);
          if (isDockerContainer(roleGroup) || isYarnDockerContainer(roleGroup)){
            addGetConfigDockerCommand(roleName, roleGroup, containerId, response);
          } else {
            addGetConfigCommand(roleName, roleGroup, containerId, response);
          }
        }
      }
      
      // if restart is required then signal
      response.setRestartEnabled(false);
      if (componentStatus.getState() == State.STARTED
          && command == Command.NOP && isMarkedAutoRestart(roleGroup)) {
        response.setRestartEnabled(true);
      }

      //If INSTALL_FAILED and no INSTALL is scheduled let the agent fail
      if (componentStatus.getState() == State.INSTALL_FAILED
         && command == Command.NOP) {
        log.warn("Sending terminate signal to container that failed installation: {}", label);
        response.setTerminateAgent(true);
      }

    } catch (SliderException e) {
      log.warn("Component instance failed operation.", e);
      componentStatus.applyCommandResult(CommandResult.FAILED, command, null);
    }

    log.debug("Heartbeat response: " + response);
    return response;
  }

  private boolean isDockerContainer(String roleGroup) {
    String type = getApplicationComponent(roleGroup).getType();
    if (SliderUtils.isSet(type)) {
      return type.toLowerCase().equals(SliderUtils.DOCKER) || type.toLowerCase().equals(SliderUtils.DOCKER_YARN);
    }
    return false;
  }

  private boolean isYarnDockerContainer(String roleGroup) {
    String type = getApplicationComponent(roleGroup).getType();
    if (SliderUtils.isSet(type)) {
      return type.toLowerCase().equals(SliderUtils.DOCKER_YARN);
    }
    return false;
  }

  protected void processAllocatedPorts(String fqdn,
                                       String roleName,
                                       String roleGroup,
                                       String containerId,
                                       Map<String, String> ports) {
    RoleInstance instance;
    try {
      instance = getAmState().getOwnedContainer(containerId);
    } catch (NoSuchNodeException e) {
      log.warn("Failed to locate instance of container {}", containerId, e);
      instance = null;
    }
    for (Map.Entry<String, String> port : ports.entrySet()) {
      String portname = port.getKey();
      String portNo = port.getValue();
      log.info("Recording allocated port for {} as {}", portname, portNo);

      // add the allocated ports to the global list as well as per container list
      // per container allocation will over-write each other in the global
      this.getAllocatedPorts().put(portname, portNo);
      this.getAllocatedPorts(containerId).put(portname, portNo);
      if (instance != null) {
        try {
          // if the returned value is not a single port number then there are no
          // meaningful way for Slider to use it during export
          // No need to error out as it may not be the responsibility of the component
          // to allocate port or the component may need an array of ports
          instance.registerPortEndpoint(Integer.valueOf(portNo), portname);
        } catch (NumberFormatException e) {
          log.warn("Failed to parse {}", portNo, e);
        }
      }
    }

    processAndPublishComponentSpecificData(ports, containerId, fqdn, roleGroup);
    processAndPublishComponentSpecificExports(ports, containerId, fqdn, roleName, roleGroup);

    // and update registration entries
    if (instance != null) {
      queueAccess.put(new RegisterComponentInstance(instance.getId(),
          roleName, roleGroup, 0, TimeUnit.MILLISECONDS));
    }
  }

  private void updateComponentStatusWithAgentState(
      ComponentInstanceState componentStatus, State agentState) {
    if (agentState != null) {
      componentStatus.setState(agentState);
    }
  }

  @Override
  public Map<String, MonitorDetail> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, MonitorDetail> details = super.buildMonitorDetails(clusterDesc);
    buildRoleHostDetails(details);
    return details;
  }

  public void applyInitialRegistryDefinitions(URL amWebURI,
      URL agentOpsURI,
      URL agentStatusURI,
      ServiceRecord serviceRecord)
    throws IOException {
    super.applyInitialRegistryDefinitions(amWebURI,
                                          serviceRecord);

    try {
      URL restURL = new URL(agentOpsURI, SLIDER_PATH_AGENTS);
      URL agentStatusURL = new URL(agentStatusURI, SLIDER_PATH_AGENTS);

      serviceRecord.addInternalEndpoint(
          new Endpoint(CustomRegistryConstants.AGENT_SECURE_REST_API,
                       ProtocolTypes.PROTOCOL_REST,
                       restURL.toURI()));
      serviceRecord.addInternalEndpoint(
          new Endpoint(CustomRegistryConstants.AGENT_ONEWAY_REST_API,
                       ProtocolTypes.PROTOCOL_REST,
                       agentStatusURL.toURI()));
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }

    // identify client component
    Component client = null;
    for (Component component : getMetaInfo().getApplication().getComponents()) {
      if (component.getCategory().equals("CLIENT")) {
        client = component;
        break;
      }
    }
    if (client == null) {
      log.info("No client component specified, not publishing client configs");
      return;
    }

    // register AM-generated client configs
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    MapOperations clientOperations = appConf.getOrAddComponent(client.getName());
    appConf.resolve();
    if (!clientOperations.getOptionBool(AM_CONFIG_GENERATION,
        false)) {
      log.info("AM config generation is false, not publishing client configs");
      return;
    }

    // build and localize configuration files
    Map<String, Map<String, String>> configurations = new TreeMap<>();
    Map<String, String> tokens = providerUtils.getStandardTokenMap(appConf,
        getAmState().getInternalsSnapshot(), client.getName(),
        client.getName(), getClusterName());

    for (ConfigFile configFile : getMetaInfo().getComponentConfigFiles(client.getName())) {
      addNamedConfiguration(configFile.getDictionaryName(),
          appConf.getGlobalOptions().options, configurations, tokens, null,
          client.getName(), client.getName());
      if (appConf.getComponent(client.getName()) != null) {
        addNamedConfiguration(configFile.getDictionaryName(),
            appConf.getComponent(client.getName()).options, configurations,
            tokens, null, client.getName(), client.getName());
      }
    }

    //do a final replacement of re-used configs
    dereferenceAllConfigs(configurations);

    for (ConfigFile configFile : getMetaInfo().getComponentConfigFiles(client.getName())) {
      ConfigFormat configFormat = ConfigFormat.resolve(configFile.getType());

      Map<String, String> config = configurations.get(configFile.getDictionaryName());
      ConfigUtils.prepConfigForTemplateOutputter(configFormat, config,
          fileSystem, getClusterName(),
          new File(configFile.getFileName()).getName());
      PublishedConfiguration publishedConfiguration =
          new PublishedConfiguration(configFile.getDictionaryName(),
              config.entrySet());
      getAmState().getPublishedSliderConfigurations().put(
          configFile.getDictionaryName(), publishedConfiguration);
      log.info("Publishing AM configuration {}", configFile.getDictionaryName());
    }
  }

  @Override
  public void notifyContainerCompleted(ContainerId containerId) {
    // containers get allocated and free'ed without being assigned to any
    // component - so many of the data structures may not be initialized
    if (containerId != null) {
      String containerIdStr = containerId.toString();
      if (getComponentInstanceData().containsKey(containerIdStr)) {
        getComponentInstanceData().remove(containerIdStr);
        log.info("Removing container specific data for {}", containerIdStr);
        publishComponentInstanceData();
      }

      if (this.allocatedPorts.containsKey(containerIdStr)) {
        Map<String, String> portsByContainerId = getAllocatedPorts(containerIdStr);
        this.allocatedPorts.remove(containerIdStr);
        // free up the allocations from global as well
        // if multiple containers allocate global ports then last one
        // wins and similarly first one removes it - its not supported anyway
        for(String portName : portsByContainerId.keySet()) {
          getAllocatedPorts().remove(portName);
        }

      }

      String componentName = null;
      synchronized (this.componentStatuses) {
        for (String label : getComponentStatuses().keySet()) {
          if (label.startsWith(containerIdStr)) {
            componentName = getRoleName(label);
            log.info("Removing component status for label {}", label);
            getComponentStatuses().remove(label);
          }
        }
      }

      tags.releaseTag(componentName, containerIdStr);

      synchronized (this.containerExportsMap) {
        Set<String> containerExportSets = containerExportsMap.get(containerIdStr);
        if (containerExportSets != null) {
          for (String containerExportStr : containerExportSets) {
            String[] parts = containerExportStr.split(":");
            Map<String, List<ExportEntry>> exportGroup = getCurrentExports(parts[0]);
            List<ExportEntry> exports = exportGroup.get(parts[1]);
            List<ExportEntry> exportToRemove = new ArrayList<ExportEntry>();
            for (ExportEntry export : exports) {
              if (containerIdStr.equals(export.getContainerId())) {
                exportToRemove.add(export);
              }
            }
            exports.removeAll(exportToRemove);
          }
          log.info("Removing container exports for {}", containerIdStr);
          containerExportsMap.remove(containerIdStr);
        }
      }
    }
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   *
   * @param instanceDefinition
   */
  private void readAndSetHeartbeatMonitoringInterval(AggregateConf instanceDefinition) {
    String hbMonitorInterval = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(HEARTBEAT_MONITOR_INTERVAL,
                                     Integer.toString(DEFAULT_HEARTBEAT_MONITOR_INTERVAL));
    try {
      setHeartbeatMonitorInterval(Integer.parseInt(hbMonitorInterval));
    } catch (NumberFormatException e) {
      log.warn(
          "Bad value {} for {}. Defaulting to ",
          hbMonitorInterval,
          HEARTBEAT_MONITOR_INTERVAL,
          DEFAULT_HEARTBEAT_MONITOR_INTERVAL);
    }
  }

  /**
   * Reads and sets the heartbeat monitoring interval. If bad value is provided then log it and set to default.
   *
   * @param instanceDefinition
   */
  private void initializeAgentDebugCommands(AggregateConf instanceDefinition) {
    String launchParameterStr = instanceDefinition.getAppConfOperations().
        getGlobalOptions().getOption(AGENT_INSTANCE_DEBUG_DATA, "");
    agentLaunchParameter = new AgentLaunchParameter(launchParameterStr);
  }

  @VisibleForTesting
  protected Map<String, ExportEntry> getLogFolderExports() {
    return logFolderExports;
  }

  @VisibleForTesting
  protected Map<String, ExportEntry> getWorkFolderExports() {
    return workFolderExports;
  }

  protected Metainfo getMetaInfo() {
    return getMetaInfo(null);
  }

  @VisibleForTesting
  protected Metainfo getMetaInfo(String roleGroup) {
    String mapKey = DEFAULT_METAINFO_MAP_KEY;
    if (roleGroup != null) {
      ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
      mapKey = appConf.getComponentOpt(roleGroup, ROLE_PREFIX,
          DEFAULT_METAINFO_MAP_KEY);
    }
    MetainfoHolder mh = this.metaInfoMap.get(mapKey);
    if (mh == null) {
      return null;
    }
    return mh.metaInfo;
  }

  @VisibleForTesting
  protected Map<String, ComponentInstanceState> getComponentStatuses() {
    return componentStatuses;
  }

  @VisibleForTesting
  protected Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
      String appDef, boolean addonPackage) throws IOException,
      BadConfigException {
    return AgentUtils.getApplicationMetainfo(fileSystem, appDef, addonPackage);
  }

  @VisibleForTesting
  protected Metainfo getApplicationMetainfo(SliderFileSystem fileSystem,
      String appDef) throws IOException, BadConfigException {
    return getApplicationMetainfo(fileSystem, appDef, false);
  }

  @VisibleForTesting
  protected void setHeartbeatMonitorInterval(int heartbeatMonitorInterval) {
    this.heartbeatMonitorInterval = heartbeatMonitorInterval;
  }

  public void setInUpgradeMode(boolean inUpgradeMode) {
    this.isInUpgradeMode = inUpgradeMode;
  }

  public void addUpgradeContainers(Set<String> upgradeContainers) {
    this.upgradeContainers.addAll(upgradeContainers);
  }

  public void setAppStopInitiated(boolean appStopInitiated) {
    this.appStopInitiated = appStopInitiated;
  }

  /**
   * Read all default configs
   *
   * @param fileSystem fs
   * @param appDef app default path
   * @param metainfo metadata
   *
   * @return configuration maps
   * 
   * @throws IOException
   */
  protected Map<String, DefaultConfig> initializeDefaultConfigs(SliderFileSystem fileSystem,
                                                                String appDef, Metainfo metainfo) throws IOException {
    Map<String, DefaultConfig> defaultConfigMap = new HashMap<>();
    if (SliderUtils.isNotEmpty(metainfo.getApplication().getConfigFiles())) {
      for (ConfigFile configFile : metainfo.getApplication().getConfigFiles()) {
        DefaultConfig config = null;
        try {
          config = AgentUtils.getDefaultConfig(fileSystem, appDef, configFile.getDictionaryName() + ".xml");
        } catch (IOException e) {
          log.warn("Default config file not found. Only the config as input during create will be applied for {}",
                   configFile.getDictionaryName());
        }
        if (config != null) {
          defaultConfigMap.put(configFile.getDictionaryName(), config);
        }
      }
    }

    return defaultConfigMap;
  }

  protected Map<String, DefaultConfig> getDefaultConfigs(String roleGroup) {
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    String mapKey = appConf.getComponentOpt(roleGroup, ROLE_PREFIX,
        DEFAULT_METAINFO_MAP_KEY);
    return metaInfoMap.get(mapKey).defaultConfigs;
  }

  private int getHeartbeatMonitorInterval() {
    return this.heartbeatMonitorInterval;
  }

  private String getClusterName() {
    if (SliderUtils.isUnset(clusterName)) {
      clusterName = getAmState().getInternalsSnapshot().get(OptionKeys.APPLICATION_NAME);
    }
    return clusterName;
  }

  @VisibleForTesting
  protected void publishApplicationInstanceData(String name, String description,
                                                Iterable<Map.Entry<String, String>> entries) {
    providerUtils.publishApplicationInstanceData(name, description, entries,
        getAmState());
  }

  /**
   * Get a list of all hosts for all role/container per role
   *
   * @return the map of role->node
   */
  protected Map<String, Map<String, ClusterNode>> getRoleClusterNodeMapping() {
    return amState.getRoleClusterNodeMapping();
  }

  private String getContainerLabel(Container container, String role, String group) {
    if (role.equals(group)) {
      return container.getId().toString() + LABEL_MAKER + role;
    } else {
      return container.getId().toString() + LABEL_MAKER + role + LABEL_MAKER +
          group;
    }
  }

  protected String getClusterInfoPropertyValue(String name) {
    StateAccessForProviders accessor = getAmState();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getInfo(name);
  }

  protected String getClusterOptionPropertyValue(String name)
      throws BadConfigException {
    StateAccessForProviders accessor = getAmState();
    assert accessor.isApplicationLive();
    ClusterDescription description = accessor.getClusterStatus();
    return description.getMandatoryOption(name);
  }

  /**
   * Lost heartbeat from the container - release it and ask for a replacement (async operation)
   *
   * @param label
   * @param containerId
   */
  protected void lostContainer(
      String label,
      ContainerId containerId) {
    getComponentStatuses().remove(label);
    getQueueAccess().put(new ProviderReportedContainerLoss(containerId));
  }

  /**
   * Build the provider status, can be empty
   *
   * @return the provider status - map of entries to add to the info section
   */
  public Map<String, String> buildProviderStatus() {
    Map<String, String> stats = new HashMap<String, String>();
    return stats;
  }

  @VisibleForTesting
  protected void publishFolderPaths(
      Map<String, String> folders, String containerId, String componentName, String hostFqdn) {
    providerUtils.publishFolderPaths(folders, containerId, componentName, hostFqdn,
        getAmState(), getLogFolderExports(), getWorkFolderExports());
  }

  /**
   * Process return status for component instances
   *
   * @param heartBeat
   * @param componentStatus
   */
  protected void publishConfigAndExportGroups(HeartBeat heartBeat,
      ComponentInstanceState componentStatus, String componentGroup) {
    List<ComponentStatus> statuses = heartBeat.getComponentStatus();
    if (statuses != null && !statuses.isEmpty()) {
      log.info("Processing {} status reports.", statuses.size());
      for (ComponentStatus status : statuses) {
        log.info("Status report: {}", status.toString());

        if (status.getConfigs() != null) {
          Application application = getMetaInfo(componentGroup).getApplication();

          if ((!canAnyMasterPublishConfig(componentGroup) || canPublishConfig(componentGroup)) &&
              !getAmState().getAppConfSnapshot().getComponentOptBool(
                  componentGroup, AM_CONFIG_GENERATION, false)) {
            // If no Master can explicitly publish then publish if its a master
            // Otherwise, wait till the master that can publish is ready

            Set<String> exportedConfigs = new HashSet();
            String exportedConfigsStr = application.getExportedConfigs();
            boolean exportedAllConfigs = exportedConfigsStr == null || exportedConfigsStr.isEmpty();
            if (!exportedAllConfigs) {
              for (String exportedConfig : exportedConfigsStr.split(",")) {
                if (exportedConfig.trim().length() > 0) {
                  exportedConfigs.add(exportedConfig.trim());
                }
              }
            }

            for (String key : status.getConfigs().keySet()) {
              if ((!exportedAllConfigs && exportedConfigs.contains(key)) ||
                  exportedAllConfigs) {
                Map<String, String> configs = status.getConfigs().get(key);
                publishApplicationInstanceData(key, key, configs.entrySet());
              }
            }
          }

          List<ExportGroup> appExportGroups = application.getExportGroups();
          boolean hasExportGroups = SliderUtils.isNotEmpty(appExportGroups);

          Set<String> appExports = new HashSet();
          String appExportsStr = getApplicationComponent(componentGroup).getAppExports();
          if (SliderUtils.isSet(appExportsStr)) {
            for (String appExport : appExportsStr.split(",")) {
              if (!appExport.trim().isEmpty()) {
                appExports.add(appExport.trim());
              }
            }
          }

          if (hasExportGroups && !appExports.isEmpty()) {
            String configKeyFormat = "${site.%s.%s}";
            String hostKeyFormat = "${%s_HOST}";

            // publish export groups if any
            Map<String, String> replaceTokens = new HashMap<String, String>();
            for (Map.Entry<String, Map<String, ClusterNode>> entry : getRoleClusterNodeMapping().entrySet()) {
              String hostName = providerUtils.getHostsList(
                  entry.getValue().values(), true).iterator().next();
              replaceTokens.put(String.format(hostKeyFormat, entry.getKey().toUpperCase(Locale.ENGLISH)), hostName);
            }

            for (String key : status.getConfigs().keySet()) {
              Map<String, String> configs = status.getConfigs().get(key);
              for (String configKey : configs.keySet()) {
                String lookupKey = String.format(configKeyFormat, key, configKey);
                replaceTokens.put(lookupKey, configs.get(configKey));
              }
            }

            Set<String> modifiedGroups = new HashSet<String>();
            for (ExportGroup exportGroup : appExportGroups) {
              List<Export> exports = exportGroup.getExports();
              if (SliderUtils.isNotEmpty(exports)) {
                String exportGroupName = exportGroup.getName();
                ConcurrentHashMap<String, List<ExportEntry>> map =
                    (ConcurrentHashMap<String, List<ExportEntry>>)getCurrentExports(exportGroupName);
                for (Export export : exports) {
                  if (canBeExported(exportGroupName, export.getName(), appExports)) {
                    String value = export.getValue();
                    // replace host names
                    for (String token : replaceTokens.keySet()) {
                      if (value.contains(token)) {
                        value = value.replace(token, replaceTokens.get(token));
                      }
                    }
                    ExportEntry entry = new ExportEntry();
                    entry.setLevel(APPLICATION_TAG);
                    entry.setValue(value);
                    entry.setUpdatedTime(new Date().toString());
                    // over-write, app exports are singletons
                    map.put(export.getName(), new ArrayList(Arrays.asList(entry)));
                    log.info("Preparing to publish. Key {} and Value {}", export.getName(), value);
                  }
                }
                modifiedGroups.add(exportGroupName);
              }
            }
            publishModifiedExportGroups(modifiedGroups);
          }

          log.info("Received and processed config for {}", heartBeat.getHostname());
          componentStatus.setConfigReported(true);

        }
      }
    }
  }

  private boolean canBeExported(String exportGroupName, String name, Set<String> appExports) {
    return appExports.contains(String.format("%s-%s", exportGroupName, name));
  }

  protected Map<String, List<ExportEntry>> getCurrentExports(String groupName) {
    if (!this.exportGroups.containsKey(groupName)) {
      synchronized (this.exportGroups) {
        if (!this.exportGroups.containsKey(groupName)) {
          this.exportGroups.put(groupName, new ConcurrentHashMap<String, List<ExportEntry>>());
        }
      }
    }

    return this.exportGroups.get(groupName);
  }

  private void publishModifiedExportGroups(Set<String> modifiedGroups) {
    for (String roleGroup : modifiedGroups) {
      Map<String, List<ExportEntry>> entries = this.exportGroups.get(roleGroup);
      // Publish in old format for the time being
      Map<String, String> simpleEntries = new HashMap<String, String>();
      for (Entry<String, List<ExportEntry>> entry : entries.entrySet()) {
        List<ExportEntry> exports = entry.getValue();
        if (SliderUtils.isNotEmpty(exports)) {
          // there is no support for multiple exports per name - so extract only the first one
          simpleEntries.put(entry.getKey(), entry.getValue().get(0).getValue());
        }
      }
      publishApplicationInstanceData(roleGroup, roleGroup,
          simpleEntries.entrySet());

      PublishedExports exports = new PublishedExports(roleGroup);
      exports.setUpdated(new Date().getTime());
      exports.putValues(entries.entrySet());
      getAmState().getPublishedExportsSet().put(roleGroup, exports);
    }
  }

  /** Publish component instance specific data if the component demands it */
  protected void processAndPublishComponentSpecificData(Map<String, String> ports,
                                                        String containerId,
                                                        String hostFqdn,
                                                        String componentGroup) {
    String portVarFormat = "${site.%s}";
    String hostNamePattern = "${THIS_HOST}";
    Map<String, String> toPublish = new HashMap<String, String>();

    Application application = getMetaInfo(componentGroup).getApplication();
    for (Component component : application.getComponents()) {
      if (component.getName().equals(componentGroup)) {
        if (component.getComponentExports().size() > 0) {

          for (ComponentExport export : component.getComponentExports()) {
            String templateToExport = export.getValue();
            for (String portName : ports.keySet()) {
              boolean publishData = false;
              String portValPattern = String.format(portVarFormat, portName);
              if (templateToExport.contains(portValPattern)) {
                templateToExport = templateToExport.replace(portValPattern, ports.get(portName));
                publishData = true;
              }
              if (templateToExport.contains(hostNamePattern)) {
                templateToExport = templateToExport.replace(hostNamePattern, hostFqdn);
                publishData = true;
              }
              if (publishData) {
                toPublish.put(export.getName(), templateToExport);
                log.info("Publishing {} for name {} and container {}",
                         templateToExport, export.getName(), containerId);
              }
            }
          }
        }
      }
    }

    if (toPublish.size() > 0) {
      Map<String, String> perContainerData = null;
      if (!getComponentInstanceData().containsKey(containerId)) {
        perContainerData = new ConcurrentHashMap<String, String>();
      } else {
        perContainerData = getComponentInstanceData().get(containerId);
      }
      perContainerData.putAll(toPublish);
      getComponentInstanceData().put(containerId, perContainerData);
      publishComponentInstanceData();
    }
  }

  /** Publish component instance specific data if the component demands it */
  protected void processAndPublishComponentSpecificExports(Map<String, String> ports,
                                                           String containerId,
                                                           String hostFqdn,
                                                           String compName,
                                                           String compGroup) {
    String portVarFormat = "${site.%s}";
    String hostNamePattern = "${" + compGroup + "_HOST}";

    List<ExportGroup> appExportGroups = getMetaInfo(compGroup).getApplication().getExportGroups();
    Component component = getApplicationComponent(compGroup);
    if (component != null && SliderUtils.isSet(component.getCompExports())
        && SliderUtils.isNotEmpty(appExportGroups)) {

      Set<String> compExports = new HashSet();
      String compExportsStr = component.getCompExports();
      for (String compExport : compExportsStr.split(",")) {
        if (!compExport.trim().isEmpty()) {
          compExports.add(compExport.trim());
        }
      }

      Date now = new Date();
      Set<String> modifiedGroups = new HashSet<String>();
      for (ExportGroup exportGroup : appExportGroups) {
        List<Export> exports = exportGroup.getExports();
        if (SliderUtils.isNotEmpty(exports)) {
          String exportGroupName = exportGroup.getName();
          ConcurrentHashMap<String, List<ExportEntry>> map =
              (ConcurrentHashMap<String, List<ExportEntry>>) getCurrentExports(exportGroupName);
          for (Export export : exports) {
            if (canBeExported(exportGroupName, export.getName(), compExports)) {
              log.info("Attempting to publish {} of group {} for component type {}",
                       export.getName(), exportGroupName, compName);
              String templateToExport = export.getValue();
              for (String portName : ports.keySet()) {
                boolean publishData = false;
                String portValPattern = String.format(portVarFormat, portName);
                if (templateToExport.contains(portValPattern)) {
                  templateToExport = templateToExport.replace(portValPattern, ports.get(portName));
                  publishData = true;
                }
                if (templateToExport.contains(hostNamePattern)) {
                  templateToExport = templateToExport.replace(hostNamePattern, hostFqdn);
                  publishData = true;
                }
                if (publishData) {
                  ExportEntry entryToAdd = new ExportEntry();
                  entryToAdd.setLevel(COMPONENT_TAG);
                  entryToAdd.setValue(templateToExport);
                  entryToAdd.setUpdatedTime(now.toString());
                  entryToAdd.setContainerId(containerId);
                  entryToAdd.setTag(tags.getTag(compName, containerId));

                  List<ExportEntry> existingList =
                      map.putIfAbsent(export.getName(), new CopyOnWriteArrayList(Arrays.asList(entryToAdd)));

                  // in-place edit, no lock needed
                  if (existingList != null) {
                    boolean updatedInPlace = false;
                    for (ExportEntry entry : existingList) {
                      if (containerId.toLowerCase(Locale.ENGLISH)
                                     .equals(entry.getContainerId())) {
                        entryToAdd.setValue(templateToExport);
                        entryToAdd.setUpdatedTime(now.toString());
                        updatedInPlace = true;
                      }
                    }
                    if (!updatedInPlace) {
                      existingList.add(entryToAdd);
                    }
                  }

                  log.info("Publishing {} for name {} and container {}",
                           templateToExport, export.getName(), containerId);
                  modifiedGroups.add(exportGroupName);
                  synchronized (containerExportsMap) {
                    if (!containerExportsMap.containsKey(containerId)) {
                      containerExportsMap.put(containerId, new HashSet<String>());
                    }
                    Set<String> containerExportMaps = containerExportsMap.get(containerId);
                    containerExportMaps.add(String.format("%s:%s", exportGroupName, export.getName()));
                  }
                }
              }
            }
          }
        }
      }
      publishModifiedExportGroups(modifiedGroups);
    }
  }

  private void publishComponentInstanceData() {
    Map<String, String> dataToPublish = new HashMap<String, String>();
    for (String container : getComponentInstanceData().keySet()) {
      for (String prop : getComponentInstanceData().get(container).keySet()) {
        dataToPublish.put(
            container + "." + prop, getComponentInstanceData().get(container).get(prop));
      }
    }
    publishApplicationInstanceData(COMPONENT_DATA_TAG, COMPONENT_DATA_TAG, dataToPublish.entrySet());
  }

  /**
   * Return Component based on group
   *
   * @param roleGroup component group
   *
   * @return the component entry or null for no match
   */
  protected Component getApplicationComponent(String roleGroup) {
    Metainfo metainfo = getMetaInfo(roleGroup);
    if (metainfo == null) {
      return null;
    }
    return metainfo.getApplicationComponent(roleGroup);
  }

  /**
   * Extract script path from the application metainfo
   *
   * @param roleGroup component group
   * @return the script path or null for no match
   */
  protected CommandScript getScriptPathForMasterPackage(String roleGroup) {
    Component component = getApplicationComponent(roleGroup);
    if (component != null) {
      return component.getCommandScript();
    }
    return null;
  }

  /**
   * Is the role of type MASTER
   *
   * @param roleGroup component group
   *
   * @return true if the role category is MASTER
   */
  protected boolean isMaster(String roleGroup) {
    Component component = getApplicationComponent(roleGroup);
    if (component != null) {
      if (component.getCategory().equals("MASTER")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Can the role publish configuration
   *
   * @param roleGroup component group
   *
   * @return true if it can be pubished
   */
  protected boolean canPublishConfig(String roleGroup) {
    Component component = getApplicationComponent(roleGroup);
    if (component != null) {
      return Boolean.TRUE.toString().equals(component.getPublishConfig());
    }
    return false;
  }

  /**
   * Checks if the role is marked auto-restart
   *
   * @param roleGroup component group
   *
   * @return true if it is auto-restart
   */
  protected boolean isMarkedAutoRestart(String roleGroup) {
    Component component = getApplicationComponent(roleGroup);
    if (component != null) {
      return component.getAutoStartOnFailureBoolean();
    }
    return false;
  }

  /**
   * Can any master publish config explicitly, if not a random master is used
   *
   * @return true if the condition holds
   */
  protected boolean canAnyMasterPublishConfig(String roleGroup) {
    if (canAnyMasterPublish == null) {
      Application application = getMetaInfo(roleGroup).getApplication();
      if (application == null) {
        log.error("Malformed app definition: Expect application as root element in the metainfo.xml");
      } else {
        for (Component component : application.getComponents()) {
          if (Boolean.TRUE.toString().equals(component.getPublishConfig()) &&
              component.getCategory().equals("MASTER")) {
            canAnyMasterPublish = true;
          }
        }
      }
    }

    if (canAnyMasterPublish == null) {
      canAnyMasterPublish = false;
    }
    return canAnyMasterPublish;
  }

  private String getRoleName(String label) {
    int index1 = label.indexOf(LABEL_MAKER);
    int index2 = label.lastIndexOf(LABEL_MAKER);
    if (index1 == index2) {
      return label.substring(index1 + LABEL_MAKER.length());
    } else {
      return label.substring(index1 + LABEL_MAKER.length(), index2);
    }
  }

  private String getRoleGroup(String label) {
    return label.substring(label.lastIndexOf(LABEL_MAKER) + LABEL_MAKER.length());
  }

  private String getContainerId(String label) {
    return label.substring(0, label.indexOf(LABEL_MAKER));
  }

  /**
   * Add install command to the heartbeat response
   *
   * @param roleName
   * @param roleGroup
   * @param containerId
   * @param response
   * @param scriptPath
   * @param pkg
   *          when this field is null, it indicates the command is for the
   *          master package; while not null, for the package named by this
   *          field
   * @throws SliderException
   */
  @VisibleForTesting
  protected void addInstallCommand(String roleName,
                                   String roleGroup,
                                   String containerId,
                                   HeartBeatResponse response,
                                   String scriptPath,
                                   ComponentCommand compCmd,
                                   long timeout,
                                   String pkg)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = getClusterName();
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    cmd.setPkg(pkg);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getOption(JAVA_HOME, getJDKDir()));
    hostLevelParams.put(PACKAGE_LIST, getPackageList(roleGroup));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, Map<String, String>> configurations =
        buildCommandConfigurations(appConf, getAmState().getInternalsSnapshot(),
            containerId, roleName, roleGroup);
    cmd.setConfigurations(configurations);
    Map<String, Map<String, String>> componentConfigurations = buildComponentConfigurations(appConf);
    cmd.setComponentConfigurations(componentConfigurations);

    if (SliderUtils.isSet(scriptPath)) {
      cmd.setCommandParams(commandParametersSet(scriptPath, timeout, false));
    } else {
      // assume it to be default shell command
      ComponentCommand effectiveCommand = compCmd;
      if (effectiveCommand == null) {
        effectiveCommand = ComponentCommand.getDefaultComponentCommand("INSTALL");
      }
      cmd.setCommandParams(commandParametersSet(effectiveCommand, timeout, false));
      configurations.get("global").put("exec_cmd", effectiveCommand.getExec());
    }

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));

    response.addExecutionCommand(cmd);

    log.debug("command looks like: {} ",  cmd);
  }

  @VisibleForTesting
  protected void addInstallDockerCommand(String roleName,
                                   String roleGroup,
                                   String containerId,
                                   HeartBeatResponse response,
                                   ComponentCommand compCmd,
                                   long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = getClusterName();
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.INSTALL.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(PACKAGE_LIST, getPackageList(roleGroup));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(
        appConf, getAmState().getInternalsSnapshot(), containerId, roleName,
        roleGroup);
    cmd.setConfigurations(configurations);
    Map<String, Map<String, String>> componentConfigurations = buildComponentConfigurations(appConf);
    cmd.setComponentConfigurations(componentConfigurations);
    
    ComponentCommand effectiveCommand = compCmd;
    if (compCmd == null) {
      effectiveCommand = new ComponentCommand();
      effectiveCommand.setName("INSTALL");
      effectiveCommand.setExec("DEFAULT");
    }
    cmd.setCommandParams(setCommandParameters(effectiveCommand, timeout, false));
    configurations.get("global").put("exec_cmd", effectiveCommand.getExec());

    cmd.setHostname(getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME));
    cmd.addContainerDetails(roleGroup, getMetaInfo(roleGroup));

    Map<String, String> dockerConfig = new HashMap<String, String>();
    if(isYarnDockerContainer(roleGroup)){
      //put nothing
      cmd.setYarnDockerMode(true);
    } else {
      dockerConfig.put(
          "docker.command_path",
          getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
              "commandPath"));
      dockerConfig.put("docker.image_name",
          getConfigFromMetaInfo(roleGroup, "image"));
    }
    configurations.put("docker", dockerConfig);

    log.debug("Docker- command: {}", cmd.toString());

    response.addExecutionCommand(cmd);
  }

  private Map<String, String> setCommandParameters(String scriptPath,
      long timeout, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder",
        "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", Long.toString(timeout));
    cmdParams.put("script_type", AbstractComponent.TYPE_PYTHON);
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  private Map<String, String> setCommandParameters(ComponentCommand compCmd,
      long timeout, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<String, String>();
    cmdParams.put("service_package_folder",
        "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("command", compCmd.getExec());
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", Long.toString(timeout));
    cmdParams.put("script_type", compCmd.getType());
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  private Map<String, Map<String, String>> buildComponentConfigurations(
      ConfTreeOperations appConf) {
    return appConf.getComponents();
  }

  protected static String getPackageListFromApplication(Application application) {
    String pkgFormatString = "{\"type\":\"%s\",\"name\":\"%s\"}";
    String pkgListFormatString = "[%s]";
    List<String> packages = new ArrayList<>();
    if (application != null) {
      if (application.getPackages().size() > 0) {
        // no-op if there are packages that are not OS-specific, as these
        // will be localized by AM rather than the Agent
        // this should be backwards compatible, as there was previously an
        // XML parsing bug that ensured non-OS-specific packages did not exist
      } else {
        List<OSSpecific> osSpecifics = application.getOSSpecifics();
        if (osSpecifics != null && osSpecifics.size() > 0) {
          for (OSSpecific osSpecific : osSpecifics) {
            if (osSpecific.getOsType().equals("any")) {
              for (OSPackage osPackage : osSpecific.getPackages()) {
                packages.add(String.format(pkgFormatString, osPackage.getType(), osPackage.getName()));
              }
            }
          }
        }
      }
    }

    if (!packages.isEmpty()) {
      return "[" + SliderUtils.join(packages, ",", false) + "]";
    } else {
      return "[]";
    }
  }

  private String getPackageList(String roleGroup) {
    return getPackageListFromApplication(getMetaInfo(roleGroup).getApplication());
  }

  private void prepareExecutionCommand(ExecutionCommand cmd) {
    cmd.setTaskId(taskId.incrementAndGet());
    cmd.setCommandId(cmd.getTaskId() + "-1");
  }

  private Map<String, String> commandParametersSet(String scriptPath, long timeout, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<>();
    cmdParams.put("service_package_folder",
                  "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("script", scriptPath);
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", Long.toString(timeout));
    cmdParams.put("script_type", "PYTHON");
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  private Map<String, String> commandParametersSet(ComponentCommand compCmd, long timeout, boolean recordConfig) {
    Map<String, String> cmdParams = new TreeMap<>();
    cmdParams.put("service_package_folder",
                  "${AGENT_WORK_ROOT}/work/app/definition/package");
    cmdParams.put("command", compCmd.getExec());
    cmdParams.put("schema_version", "2.0");
    cmdParams.put("command_timeout", Long.toString(timeout));
    cmdParams.put("script_type", compCmd.getType());
    cmdParams.put("record_config", Boolean.toString(recordConfig));
    return cmdParams;
  }

  @VisibleForTesting
  protected void addStatusCommand(String roleName,
                                  String roleGroup,
                                  String containerId,
                                  HeartBeatResponse response,
                                  String scriptPath,
                                  long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    if (isDockerContainer(roleGroup) || isYarnDockerContainer(roleGroup)) {
      addStatusDockerCommand(roleName, roleGroup, containerId, response,
          scriptPath, timeout);
      return;
    }

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.STATUS_COMMAND);

    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getOption(JAVA_HOME, getJDKDir()));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(commandParametersSet(scriptPath, timeout, false));

    Map<String, Map<String, String>> configurations =
        buildCommandConfigurations(appConf, getAmState().getInternalsSnapshot(),
            containerId, roleName, roleGroup);

    cmd.setConfigurations(configurations);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addStatusDockerCommand(String roleName,
                                  String roleGroup,
                                  String containerId,
                                  HeartBeatResponse response,
                                  String scriptPath,
                                  long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.STATUS_COMMAND);

    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    cmd.setCommandParams(setCommandParameters(scriptPath, timeout, false));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(
        appConf, getAmState().getInternalsSnapshot(), containerId, roleName,
        roleGroup);
    Map<String, String> dockerConfig = new HashMap<String, String>();
    String statusCommand = getConfigFromMetaInfoWithAppConfigOverriding(roleGroup, "statusCommand");
    if (statusCommand == null) {
      if(isYarnDockerContainer(roleGroup)){
        //should complain the required field is null
        cmd.setYarnDockerMode(true);
      } else {
        statusCommand = "docker top "
            + containerId
            + " | grep \"\"";// default value
      }
    }
    dockerConfig.put("docker.status_command",statusCommand);
    configurations.put("docker", dockerConfig);
    cmd.setConfigurations(configurations);
    log.debug("Docker- status {}", cmd);
    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addGetConfigDockerCommand(String roleName, String roleGroup,
      String containerId, HeartBeatResponse response) throws SliderException {
    assert getAmState().isApplicationLive();

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.GET_CONFIG_COMMAND);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    hostLevelParams.put(CONTAINER_ID, containerId);

    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    Map<String, Map<String, String>> configurations = buildCommandConfigurations(
        appConf, getAmState().getInternalsSnapshot(), containerId, roleName,
        roleGroup);
    Map<String, String> dockerConfig = new HashMap<String, String>();
    String statusCommand = getConfigFromMetaInfoWithAppConfigOverriding(roleGroup, "statusCommand");
    if (statusCommand == null) {
      if(isYarnDockerContainer(roleGroup)){
        //should complain the required field is null
        cmd.setYarnDockerMode(true);
      } else {
        statusCommand = "docker top "
            + containerId
            + " | grep \"\"";// default value
      }
    }
    dockerConfig.put("docker.status_command",statusCommand);
    configurations.put("docker", dockerConfig);

    cmd.setConfigurations(configurations);
    log.debug("Docker- getconfig command {}", cmd);
    
    response.addStatusCommand(cmd);
  }
  
  private String getConfigFromMetaInfoWithAppConfigOverriding(String roleGroup,
      String configName){
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    String containerName = getApplicationComponent(roleGroup)
        .getDockerContainers().get(0).getName();
    String composedConfigName = null;
    String appConfigValue = null;
    //if the configName is about port , mount, inputfile, then check differently
    if (configName.equals("containerPort") || configName.equals("hostPort")){
      composedConfigName = containerName + ".ports." + configName;
    } else 
    if (configName.equals("containerMount")
        || configName.equals("hostMount")){
      composedConfigName = containerName + ".mounts." + configName;
    } else
    if (configName.equals("containerPath")
        || configName.equals("fileLocalPath")) {
      composedConfigName = containerName + ".inputFiles." + configName;
    } else {
      composedConfigName = containerName + "." + configName;
    }
    appConfigValue = appConf.getComponentOpt(roleGroup, composedConfigName,
        null);
    log.debug(
        "Docker- value from appconfig component: {} configName: {} value: {}",
        roleGroup, composedConfigName, appConfigValue);
    if (appConfigValue == null) {
      appConfigValue = getConfigFromMetaInfo(roleGroup, configName);
      log.debug(
          "Docker- value from metainfo component: {} configName: {} value: {}",
          roleGroup, configName, appConfigValue);

    }
    return appConfigValue;
  }

  @VisibleForTesting
  protected void addStartDockerCommand(String roleName, String roleGroup,
      String containerId, HeartBeatResponse response,
      ComponentCommand startCommand, long timeout, boolean isMarkedAutoRestart)
      throws
      SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.START.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, String> roleParams = new TreeMap<>();
    cmd.setRoleParams(roleParams);
    cmd.getRoleParams().put("auto_restart", Boolean.toString(isMarkedAutoRestart));
    startCommand = new ComponentCommand();
    startCommand.setName("START");
    startCommand.setType("docker");
    startCommand.setExec("exec");
    cmd.setCommandParams(setCommandParameters(startCommand, timeout, true));
    
    Map<String, Map<String, String>> configurations = buildCommandConfigurations(
        appConf, getAmState().getInternalsSnapshot(), containerId, roleName,
        roleGroup);
    Map<String, Map<String, String>> componentConfigurations = buildComponentConfigurations(appConf);
    cmd.setComponentConfigurations(componentConfigurations);
    
    Map<String, String> dockerConfig = new HashMap<String, String>();
    if (isYarnDockerContainer(roleGroup)) {
      dockerConfig.put(
          "docker.startCommand",
          getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
              "start_command"));
      cmd.setYarnDockerMode(true);
    } else {
      dockerConfig.put(
        "docker.command_path",
        getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
            "commandPath"));

      dockerConfig.put("docker.image_name",
          getConfigFromMetaInfo(roleGroup, "image"));
      // options should always have -d
      String options = getConfigFromMetaInfoWithAppConfigOverriding(
          roleGroup, "options");
      if(options != null && !options.isEmpty()){
        options = options + " -d";
      } else {
        options = "-d";
      }
      dockerConfig.put("docker.options", options);
      // options should always have -d
      dockerConfig.put(
          "docker.containerPort",
          getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
              "containerPort"));
      dockerConfig
          .put(
              "docker.hostPort",
              getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
                  "hostPort"));
  
      dockerConfig.put(
          "docker.mounting_directory",
          getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
              "containerMount"));
      dockerConfig
          .put(
              "docker.host_mounting_directory",
              getConfigFromMetaInfoWithAppConfigOverriding(roleGroup,
                  "hostMount"));
  
      dockerConfig.put("docker.additional_param",
          getConfigFromMetaInfoWithAppConfigOverriding(roleGroup, "additionalParam"));
  
      dockerConfig.put("docker.input_file.mount_path", getConfigFromMetaInfo(
          roleGroup, "containerPath"));
    }

    String lifetime = getConfigFromMetaInfoWithAppConfigOverriding(
        roleGroup, "lifetime");
    dockerConfig.put("docker.lifetime", lifetime);
    configurations.put("docker", dockerConfig);
    String statusCommand = getConfigFromMetaInfoWithAppConfigOverriding(
        roleGroup, "statusCommand");
    if (statusCommand == null) {
      if(isYarnDockerContainer(roleGroup)){
        //should complain the required field is null
      } else {
        statusCommand = "docker top "
          + containerId + " | grep \"\"";
      }
    }
    dockerConfig.put("docker.status_command",statusCommand);
    
    cmd.setConfigurations(configurations);
   // configurations.get("global").put("exec_cmd", startCommand.getExec());
    cmd.addContainerDetails(roleGroup, getMetaInfo(roleGroup));

    log.info("Docker- command: {}", cmd.toString());

    response.addExecutionCommand(cmd);
  }

  private String getConfigFromMetaInfo(String roleGroup, String configName) {
    String result = null;

    List<DockerContainer> containers = getApplicationComponent(
        roleGroup).getDockerContainers();// to support multi container per
                                             // component later
    log.debug("Docker- containers metainfo: {}", containers.toString());
    if (containers.size() > 0) {
      DockerContainer container = containers.get(0);
      switch (configName) {
      case "start_command":
        result = container.getStartCommand();
        break;
      case "image":
        result = container.getImage();
        break;
      case "network":
        if (container.getNetwork() == null || container.getNetwork().isEmpty()) {
          result = "none";
        } else {
          result = container.getNetwork();
        }
        break;
      case "statusCommand":
        result = container.getStatusCommand();
        break;
      case "commandPath":
        result = container.getCommandPath();
        break;
      case "options":
        result = container.getOptions();
        break;
      case "containerPort":
        result = container.getPorts().size() > 0 ? container.getPorts().get(0)
            .getContainerPort() : null;// to support
        // multi port
        // later
        break;
      case "hostPort":
        result = container.getPorts().size() > 0 ? container.getPorts().get(0)
            .getHostPort() : null;// to support multi
        // port later
        break;
      case "containerMount":
        result = container.getMounts().size() > 0 ? container.getMounts()
            .get(0).getContainerMount() : null;// to support
        // multi port
        // later
        break;
      case "hostMount":
        result = container.getMounts().size() > 0 ? container.getMounts()
            .get(0).getHostMount() : null;// to support multi
        // port later
        break;
      case "additionalParam":
        result = container.getAdditionalParam();// to support multi port later
        break;
      case "runPriviledgedContainer":
        if (container.getRunPrivilegedContainer() == null) {
          result = "false";
        } else {
          result = container.getRunPrivilegedContainer();
        }
        break;
      default:
        break;
      }
    }
    log.debug("Docker- component: {} configName: {} value: {}", roleGroup, configName, result);
    return result;
  }

  @VisibleForTesting
  protected void addGetConfigCommand(String roleName, String roleGroup,
      String containerId, HeartBeatResponse response) throws SliderException {
    assert getAmState().isApplicationLive();

    StatusCommand cmd = new StatusCommand();
    String clusterName = getClusterName();

    cmd.setCommandType(AgentCommandType.STATUS_COMMAND);
    cmd.setComponentName(roleName);
    cmd.setServiceName(clusterName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(StatusCommand.GET_CONFIG_COMMAND);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    hostLevelParams.put(CONTAINER_ID, containerId);

    response.addStatusCommand(cmd);
  }

  @VisibleForTesting
  protected void addStartCommand(String roleName, String roleGroup, String containerId,
                                 HeartBeatResponse response,
                                 String scriptPath, ComponentCommand startCommand,
                                 ComponentCommand stopCommand,
                                 long timeout, boolean isMarkedAutoRestart)
      throws
      SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.START.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions().getOption(JAVA_HOME, getJDKDir()));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);

    Map<String, String> roleParams = new TreeMap<>();
    cmd.setRoleParams(roleParams);
    cmd.getRoleParams().put("auto_restart", Boolean.toString(isMarkedAutoRestart));

    Map<String, Map<String, String>> configurations =
        buildCommandConfigurations(appConf, internalsConf, containerId,
            roleName, roleGroup);
    cmd.setConfigurations(configurations);
    Map<String, Map<String, String>> componentConfigurations = buildComponentConfigurations(appConf);
    cmd.setComponentConfigurations(componentConfigurations);
    
    if (SliderUtils.isSet(scriptPath)) {
      cmd.setCommandParams(commandParametersSet(scriptPath, timeout, true));
    } else {
      if (startCommand == null) {
        throw new SliderException("Expected START command not found for component " + roleName);
      }
      cmd.setCommandParams(commandParametersSet(startCommand, timeout, true));
      configurations.get("global").put("exec_cmd", startCommand.getExec());
    }

    response.addExecutionCommand(cmd);

    log.debug("command looks like: {}", cmd);
    // With start command, the corresponding command for graceful stop needs to
    // be sent. This will be used when a particular container is lost as per RM,
    // but then the agent is still running and heart-beating to the Slider AM.
    ExecutionCommand cmdStop = new ExecutionCommand(
        AgentCommandType.EXECUTION_COMMAND);
    cmdStop.setTaskId(taskId.get());
    cmdStop.setCommandId(cmdStop.getTaskId() + "-1");
    cmdStop.setHostname(hostName);
    cmdStop.setClusterName(clusterName);
    cmdStop.setRoleCommand(Command.STOP.toString());
    cmdStop.setServiceName(clusterName);
    cmdStop.setComponentName(roleName);
    cmdStop.setRole(roleName);
    Map<String, String> hostLevelParamsStop = new TreeMap<String, String>();
    hostLevelParamsStop.put(JAVA_HOME, appConf.getGlobalOptions()
        .getOption(JAVA_HOME, ""));
    hostLevelParamsStop.put(CONTAINER_ID, containerId);
    cmdStop.setHostLevelParams(hostLevelParamsStop);

    Map<String, String> roleParamsStop = new TreeMap<String, String>();
    cmdStop.setRoleParams(roleParamsStop);
    cmdStop.getRoleParams().put("auto_restart",
                                Boolean.toString(isMarkedAutoRestart));

    if (SliderUtils.isSet(scriptPath)) {
      cmdStop.setCommandParams(commandParametersSet(scriptPath, timeout, true));
    } else {
      if (stopCommand == null) {
        stopCommand = ComponentCommand.getDefaultComponentCommand("STOP");
      }
      cmd.setCommandParams(commandParametersSet(stopCommand, timeout, true));
      configurations.get("global").put("exec_cmd", startCommand.getExec());
    }


    Map<String, Map<String, String>> configurationsStop = buildCommandConfigurations(
        appConf, internalsConf, containerId, roleName, roleGroup);
    cmdStop.setConfigurations(configurationsStop);
    response.addExecutionCommand(cmdStop);
  }

  @VisibleForTesting
  protected void addUpgradeCommand(String roleName, String roleGroup, String containerId,
      HeartBeatResponse response, String scriptPath, long timeout)
      throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmd = new ExecutionCommand(
        AgentCommandType.EXECUTION_COMMAND);
    prepareExecutionCommand(cmd);
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmd.setHostname(hostName);
    cmd.setClusterName(clusterName);
    cmd.setRoleCommand(Command.UPGRADE.toString());
    cmd.setServiceName(clusterName);
    cmd.setComponentName(roleName);
    cmd.setRole(roleName);
    Map<String, String> hostLevelParams = new TreeMap<String, String>();
    hostLevelParams.put(JAVA_HOME, appConf.getGlobalOptions()
        .getMandatoryOption(JAVA_HOME));
    hostLevelParams.put(CONTAINER_ID, containerId);
    cmd.setHostLevelParams(hostLevelParams);
    cmd.setCommandParams(commandParametersSet(scriptPath, timeout, true));

    Map<String, Map<String, String>> configurations = buildCommandConfigurations(
        appConf, internalsConf, containerId, roleName, roleGroup);
    cmd.setConfigurations(configurations);
    response.addExecutionCommand(cmd);
  }
    
  protected void addStopCommand(String roleName, String roleGroup, String containerId,
      HeartBeatResponse response, String scriptPath, long timeout,
      boolean isInUpgradeMode) throws SliderException {
    assert getAmState().isApplicationLive();
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    ExecutionCommand cmdStop = new ExecutionCommand(
        AgentCommandType.EXECUTION_COMMAND);
    cmdStop.setTaskId(taskId.get());
    cmdStop.setCommandId(cmdStop.getTaskId() + "-1");
    String clusterName = internalsConf.get(OptionKeys.APPLICATION_NAME);
    String hostName = getClusterInfoPropertyValue(StatusKeys.INFO_AM_HOSTNAME);
    cmdStop.setHostname(hostName);
    cmdStop.setClusterName(clusterName);
    // Upgrade stop is differentiated by passing a transformed role command -
    // UPGRADE_STOP
    cmdStop.setRoleCommand(Command.transform(Command.STOP, isInUpgradeMode));
    cmdStop.setServiceName(clusterName);
    cmdStop.setComponentName(roleName);
    cmdStop.setRole(roleName);
    Map<String, String> hostLevelParamsStop = new TreeMap<String, String>();
    hostLevelParamsStop.put(JAVA_HOME, appConf.getGlobalOptions()
        .getMandatoryOption(JAVA_HOME));
    hostLevelParamsStop.put(CONTAINER_ID, containerId);
    cmdStop.setHostLevelParams(hostLevelParamsStop);
    cmdStop.setCommandParams(commandParametersSet(scriptPath, timeout, true));

    Map<String, Map<String, String>> configurationsStop = buildCommandConfigurations(
        appConf, internalsConf, containerId, roleName, roleGroup);
    cmdStop.setConfigurations(configurationsStop);
    response.addExecutionCommand(cmdStop);
  }

  protected static String getJDKDir() {
    File javaHome = new File(System.getProperty("java.home")).getParentFile();
    File jdkDirectory = null;
    if (javaHome.getName().contains("jdk")) {
      jdkDirectory = javaHome;
    }
    if (jdkDirectory != null) {
      return jdkDirectory.getAbsolutePath();
    } else {
      return "";
    }
  }

  protected Map<String, String> getAllocatedPorts() {
    return getAllocatedPorts(SHARED_PORT_TAG);
  }

  protected Map<String, Map<String, String>> getComponentInstanceData() {
    return this.componentInstanceData;
  }

  protected Map<String, String> getAllocatedPorts(String containerId) {
    if (!this.allocatedPorts.containsKey(containerId)) {
      synchronized (this.allocatedPorts) {
        if (!this.allocatedPorts.containsKey(containerId)) {
          this.allocatedPorts.put(containerId,
                                  new ConcurrentHashMap<String, String>());
        }
      }
    }
    return this.allocatedPorts.get(containerId);
  }

  private Map<String, Map<String, String>> buildCommandConfigurations(
      ConfTreeOperations appConf, ConfTreeOperations internalsConf,
      String containerId, String roleName, String roleGroup)
      throws SliderException {

    Map<String, Map<String, String>> configurations = new TreeMap<>();
    Map<String, String> tokens = providerUtils.getStandardTokenMap(appConf,
        internalsConf, roleName, roleGroup, getClusterName());
    tokens.put("${CONTAINER_ID}", containerId);

    Set<String> configs = new HashSet<String>();
    configs.addAll(getApplicationConfigurationTypes(roleGroup));
    configs.addAll(getSystemConfigurationsRequested(appConf));

    for (String configType : configs) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
                            configurations, tokens, containerId, roleName,
                            roleGroup);
      if (appConf.getComponent(roleGroup) != null) {
        addNamedConfiguration(configType, appConf.getComponent(roleGroup).options,
            configurations, tokens, containerId, roleName, roleGroup);
      }
    }

    //do a final replacement of re-used configs
    dereferenceAllConfigs(configurations);

    return configurations;
  }

  @VisibleForTesting
  protected void dereferenceAllConfigs(Map<String, Map<String, String>> configurations) {
    providerUtils.dereferenceAllConfigs(configurations);
  }

  @VisibleForTesting
  protected Set<String> getSystemConfigurationsRequested(ConfTreeOperations appConf) {
    return providerUtils.getSystemConfigurationsRequested(appConf);
  }

  @VisibleForTesting
  protected List<String> getApplicationConfigurationTypes(String roleGroup) {
    List<String> configList = new ArrayList<String>();
    configList.add(GLOBAL_CONFIG_TAG);

    List<ConfigFile> configFiles = getMetaInfo(roleGroup).getApplication().getConfigFiles();
    for (ConfigFile configFile : configFiles) {
      log.info("Expecting config type {}.", configFile.getDictionaryName());
      configList.add(configFile.getDictionaryName());
    }
    for (Component component : getMetaInfo(roleGroup).getApplication().getComponents()) {
      if (!component.getName().equals(roleGroup)) {
        continue;
      }
      if (component.getDockerContainers() == null) {
        continue;
      }
      for (DockerContainer container : component.getDockerContainers()) {
        if (container.getConfigFiles() == null) {
          continue;
        }
        for (ConfigFile configFile : container.getConfigFiles()) {
          log.info("Expecting config type {}.", configFile.getDictionaryName());
          configList.add(configFile.getDictionaryName());
        }
      }
    }

    // remove duplicates.  mostly worried about 'global' being listed
    return new ArrayList<String>(new HashSet<String>(configList));
  }

  private void addNamedConfiguration(String configName, Map<String, String> sourceConfig,
                                     Map<String, Map<String, String>> configurations,
                                     Map<String, String> tokens, String containerId,
                                     String roleName, String roleGroup) {
    Map<String, String> config = new HashMap<String, String>();
    if (configName.equals(GLOBAL_CONFIG_TAG)) {
      addDefaultGlobalConfig(config, containerId, roleName);
    }
    // add role hosts to tokens
    addRoleRelatedTokens(tokens);
    providerUtils.propagateSiteOptions(sourceConfig, config, configName, tokens);

    //apply any port updates
    if (!this.getAllocatedPorts().isEmpty()) {
      for (String key : config.keySet()) {
        String value = config.get(key);
        String lookupKey = configName + "." + key;
        if (!value.contains(PER_CONTAINER_TAG)) {
          // If the config property is shared then pass on the already allocated value
          // from any container
          if (this.getAllocatedPorts().containsKey(lookupKey)) {
            config.put(key, getAllocatedPorts().get(lookupKey));
          }
        } else {
          if (this.getAllocatedPorts(containerId).containsKey(lookupKey)) {
            config.put(key, getAllocatedPorts(containerId).get(lookupKey));
          }
        }
      }
    }

    //apply defaults only if the key is not present and value is not empty
    if (getDefaultConfigs(roleGroup).containsKey(configName)) {
      log.info("Adding default configs for type {}.", configName);
      for (PropertyInfo defaultConfigProp : getDefaultConfigs(roleGroup).get(configName).getPropertyInfos()) {
        if (!config.containsKey(defaultConfigProp.getName())) {
          if (!defaultConfigProp.getName().isEmpty() &&
              defaultConfigProp.getValue() != null &&
              !defaultConfigProp.getValue().isEmpty()) {
            config.put(defaultConfigProp.getName(), defaultConfigProp.getValue());
          }
        }
      }
    }

    configurations.put(configName, config);
  }

  @VisibleForTesting
  protected void addRoleRelatedTokens(Map<String, String> tokens) {
    providerUtils.addRoleRelatedTokens(tokens, getAmState());
  }

  private void addDefaultGlobalConfig(Map<String, String> config, String containerId, String roleName) {
    config.put("app_log_dir", "${AGENT_LOG_ROOT}");
    config.put("app_pid_dir", "${AGENT_WORK_ROOT}/app/run");
    config.put("app_install_dir", "${AGENT_WORK_ROOT}/app/install");
    config.put("app_conf_dir", "${AGENT_WORK_ROOT}/" + APP_CONF_DIR);
    config.put("app_input_conf_dir", "${AGENT_WORK_ROOT}/" + PROPAGATED_CONF_DIR_NAME);
    config.put("app_container_id", containerId);
    config.put("app_container_tag", tags.getTag(roleName, containerId));

    // add optional parameters only if they are not already provided
    if (!config.containsKey("pid_file")) {
      config.put("pid_file", "${AGENT_WORK_ROOT}/app/run/component.pid");
    }
    if (!config.containsKey("app_root")) {
      config.put("app_root", "${AGENT_WORK_ROOT}/app/install");
    }
  }

  private void buildRoleHostDetails(Map<String, MonitorDetail> details) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getRoleClusterNodeMapping().entrySet()) {
      details.put(entry.getKey() + " Host(s)/Container(s)",
                  new MonitorDetail(providerUtils.getHostsList(
                      entry.getValue().values(), false).toString(), false));
    }
  }
}
