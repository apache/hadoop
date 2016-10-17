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
package org.apache.slider.providers.docker;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.CommandLineBuilder;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.ConfigUtils;
import org.apache.slider.core.registry.docstore.ExportEntry;
import org.apache.slider.providers.AbstractProviderService;
import org.apache.slider.providers.MonitorDetail;
import org.apache.slider.providers.ProviderCore;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.providers.ProviderUtils;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.regex.Pattern;

import static org.apache.slider.api.RoleKeys.ROLE_PREFIX;

public class DockerProviderService extends AbstractProviderService implements
    ProviderCore,
    DockerKeys,
    SliderKeys {

  protected static final Logger log =
      LoggerFactory.getLogger(DockerProviderService.class);
  private static final ProviderUtils providerUtils = new ProviderUtils(log);
  private static final String EXPORT_GROUP = "quicklinks";
  private static final String APPLICATION_TAG = "application";

  private String clusterName = null;
  private SliderFileSystem fileSystem = null;

  protected DockerProviderService() {
    super("DockerProviderService");
  }

  @Override
  public List<ProviderRole> getRoles() {
    return Collections.emptyList();
  }

  @Override
  public boolean isSupportedRole(String role) {
    return true;
  }

  @Override
  public void validateInstanceDefinition(AggregateConf instanceDefinition)
      throws SliderException {
  }

  private String getClusterName() {
    if (SliderUtils.isUnset(clusterName)) {
      clusterName = getAmState().getInternalsSnapshot().get(OptionKeys.APPLICATION_NAME);
    }
    return clusterName;
  }

  @Override
  public void buildContainerLaunchContext(ContainerLauncher launcher,
      AggregateConf instanceDefinition, Container container,
      ProviderRole providerRole, SliderFileSystem fileSystem,
      Path generatedConfPath, MapOperations resourceComponent,
      MapOperations appComponent, Path containerTmpDirPath)
      throws IOException, SliderException {

    String roleName = providerRole.name;
    String roleGroup = providerRole.group;

    initializeApplicationConfiguration(instanceDefinition, fileSystem,
        roleGroup);

    log.info("Build launch context for Docker");
    log.debug(instanceDefinition.toString());

    ConfTreeOperations appConf = instanceDefinition.getAppConfOperations();
    launcher.setYarnDockerMode(true);
    launcher.setDockerImage(appConf.getComponentOpt(roleGroup, DOCKER_IMAGE,
        null));
    launcher.setDockerNetwork(appConf.getComponentOpt(roleGroup, DOCKER_NETWORK,
        DEFAULT_DOCKER_NETWORK));
    launcher.setRunPrivilegedContainer(appConf.getComponentOptBool(roleGroup,
        DOCKER_USE_PRIVILEGED, false));

    // Set the environment
    Map<String, String> standardTokens = providerUtils.getStandardTokenMap(
        getAmState().getAppConfSnapshot(), getAmState().getInternalsSnapshot(),
        roleName, roleGroup, container.getId().toString(), getClusterName());
    Map<String, String> replaceTokens = providerUtils.filterSiteOptions(
            appConf.getComponent(roleGroup).options, standardTokens);
    replaceTokens.putAll(standardTokens);
    launcher.putEnv(SliderUtils.buildEnvMap(appComponent, replaceTokens));

    String workDir = ApplicationConstants.Environment.PWD.$();
    launcher.setEnv("WORK_DIR", workDir);
    log.info("WORK_DIR set to {}", workDir);
    String logDir = ApplicationConstants.LOG_DIR_EXPANSION_VAR;
    launcher.setEnv("LOG_DIR", logDir);
    log.info("LOG_DIR set to {}", logDir);
    if (System.getenv(HADOOP_USER_NAME) != null) {
      launcher.setEnv(HADOOP_USER_NAME, System.getenv(HADOOP_USER_NAME));
    }
    //add english env
    launcher.setEnv("LANG", "en_US.UTF-8");
    launcher.setEnv("LC_ALL", "en_US.UTF-8");
    launcher.setEnv("LANGUAGE", "en_US.UTF-8");

    //local resources
    providerUtils.localizePackages(launcher, fileSystem, appConf, roleGroup,
        getClusterName());

    if (SliderUtils.isHadoopClusterSecure(getConfig())) {
      providerUtils.localizeServiceKeytabs(launcher, instanceDefinition,
          fileSystem, getClusterName());
    }

    if (providerUtils.areStoresRequested(appComponent)) {
      providerUtils.localizeContainerSecurityStores(launcher, container,
          roleName, fileSystem, instanceDefinition, appComponent, getClusterName());
    }

    if (appComponent.getOptionBool(AM_CONFIG_GENERATION, false)) {
      // build and localize configuration files
      Map<String, Map<String, String>> configurations =
          providerUtils.buildConfigurations(
              instanceDefinition.getAppConfOperations(),
              instanceDefinition.getInternalOperations(),
              container.getId().toString(), getClusterName(),
              roleName, roleGroup, getAmState());
      providerUtils.localizeConfigFiles(launcher, roleName, roleGroup,
          appConf, configurations, launcher.getEnv(), fileSystem,
          getClusterName());
    }

    //add the configuration resources
    launcher.addLocalResources(fileSystem.submitDirectory(
        generatedConfPath,
        PROPAGATED_CONF_DIR_NAME));

    CommandLineBuilder operation = new CommandLineBuilder();
    operation.add(appConf.getComponentOpt(roleGroup, DOCKER_START_COMMAND,
        "/bin/bash"));

    operation.add("> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/"
        + OUT_FILE + " 2>" + ERR_FILE);

    launcher.addCommand(operation.build());

    // Additional files to localize
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
  }

  @Override
  public void initializeApplicationConfiguration(
      AggregateConf instanceDefinition, SliderFileSystem fileSystem,
      String roleGroup)
      throws IOException, SliderException {
        this.fileSystem = fileSystem;
  }

  @Override
  public void applyInitialRegistryDefinitions(URL amWebURI,
      ServiceRecord serviceRecord)
      throws IOException {
    super.applyInitialRegistryDefinitions(amWebURI, serviceRecord);

    // identify client component
    String clientName = null;
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    for (String component : appConf.getComponentNames()) {
      if (COMPONENT_TYPE_CLIENT.equals(appConf.getComponentOpt(component,
          COMPONENT_TYPE_KEY, null))) {
        clientName = component;
        break;
      }
    }
    if (clientName == null) {
      log.info("No client component specified, not publishing client configs");
      return;
    }

    // register AM-generated client configs
    // appConf should already be resolved!
    MapOperations clientOperations = appConf.getComponent(clientName);
    if (!clientOperations.getOptionBool(AM_CONFIG_GENERATION, false)) {
      log.info("AM config generation is false, not publishing client configs");
      return;
    }

    // build and localize configuration files
    Map<String, Map<String, String>> configurations =
        providerUtils.buildConfigurations(appConf, getAmState()
            .getInternalsSnapshot(), null, getClusterName(), clientName,
            clientName, getAmState());

    for (String configFileDN : configurations.keySet()) {
      String configFileName = appConf.getComponentOpt(clientName,
          OptionKeys.CONF_FILE_PREFIX + configFileDN + OptionKeys
              .NAME_SUFFIX, null);
      String configFileType = appConf.getComponentOpt(clientName,
          OptionKeys.CONF_FILE_PREFIX + configFileDN + OptionKeys
              .TYPE_SUFFIX, null);
      if (configFileName == null && configFileType == null) {
        continue;
      }
      ConfigFormat configFormat = ConfigFormat.resolve(configFileType);

      Map<String, String> config = configurations.get(configFileDN);
      ConfigUtils.prepConfigForTemplateOutputter(configFormat, config,
          fileSystem, getClusterName(),
          new File(configFileName).getName());
      providerUtils.publishApplicationInstanceData(configFileDN, configFileDN,
          config.entrySet(), getAmState());
    }
  }

  @Override
  public boolean processContainerStatus(ContainerId containerId,
      ContainerStatus status) {
    log.debug("Handling container status: {}", status);
    if (SliderUtils.isEmpty(status.getIPs()) ||
        SliderUtils.isUnset(status.getHost())) {
      return true;
    }
    RoleInstance instance = getAmState().getOwnedContainer(containerId);
    if (instance == null) {
      // container is completed?
      return false;
    }

    String roleName = instance.role;
    String roleGroup = instance.group;
    String containerIdStr = containerId.toString();

    providerUtils.updateServiceRecord(getAmState(), yarnRegistry,
        containerIdStr, roleName, status.getIPs(), status.getHost());

    publishExportGroups(containerIdStr, roleName, roleGroup,
        status.getHost());
    return false;
  }

  /**
   * This method looks for configuration properties of the form
   * export.key,value and publishes the key,value pair. Standard tokens are
   * substituted into the value, and COMPONENTNAME_HOST and THIS_HOST tokens
   * are substituted with the actual hostnames of the containers.
   */
  protected void publishExportGroups(String containerId,
      String roleName, String roleGroup, String thisHost) {
    ConfTreeOperations appConf = getAmState().getAppConfSnapshot();
    ConfTreeOperations internalsConf = getAmState().getInternalsSnapshot();

    Map<String, String> exports = providerUtils.getExports(
        getAmState().getAppConfSnapshot(), roleGroup);

    String hostKeyFormat = "${%s_HOST}";
    String hostNameKeyFormat = "${%s_HOSTNAME}";
    String ipKeyFormat = "${%s_IP}";

    // publish export groups if any
    Map<String, String> standardTokens = providerUtils.getStandardTokenMap(
        appConf, internalsConf, roleName, roleGroup, containerId,
        getClusterName());
    Map<String, String> replaceTokens = providerUtils.filterSiteOptions(
            appConf.getComponent(roleGroup).options, standardTokens);
    replaceTokens.putAll(standardTokens);

    String rolePrefix = appConf.getComponentOpt(roleGroup, ROLE_PREFIX, "");
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getAmState().getRoleClusterNodeMapping().entrySet()) {
      String otherRolePrefix = appConf.getComponentOpt(entry.getKey(),
          ROLE_PREFIX, "");
      if (!otherRolePrefix.equals(rolePrefix)) {
        // hostname replacements are only made within role prefix groups
        continue;
      }
      String key = entry.getKey();
      if (!rolePrefix.isEmpty()) {
        if (!key.startsWith(rolePrefix)) {
          log.warn("Something went wrong, {} doesn't start with {}", key,
              rolePrefix);
          continue;
        }
        key = key.substring(rolePrefix.length());
      }
      key = key.toUpperCase(Locale.ENGLISH);
      String host = providerUtils.getHostsList(
          entry.getValue().values(), true).iterator().next();
      replaceTokens.put(String.format(hostKeyFormat, key), host);
      String hostName = providerUtils.getHostNamesList(
          entry.getValue().values()).iterator().next();
      replaceTokens.put(String.format(hostNameKeyFormat, key), hostName);
      String ip = providerUtils.getIPsList(
          entry.getValue().values()).iterator().next();
      replaceTokens.put(String.format(ipKeyFormat, key), ip);
    }
    replaceTokens.put("${THIS_HOST}", thisHost);

    Map<String, List<ExportEntry>> entries = new HashMap<>();
    for (Entry<String, String> export : exports.entrySet()) {
      String value = export.getValue();
      // replace host names and site properties
      for (String token : replaceTokens.keySet()) {
        if (value.contains(token)) {
          value = value.replaceAll(Pattern.quote(token), replaceTokens.get(token));
        }
      }
      ExportEntry entry = new ExportEntry();
      entry.setLevel(APPLICATION_TAG);
      entry.setValue(value);
      entry.setUpdatedTime(new Date().toString());
      // over-write, app exports are singletons
      entries.put(export.getKey(), new ArrayList(Arrays.asList(entry)));
      log.info("Preparing to publish. Key {} and Value {}",
          export.getKey(), value);
    }
    if (!entries.isEmpty()) {
      providerUtils.publishExportGroup(entries, getAmState(), EXPORT_GROUP);
    }
  }

  @Override
  public Map<String, MonitorDetail> buildMonitorDetails(ClusterDescription clusterDesc) {
    Map<String, MonitorDetail> details = super.buildMonitorDetails(clusterDesc);
    buildRoleHostDetails(details);
    return details;
  }

  private void buildRoleHostDetails(Map<String, MonitorDetail> details) {
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        getAmState().getRoleClusterNodeMapping().entrySet()) {
      details.put(entry.getKey() + " Host(s)/Container(s)",
          new MonitorDetail(providerUtils.getHostsList(
              entry.getValue().values(), false).toString(), false));
    }
  }
}
