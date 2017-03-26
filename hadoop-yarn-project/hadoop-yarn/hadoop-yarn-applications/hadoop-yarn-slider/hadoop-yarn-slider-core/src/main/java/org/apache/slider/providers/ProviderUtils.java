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

package org.apache.slider.providers;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.resource.Application;
import org.apache.slider.api.resource.Component;
import org.apache.slider.api.resource.ConfigFile;
import org.apache.slider.api.resource.Configuration;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.services.yarnregistry.YarnRegistryViewForProviders;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This is a factoring out of methods handy for providers. It's bonded to a log
 * at construction time.
 */
public class ProviderUtils implements RoleKeys, SliderKeys {

  protected final Logger log;

  /**
   * Create an instance
   * @param log log directory to use -usually the provider
   */
  
  public ProviderUtils(Logger log) {
    this.log = log;
  }

  /**
   * Add oneself to the classpath. This does not work
   * on minicluster test runs where the JAR is not built up.
   * @param providerResources map of provider resources to add these entries to
   * @param provider provider to add
   * @param jarName name of the jar to use
   * @param sliderFileSystem target filesystem
   * @param tempPath path in the cluster FS for temp files
   * @param libdir relative directory to place resources
   * @param miniClusterTestRun true if minicluster is being used
   * @return true if the class was found in a JAR
   * 
   * @throws FileNotFoundException if the JAR was not found and this is NOT
   * a mini cluster test run
   * @throws IOException IO problems
   * @throws SliderException any Slider problem
   */
  public static boolean addProviderJar(
      Map<String, LocalResource> providerResources,
      Class providerClass,
      String jarName,
      SliderFileSystem sliderFileSystem,
      Path tempPath,
      String libdir,
      boolean miniClusterTestRun) throws
      IOException,
      SliderException {
    try {
      SliderUtils.putJar(providerResources,
          sliderFileSystem,
          providerClass,
          tempPath,
          libdir,
          jarName);
      return true;
    } catch (FileNotFoundException e) {
      if (miniClusterTestRun) {
        return false;
      } else {
        throw e;
      }
    }
  }
  
  /**
   * Loads all dependency jars from the default path.
   * @param providerResources map of provider resources to add these entries to
   * @param sliderFileSystem target filesystem
   * @param tempPath path in the cluster FS for temp files
   * @param libDir relative directory to place resources
   * @param libLocalSrcDir explicitly supplied local libs dir
   * @throws IOException trouble copying to HDFS
   * @throws SliderException trouble copying to HDFS
   */
  public static void addAllDependencyJars(
      Map<String, LocalResource> providerResources,
      SliderFileSystem sliderFileSystem,
      Path tempPath,
      String libDir,
      String libLocalSrcDir)
      throws IOException, SliderException {
    if (SliderUtils.isSet(libLocalSrcDir)) {
      File file = new File(libLocalSrcDir);
      if (!file.exists() || !file.isDirectory()) {
        throw new BadCommandArgumentsException(
            "Supplied lib src dir %s is not valid", libLocalSrcDir);
      }
    }
    SliderUtils.putAllJars(providerResources, sliderFileSystem, tempPath,
        libDir, libLocalSrcDir);
  }

  // Build key -> value map
  // value will be substituted by corresponding data in tokenMap
  public Map<String, String> substituteConfigs(Map<String, String> configs,
      Map<String, String> tokenMap) {
    String format = "${%s}";
    Map<String, String> filteredOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (tokenMap != null) {
        for (Map.Entry<String, String> token : tokenMap.entrySet()) {
          value =
              value.replaceAll(Pattern.quote(token.getKey()), token.getValue());
        }
      }
      filteredOptions.put(String.format(format, key), value);
    }

    return filteredOptions;
  }

  /**
   * Get resource requirements from a String value. If value isn't specified,
   * use the default value. If value is greater than max, use the max value.
   * @param val string value
   * @param defVal default value
   * @param maxVal maximum value
   * @return int resource requirement
   */
  public int getRoleResourceRequirement(String val,
                                        int defVal,
                                        int maxVal) {
    if (val==null) {
      val = Integer.toString(defVal);
    }
    Integer intVal;
    if (ResourceKeys.YARN_RESOURCE_MAX.equals(val)) {
      intVal = maxVal;
    } else {
      intVal = Integer.decode(val);
    }
    return intVal;
  }


  /**
   * Localize the service keytabs for the application.
   * @param launcher container launcher
   * @param fileSystem file system
   * @throws IOException trouble uploading to HDFS
   */
  public void localizeServiceKeytabs(ContainerLauncher launcher,
      SliderFileSystem fileSystem, Application application) throws IOException {

    Configuration conf = application.getConfiguration();
    String keytabPathOnHost =
        conf.getProperty(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
    if (SliderUtils.isUnset(keytabPathOnHost)) {
      String amKeytabName =
          conf.getProperty(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
      String keytabDir =
          conf.getProperty(SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR);
      // we need to localize the keytab files in the directory
      Path keytabDirPath = fileSystem.buildKeytabPath(keytabDir, null,
          application.getName());
      boolean serviceKeytabsDeployed = false;
      if (fileSystem.getFileSystem().exists(keytabDirPath)) {
        FileStatus[] keytabs = fileSystem.getFileSystem().listStatus(
            keytabDirPath);
        LocalResource keytabRes;
        for (FileStatus keytab : keytabs) {
          if (!amKeytabName.equals(keytab.getPath().getName())
              && keytab.getPath().getName().endsWith(".keytab")) {
            serviceKeytabsDeployed = true;
            log.info("Localizing keytab {}", keytab.getPath().getName());
            keytabRes = fileSystem.createAmResource(keytab.getPath(),
                LocalResourceType.FILE);
            launcher.addLocalResource(KEYTAB_DIR + "/" +
                    keytab.getPath().getName(),
                keytabRes);
          }
        }
      }
      if (!serviceKeytabsDeployed) {
        log.warn("No service keytabs for the application have been localized.  "
            + "If the application requires keytabs for secure operation, "
            + "please ensure that the required keytabs have been uploaded "
            + "to the folder {}", keytabDirPath);
      }
    }
  }


  // 1. Create all config files for a component on hdfs for localization
  // 2. Add the config file to localResource
  //TODO handle Template format config file
  public void createConfigFileAndAddLocalResource(ContainerLauncher launcher,
      SliderFileSystem fs, Component component,
      Map<String, String> tokensForSubstitution,
      StateAccessForProviders amState) throws IOException {
    Path compDir =
        new Path(new Path(fs.getAppDir(), "components"), component.getName());
    if (!fs.getFileSystem().exists(compDir)) {
      fs.getFileSystem().mkdirs(compDir,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      log.info("Creating component dir: " + compDir);
    } else {
      log.info("Component conf dir already exists: " + compDir);
      return;
    }

    for (ConfigFile configFile : component.getConfiguration().getFiles()) {
      String fileName = configFile.getSrcFile();
      // substitute file name
      for (Map.Entry<String, String> token : tokensForSubstitution.entrySet()) {
        configFile.setDestFile(configFile.getDestFile()
            .replaceAll(Pattern.quote(token.getKey()), token.getValue()));
      }
      // substitute configs
      substituteConfigs(configFile.getProps(), tokensForSubstitution);

      // write configs onto hdfs
      PublishedConfiguration publishedConfiguration =
          new PublishedConfiguration(fileName,
              configFile.getProps().entrySet());
      Path remoteFile = new Path(compDir, fileName);
      if (!fs.getFileSystem().exists(remoteFile)) {
        synchronized (this) {
          if (!fs.getFileSystem().exists(remoteFile)) {
            PublishedConfigurationOutputter configurationOutputter =
                PublishedConfigurationOutputter.createOutputter(
                    ConfigFormat.resolve(configFile.getType().toString()),
                    publishedConfiguration);
            FSDataOutputStream os = null;
            try {
              os = fs.getFileSystem().create(remoteFile);
              configurationOutputter.save(os);
              os.flush();
              log.info("Created config file on hdfs: " + remoteFile);
            } finally {
              IOUtils.closeStream(os);
            }
          }
        }
      }

      // Publish configs
      amState.getPublishedSliderConfigurations()
          .put(configFile.getSrcFile(), publishedConfiguration);

      // Add resource for localization
      LocalResource configResource =
          fs.createAmResource(remoteFile, LocalResourceType.FILE);
      File destFile = new File(configFile.getDestFile());
      //TODO why to we need to differetiate  RESOURCE_DIR vs APP_CONF_DIR
      if (destFile.isAbsolute()) {
        String symlink = RESOURCE_DIR + "/" + fileName;
        launcher.addLocalResource(symlink, configResource,
            configFile.getDestFile());
        log.info("Add config file for localization: " + symlink + " -> "
            + configResource.getResource().getFile() + ", dest mount path: "
            + configFile.getDestFile());
      } else {
        String symlink = APP_CONF_DIR + "/" + fileName;
        launcher.addLocalResource(symlink, configResource);
        log.info("Add config file for localization: " + symlink + " -> "
            + configResource.getResource().getFile());
      }
    }
  }

  /**
   * Get initial token map to be substituted into config values.
   * @param appConf app configurations
   * @param componentName component name
   * @param componentGroup component group
   * @param containerId container ID
   * @param clusterName app name
   * @return tokens to replace
   */
  public Map<String, String> getStandardTokenMap(
      Configuration appConf, Configuration componentConf, String componentName,
      String componentGroup, String containerId, String clusterName) {

    Map<String, String> tokens = new HashMap<>();
    if (containerId != null) {
      tokens.put("${CONTAINER_ID}", containerId);
    }
    String nnuri = appConf.getProperty("fs.defaultFS");
    if (nnuri != null && !nnuri.isEmpty()) {
      tokens.put("${NN_URI}", nnuri);
      tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    }
    tokens.put("${ZK_HOST}", appConf.getProperty(OptionKeys.ZOOKEEPER_HOSTS));
    tokens.put("${DEFAULT_ZK_PATH}", appConf.getProperty(OptionKeys.ZOOKEEPER_PATH));
    String prefix = componentConf.getProperty(ROLE_PREFIX);
    String dataDirSuffix = "";
    if (prefix == null) {
      prefix = "";
    } else {
      dataDirSuffix = "_" + SliderUtils.trimPrefix(prefix);
    }
    tokens.put("${DEFAULT_DATA_DIR}",
        appConf.getProperty(InternalKeys.INTERNAL_DATA_DIR_PATH)
            + dataDirSuffix);
    tokens.put("${JAVA_HOME}", appConf.getProperty(JAVA_HOME));
    tokens.put("${COMPONENT_NAME}", componentName);
    tokens.put("${COMPONENT_NAME.lc}", componentName.toLowerCase());
    tokens.put("${COMPONENT_PREFIX}", prefix);
    tokens.put("${COMPONENT_PREFIX.lc}", prefix.toLowerCase());
    if (!componentName.equals(componentGroup) &&
        componentName.startsWith(componentGroup)) {
      tokens.put("${COMPONENT_ID}",
          componentName.substring(componentGroup.length()));
    }
    if (clusterName != null) {
      tokens.put("${CLUSTER_NAME}", clusterName);
      tokens.put("${CLUSTER_NAME.lc}", clusterName.toLowerCase());
      tokens.put("${APP_NAME}", clusterName);
      tokens.put("${APP_NAME.lc}", clusterName.toLowerCase());
    }
    tokens.put("${APP_COMPONENT_NAME}", componentName);
    tokens.put("${APP_COMPONENT_NAME.lc}", componentName.toLowerCase());
    return tokens;
  }

  /**
   * Add ROLE_HOST tokens for substitution into config values.
   * @param tokens existing tokens
   * @param amState access to AM state
   */
  public void addRoleHostTokens(Map<String, String> tokens,
      StateAccessForProviders amState) {
    if (amState == null) {
      return;
    }
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        amState.getRoleClusterNodeMapping().entrySet()) {
      String tokenName = entry.getKey().toUpperCase(Locale.ENGLISH) + "_HOST";
      String hosts = StringUtils .join(",",
          getHostsList(entry.getValue().values(), true));
      tokens.put("${" + tokenName + "}", hosts);
    }
  }

  /**
   * Return a list of hosts based on current ClusterNodes.
   * @param values cluster nodes
   * @param hostOnly whether host or host/server name will be added to list
   * @return list of hosts
   */
  public Iterable<String> getHostsList(Collection<ClusterNode> values,
      boolean hostOnly) {
    List<String> hosts = new ArrayList<>();
    for (ClusterNode cn : values) {
      hosts.add(hostOnly ? cn.host : cn.host + "/" + cn.name);
    }
    return hosts;
  }

  /**
   * Update ServiceRecord in Registry with IP and hostname.
   * @param amState access to AM state
   * @param yarnRegistry acces to YARN registry
   * @param containerId container ID
   * @param roleName component name
   * @param ip list of IPs
   * @param hostname hostname
   */
  public void updateServiceRecord(StateAccessForProviders amState,
      YarnRegistryViewForProviders yarnRegistry,
      String containerId, String roleName, List<String> ip, String hostname) {
    try {
      RoleInstance role = null;
      if(ip != null && !ip.isEmpty()){
        role = amState.getOwnedContainer(containerId);
        role.ip = ip.get(0);
      }
      if(hostname != null && !hostname.isEmpty()){
        role = amState.getOwnedContainer(containerId);
        role.hostname = hostname;
      }
      if (role != null) {
        // create and publish updated service record (including hostname & ip)
        ServiceRecord record = new ServiceRecord();
        record.set(YarnRegistryAttributes.YARN_ID, containerId);
        record.description = roleName.replaceAll("_", "-");
        record.set(YarnRegistryAttributes.YARN_PERSISTENCE,
            PersistencePolicies.CONTAINER);
        // TODO: use constants from YarnRegistryAttributes
        if (role.ip != null) {
          record.set("yarn:ip", role.ip);
        }
        if (role.hostname != null) {
          record.set("yarn:hostname", role.hostname);
        }
        yarnRegistry.putComponent(
            RegistryPathUtils.encodeYarnID(containerId), record);
      }
    } catch (NoSuchNodeException e) {
      // ignore - there is nothing to do if we don't find a container
      log.warn("Owned container {} not found - {}", containerId, e);
    } catch (IOException e) {
      log.warn("Error updating container {} service record in registry",
          containerId, e);
    }
  }
}
