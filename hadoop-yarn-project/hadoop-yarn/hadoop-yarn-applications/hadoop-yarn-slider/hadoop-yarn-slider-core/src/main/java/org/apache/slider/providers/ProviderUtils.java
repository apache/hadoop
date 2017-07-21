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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.slider.api.ClusterNode;
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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.slider.api.ServiceApiConstants.*;
import static org.apache.slider.util.ServiceApiUtil.$;

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
   * @param providerClass provider to add
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

  public static String substituteStrWithTokens(String content,
      Map<String, String> tokensForSubstitution) {
    for (Map.Entry<String, String> token : tokensForSubstitution.entrySet()) {
      content =
          content.replaceAll(Pattern.quote(token.getKey()), token.getValue());
    }
    return content;
  }

  // configs will be substituted by corresponding env in tokenMap
  public void substituteMapWithTokens(Map<String, String> configs,
      Map<String, String> tokenMap) {
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      String value = entry.getValue();
      if (tokenMap != null) {
        for (Map.Entry<String, String> token : tokenMap.entrySet()) {
          value =
              value.replaceAll(Pattern.quote(token.getKey()), token.getValue());
        }
      }
      entry.setValue(value);
    }
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
  public synchronized void createConfigFileAndAddLocalResource(
      ContainerLauncher launcher, SliderFileSystem fs, Component component,
      Map<String, String> tokensForSubstitution, RoleInstance roleInstance,
      StateAccessForProviders appState) throws IOException {
    Path compDir =
        new Path(new Path(fs.getAppDir(), "components"), component.getName());
    Path compInstanceDir =
        new Path(compDir, roleInstance.getCompInstanceName());
    if (!fs.getFileSystem().exists(compInstanceDir)) {
      fs.getFileSystem().mkdirs(compInstanceDir,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
      roleInstance.compInstanceDir = compInstanceDir;
      log.info("Creating component instance dir: " + compInstanceDir);
    } else {
      log.info("Component instance conf dir already exists: " + compInstanceDir);
    }

    log.info("Tokens substitution for component: " + roleInstance
        .getCompInstanceName() + System.lineSeparator()
        + tokensForSubstitution);

    for (ConfigFile originalFile : component.getConfiguration().getFiles()) {
      ConfigFile configFile = originalFile.copy();
      String fileName = new Path(configFile.getDestFile()).getName();

      // substitute file name
      for (Map.Entry<String, String> token : tokensForSubstitution.entrySet()) {
        configFile.setDestFile(configFile.getDestFile()
            .replaceAll(Pattern.quote(token.getKey()), token.getValue()));
      }

      Path remoteFile = new Path(compInstanceDir, fileName);
      if (!fs.getFileSystem().exists(remoteFile)) {
        log.info("Saving config file on hdfs for component " + roleInstance
            .getCompInstanceName() + ": " + configFile);

        if (configFile.getSrcFile() != null) {
          // Load config file template
          switch (configFile.getType()) {
          case HADOOP_XML:
            // Hadoop_xml_template
            resolveHadoopXmlTemplateAndSaveOnHdfs(fs.getFileSystem(),
                tokensForSubstitution, configFile, remoteFile, appState);
            break;
          case TEMPLATE:
            // plain-template
            resolvePlainTemplateAndSaveOnHdfs(fs.getFileSystem(),
                tokensForSubstitution, configFile, remoteFile, appState);
            break;
          default:
            log.info("Not supporting loading src_file for " + configFile);
            break;
          }
        } else {
          // non-template
          resolveNonTemplateConfigsAndSaveOnHdfs(fs, tokensForSubstitution,
              roleInstance, configFile, fileName, remoteFile);
        }
      }

      // Add resource for localization
      LocalResource configResource =
          fs.createAmResource(remoteFile, LocalResourceType.FILE);
      File destFile = new File(configFile.getDestFile());
      String symlink = APP_CONF_DIR + "/" + fileName;
      if (destFile.isAbsolute()) {
        launcher.addLocalResource(symlink, configResource,
            configFile.getDestFile());
        log.info("Add config file for localization: " + symlink + " -> "
            + configResource.getResource().getFile() + ", dest mount path: "
            + configFile.getDestFile());
      } else {
        launcher.addLocalResource(symlink, configResource);
        log.info("Add config file for localization: " + symlink + " -> "
            + configResource.getResource().getFile());
      }
    }
  }

  private void resolveNonTemplateConfigsAndSaveOnHdfs(SliderFileSystem fs,
      Map<String, String> tokensForSubstitution, RoleInstance roleInstance,
      ConfigFile configFile, String fileName, Path remoteFile)
      throws IOException {
    // substitute non-template configs
    substituteMapWithTokens(configFile.getProps(), tokensForSubstitution);

    // write configs onto hdfs
    PublishedConfiguration publishedConfiguration =
        new PublishedConfiguration(fileName,
            configFile.getProps().entrySet());
    if (!fs.getFileSystem().exists(remoteFile)) {
      PublishedConfigurationOutputter configurationOutputter =
          PublishedConfigurationOutputter.createOutputter(
              ConfigFormat.resolve(configFile.getType().toString()),
              publishedConfiguration);
      try (FSDataOutputStream os = fs.getFileSystem().create(remoteFile)) {
        configurationOutputter.save(os);
        os.flush();
      }
    } else {
      log.info("Component instance = " + roleInstance.getCompInstanceName()
              + ", config file already exists: " + remoteFile);
    }
  }

  // 1. substitute config template - only handle hadoop_xml format
  // 2. save on hdfs
  @SuppressWarnings("unchecked")
  private void resolveHadoopXmlTemplateAndSaveOnHdfs(FileSystem fs,
      Map<String, String> tokensForSubstitution, ConfigFile configFile,
      Path remoteFile, StateAccessForProviders appState) throws IOException {
    Map<String, String> conf;
    try {
      conf = (Map<String, String>) appState.getConfigFileCache()
          .get(configFile);
    } catch (ExecutionException e) {
      log.info("Failed to load config file: " + configFile, e);
      return;
    }
    // make a copy for substitution
    org.apache.hadoop.conf.Configuration confCopy =
        new org.apache.hadoop.conf.Configuration(false);
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      confCopy.set(entry.getKey(), entry.getValue());
    }
    // substitute properties
    for (Map.Entry<String, String> entry : configFile.getProps().entrySet()) {
      confCopy.set(entry.getKey(), entry.getValue());
    }
    // substitute env variables
    for (Map.Entry<String, String> entry : confCopy) {
      String val = entry.getValue();
      if (val != null) {
        for (Map.Entry<String, String> token : tokensForSubstitution
            .entrySet()) {
          val = val.replaceAll(Pattern.quote(token.getKey()), token.getValue());
          confCopy.set(entry.getKey(), val);
        }
      }
    }
    // save on hdfs
    try (OutputStream output = fs.create(remoteFile)) {
      confCopy.writeXml(output);
      log.info("Reading config from: " + configFile.getSrcFile()
          + ", writing to: " + remoteFile);
    }
  }

  // 1) read the template as a string
  // 2) do token substitution
  // 3) save on hdfs
  private void resolvePlainTemplateAndSaveOnHdfs(FileSystem fs,
      Map<String, String> tokensForSubstitution, ConfigFile configFile,
      Path remoteFile, StateAccessForProviders appState) {
    String content;
    try {
      content = (String) appState.getConfigFileCache().get(configFile);
    } catch (ExecutionException e) {
      log.info("Failed to load config file: " + configFile, e);
      return;
    }
    // substitute tokens
    content = substituteStrWithTokens(content, tokensForSubstitution);

    try (OutputStream output = fs.create(remoteFile)) {
      org.apache.commons.io.IOUtils.write(content, output);
    } catch (IOException e) {
      log.info("Failed to create " + remoteFile);
    }
  }

  /**
   * Get initial component token map to be substituted into config values.
   * @param roleInstance role instance
   * @return tokens to replace
   */
  public Map<String, String> initCompTokensForSubstitute(
      RoleInstance roleInstance) {
    Map<String, String> tokens = new HashMap<>();
    tokens.put(COMPONENT_NAME, roleInstance.role);
    tokens.put(COMPONENT_NAME_LC, roleInstance.role.toLowerCase());
    tokens.put(COMPONENT_INSTANCE_NAME, roleInstance.getCompInstanceName());
    tokens.put(CONTAINER_ID, roleInstance.getContainerId().toString());
    tokens.put(COMPONENT_ID, String.valueOf(roleInstance.componentId));
    return tokens;
  }

  /**
   * Add ROLE_HOST tokens for substitution into config values.
   * @param tokens existing tokens
   * @param amState access to AM state
   */
  public void addComponentHostTokens(Map<String, String> tokens,
      StateAccessForProviders amState) {
    if (amState == null) {
      return;
    }
    for (Map.Entry<String, Map<String, ClusterNode>> entry :
        amState.getRoleClusterNodeMapping().entrySet()) {
      String tokenName = entry.getKey().toUpperCase(Locale.ENGLISH) + "_HOST";
      String hosts = StringUtils .join(",",
          getHostsList(entry.getValue().values(), true));
      tokens.put($(tokenName), hosts);
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
      String containerId, String roleName, List<String> ip, String hostname)
      throws IOException {
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
        record.description = role.getCompInstanceName();
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
    }
  }
}
