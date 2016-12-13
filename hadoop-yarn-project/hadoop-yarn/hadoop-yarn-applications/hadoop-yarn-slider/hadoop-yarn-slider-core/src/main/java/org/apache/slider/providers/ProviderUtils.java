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

import org.apache.commons.io.FileUtils;
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
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.common.tools.SliderFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ContainerLauncher;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.ConfigUtils;
import org.apache.slider.core.registry.docstore.ExportEntry;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.core.registry.docstore.PublishedExports;
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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
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
      Object provider,
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
          provider.getClass(),
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
   * Add/overwrite the agent tarball (overwritten every time application is
   * restarted).
   * @param provider an instance of a provider class
   * @param tarName name of the tarball to upload
   * @param sliderFileSystem the file system
   * @param agentDir directory to upload to
   * @return true the location could be determined and the file added
   * @throws IOException if the upload fails
   */
  public static boolean addAgentTar(Object provider,
                                    String tarName,
                                    SliderFileSystem sliderFileSystem,
                                    Path agentDir) throws
  IOException {
    File localFile = SliderUtils.findContainingJar(provider.getClass());
    if(localFile != null) {
      String parentDir = localFile.getParent();
      Path agentTarPath = new Path(parentDir, tarName);
      sliderFileSystem.getFileSystem().copyFromLocalFile(false, true,
          agentTarPath, agentDir);
      return true;
    }
    return false;
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


  /**
   * Validate the requested number of instances of a component.
   * <p>
   * If max &lt;= 0:  min &lt;= count
   * If max &gt; 0:  min &lt;= count &lt;= max
   * @param instanceDescription configuration
   * @param name node class name
   * @param min requested heap size
   * @param max maximum value.
   * @throws BadCommandArgumentsException if the values are out of range
   */
  public void validateNodeCount(AggregateConf instanceDescription,
                                String name, int min, int max)
      throws BadCommandArgumentsException {
    MapOperations component =
        instanceDescription.getResourceOperations().getComponent(name);
    int count;
    if (component == null) {
      count = 0;
    } else {
      count = component.getOptionInt(ResourceKeys.COMPONENT_INSTANCES, 0);
    }
    validateNodeCount(name, count, min, max);
  }
  
  /**
   * Validate the count is between min and max.
   * <p>
   * If max &lt;= 0:  min &lt;= count
   * If max &gt; 0:  min &lt;= count &lt;= max
   * @param name node class name
   * @param count requested node count
   * @param min requested heap size
   * @param max maximum value. 
   * @throws BadCommandArgumentsException if the values are out of range
   */
  public void validateNodeCount(String name,
                                int count,
                                int min,
                                int max) throws BadCommandArgumentsException {
    if (count < min) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is below the minimum of %d", name, count,
        min);
    }
    if (max > 0 && count > max) {
      throw new BadCommandArgumentsException(
        "requested no of %s nodes: %d is above the maximum of %d", name, count,
        max);
    }
  }

  /**
   * Copy options beginning with "site.configName." prefix from options map
   * to sitexml map, removing the prefix and substituting the tokens
   * specified in the tokenMap.
   * @param options source map
   * @param sitexml destination map
   * @param configName optional ".configName" portion of the prefix
   * @param tokenMap key/value pairs to substitute into the option values
   */
  public void propagateSiteOptions(Map<String, String> options,
      Map<String, String> sitexml,
      String configName,
      Map<String,String> tokenMap) {
    String prefix = OptionKeys.SITE_XML_PREFIX +
        (!configName.isEmpty() ? configName + "." : "");
    propagateOptions(options, sitexml, tokenMap, prefix);
  }

  /**
   * Copy options beginning with prefix from options map
   * to sitexml map, removing the prefix and substituting the tokens
   * specified in the tokenMap.
   * @param options source map
   * @param sitexml destination map
   * @param tokenMap key/value pairs to substitute into the option values
   * @param prefix which options to copy to destination map
   */
  public void propagateOptions(Map<String, String> options,
                                   Map<String, String> sitexml,
                                   Map<String,String> tokenMap,
                                   String prefix) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        String envName = key.substring(prefix.length());
        if (!envName.isEmpty()) {
          String value = entry.getValue();
          if (tokenMap != null) {
            for (Map.Entry<String,String> token : tokenMap.entrySet()) {
              value = value.replaceAll(Pattern.quote(token.getKey()),
                                       token.getValue());
            }
          }
          sitexml.put(envName, value);
        }
      }
    }
  }

  /**
   * Substitute tokens into option map values, returning a new map.
   * @param options source map
   * @param tokenMap key/value pairs to substitute into the option values
   * @return map with substituted values
   */
  public Map<String, String> filterSiteOptions(Map<String, String> options,
      Map<String, String> tokenMap) {
    String prefix = OptionKeys.SITE_XML_PREFIX;
    String format = "${%s}";
    Map<String, String> filteredOptions = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        String value = entry.getValue();
        if (tokenMap != null) {
          for (Map.Entry<String,String> token : tokenMap.entrySet()) {
            value = value.replaceAll(Pattern.quote(token.getKey()),
                token.getValue());
          }
        }
        filteredOptions.put(String.format(format, key), value);
      }
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
   * @param instanceDefinition app specification
   * @param fileSystem file system
   * @param clusterName app name
   * @throws IOException trouble uploading to HDFS
   */
  public void localizeServiceKeytabs(ContainerLauncher launcher,
      AggregateConf instanceDefinition, SliderFileSystem fileSystem,
      String clusterName) throws IOException {
    ConfTreeOperations appConf = instanceDefinition.getAppConfOperations();
    String keytabPathOnHost = appConf.getComponent(COMPONENT_AM).get(
            SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH);
    if (SliderUtils.isUnset(keytabPathOnHost)) {
      String amKeytabName = appConf.getComponent(COMPONENT_AM).get(
              SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME);
      String keytabDir = appConf.getComponent(COMPONENT_AM).get(
              SliderXmlConfKeys.KEY_HDFS_KEYTAB_DIR);
      // we need to localize the keytab files in the directory
      Path keytabDirPath = fileSystem.buildKeytabPath(keytabDir, null,
          clusterName);
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


  /**
   * Upload a local file to the cluster security dir in HDFS. If the file
   * already exists, it is not replaced.
   * @param resource file to upload
   * @param fileSystem file system
   * @param clusterName app name
   * @return Path of the uploaded file
   * @throws IOException file cannot be uploaded
   */
  private Path uploadSecurityResource(File resource,
      SliderFileSystem fileSystem, String clusterName) throws IOException {
    Path certsDir = fileSystem.buildClusterSecurityDirPath(clusterName);
    return uploadResource(resource, fileSystem, certsDir);
  }

  /**
   * Upload a local file to the cluster resources dir in HDFS. If the file
   * already exists, it is not replaced.
   * @param resource file to upload
   * @param fileSystem file system
   * @param roleName optional subdirectory (for component-specific resources)
   * @param clusterName app name
   * @return Path of the uploaded file
   * @throws IOException file cannot be uploaded
   */
  private Path uploadResource(File resource, SliderFileSystem fileSystem,
      String roleName, String clusterName) throws IOException {
    Path dir;
    if (roleName == null) {
      dir = fileSystem.buildClusterResourcePath(clusterName);
    } else {
      dir = fileSystem.buildClusterResourcePath(clusterName, roleName);
    }
    return uploadResource(resource, fileSystem, dir);
  }

  /**
   * Upload a local file to a specified HDFS directory. If the file already
   * exists, it is not replaced.
   * @param resource file to upload
   * @param fileSystem file system
   * @param parentDir destination directory in HDFS
   * @return Path of the uploaded file
   * @throws IOException file cannot be uploaded
   */
  private synchronized Path uploadResource(File resource,
      SliderFileSystem fileSystem, Path parentDir) throws IOException {
    if (!fileSystem.getFileSystem().exists(parentDir)) {
      fileSystem.getFileSystem().mkdirs(parentDir,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    }
    Path destPath = new Path(parentDir, resource.getName());
    if (!fileSystem.getFileSystem().exists(destPath)) {
      FSDataOutputStream os = null;
      try {
        os = fileSystem.getFileSystem().create(destPath);
        byte[] contents = FileUtils.readFileToByteArray(resource);
        os.write(contents, 0, contents.length);
        os.flush();
      } finally {
        IOUtils.closeStream(os);
      }
      log.info("Uploaded {} to localization path {}", resource, destPath);
    } else {
      log.info("Resource {} already existed at localization path {}", resource,
          destPath);
    }

    while (!fileSystem.getFileSystem().exists(destPath)) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        // ignore
      }
    }

    fileSystem.getFileSystem().setPermission(destPath,
        new FsPermission(FsAction.READ, FsAction.NONE, FsAction.NONE));

    return destPath;
  }

  /**
   * Write a configuration property map to a local file in a specified format.
   * @param fileSystem file system
   * @param file destination file
   * @param configFormat file format
   * @param configFileDN file description
   * @param config properties to save to the file
   * @param clusterName app name
   * @throws IOException file cannot be created
   */
  private synchronized void createConfigFile(SliderFileSystem fileSystem,
      File file, ConfigFormat configFormat, String configFileDN,
      Map<String, String> config, String clusterName) throws IOException {
    if (file.exists()) {
      log.info("Skipping writing {} file {} because it already exists",
          configFormat, file);
      return;
    }
    log.info("Writing {} file {}", configFormat, file);

    ConfigUtils.prepConfigForTemplateOutputter(configFormat, config,
        fileSystem, clusterName, file.getName());
    PublishedConfiguration publishedConfiguration =
        new PublishedConfiguration(configFileDN,
            config.entrySet());
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(configFormat,
            publishedConfiguration);
    configurationOutputter.save(file);
  }

  /**
   * Determine config files requested in the appConf, generate the files, and
   * localize them.
   * @param launcher container launcher
   * @param roleName component name
   * @param roleGroup component group
   * @param appConf app configurations
   * @param configs configurations grouped by config name
   * @param env environment variables
   * @param fileSystem file system
   * @param clusterName app name
   * @throws IOException file(s) cannot be uploaded
   * @throws BadConfigException file name not specified or file format not
   * supported
   */
  public void localizeConfigFiles(ContainerLauncher launcher,
      String roleName, String roleGroup,
      ConfTreeOperations appConf,
      Map<String, Map<String, String>> configs,
      MapOperations env,
      SliderFileSystem fileSystem,
      String clusterName)
      throws IOException, BadConfigException {
    for (Entry<String, Map<String, String>> configEntry : configs.entrySet()) {
      String configFileName = appConf.getComponentOpt(roleGroup,
          OptionKeys.CONF_FILE_PREFIX + configEntry.getKey() + OptionKeys
              .NAME_SUFFIX, null);
      String configFileType = appConf.getComponentOpt(roleGroup,
          OptionKeys.CONF_FILE_PREFIX + configEntry.getKey() + OptionKeys
              .TYPE_SUFFIX, null);
      if (configFileName == null && configFileType == null) {
        // config file not requested, so continue
        continue;
      }
      if (configFileName == null) {
        throw new BadConfigException("Config file name null for " +
            configEntry.getKey());
      }
      if (configFileType == null) {
        throw new BadConfigException("Config file type null for " +
            configEntry.getKey());
      }
      ConfigFormat configFormat = ConfigFormat.resolve(configFileType);
      if (configFormat == null) {
        throw new BadConfigException("Config format " + configFileType +
            " doesn't exist");
      }
      boolean perComponent = appConf.getComponentOptBool(roleGroup,
          OptionKeys.CONF_FILE_PREFIX + configEntry.getKey() + OptionKeys
              .PER_COMPONENT, false);
      boolean perGroup = appConf.getComponentOptBool(roleGroup,
          OptionKeys.CONF_FILE_PREFIX + configEntry.getKey() + OptionKeys
              .PER_GROUP, false);

      localizeConfigFile(launcher, roleName, roleGroup, configEntry.getKey(),
          configFormat, configFileName, configs, env, fileSystem,
          clusterName, perComponent, perGroup);
    }
  }

  /**
   * Create and localize a config file.
   * @param launcher container launcher
   * @param roleName component name
   * @param roleGroup component group
   * @param configFileDN config description/name
   * @param configFormat config format
   * @param configFileName config file name
   * @param configs configs grouped by config description/name
   * @param env environment variables
   * @param fileSystem file system
   * @param clusterName app name
   * @param perComponent true if file should be created per unique component
   * @param perGroup true if file should be created per component group
   * @throws IOException file cannot be uploaded
   */
  public void localizeConfigFile(ContainerLauncher launcher,
      String roleName, String roleGroup,
      String configFileDN, ConfigFormat configFormat, String configFileName,
      Map<String, Map<String, String>> configs,
      MapOperations env,
      SliderFileSystem fileSystem,
      String clusterName,
      boolean perComponent,
      boolean perGroup)
      throws IOException {
    if (launcher == null) {
      return;
    }
    Map<String, String> config = ConfigUtils.replacePropsInConfig(
        configs.get(configFileDN), env.options);
    String fileName = ConfigUtils.replaceProps(config, configFileName);
    File localFile = new File(RESOURCE_DIR);
    if (!localFile.exists()) {
      if (!localFile.mkdir() && !localFile.exists()) {
        throw new IOException(RESOURCE_DIR + " could not be created!");
      }
    }

    String folder = null;
    if (perComponent) {
      folder = roleName;
    } else if (perGroup) {
      folder = roleGroup;
    }
    if (folder != null) {
      localFile = new File(localFile, folder);
      if (!localFile.exists()) {
        if (!localFile.mkdir() && !localFile.exists()) {
          throw new IOException(localFile + " could not be created!");
        }
      }
    }
    localFile = new File(localFile, new File(fileName).getName());

    log.info("Localizing {} configs to config file {} (destination {}) " +
            "based on {} configs", config.size(), localFile, fileName,
        configFileDN);
    if (!localFile.exists()) {
      createConfigFile(fileSystem, localFile, configFormat, configFileDN,
          config, clusterName);
    } else {
      log.info("Local {} file {} already exists", configFormat, localFile);
    }
    Path destPath = uploadResource(localFile, fileSystem, folder, clusterName);
    LocalResource configResource = fileSystem.createAmResource(destPath,
        LocalResourceType.FILE);

    File destFile = new File(fileName);
    if (destFile.isAbsolute()) {
      launcher.addLocalResource(
          RESOURCE_DIR + "/" + destFile.getName(),
          configResource, fileName);
    } else {
      launcher.addLocalResource(APP_CONF_DIR + "/" + fileName,
          configResource);
    }
  }

  /**
   * Localize application tarballs and other resources requested by the app.
   * @param launcher container launcher
   * @param fileSystem file system
   * @param appConf app configurations
   * @param roleGroup component group
   * @param clusterName app name
   * @throws IOException resources cannot be uploaded
   * @throws BadConfigException package name or type is not specified
   */
  public void localizePackages(ContainerLauncher launcher,
      SliderFileSystem fileSystem, ConfTreeOperations appConf, String roleGroup,
      String clusterName) throws IOException, BadConfigException {
    for (Entry<String, Map<String, String>> pkg :
        getPackages(roleGroup, appConf).entrySet()) {
      String pkgName = pkg.getValue().get(OptionKeys.NAME_SUFFIX);
      String pkgType = pkg.getValue().get(OptionKeys.TYPE_SUFFIX);
      Path pkgPath = fileSystem.buildResourcePath(pkgName);
      if (!fileSystem.isFile(pkgPath)) {
        pkgPath = fileSystem.buildResourcePath(clusterName,
            pkgName);
      }
      if (!fileSystem.isFile(pkgPath)) {
        throw new IOException("Package doesn't exist as a resource: " +
            pkgName);
      }
      log.info("Adding resource {}", pkgName);
      LocalResourceType type = LocalResourceType.FILE;
      if ("archive".equals(pkgType)) {
        type = LocalResourceType.ARCHIVE;
      }
      LocalResource packageResource = fileSystem.createAmResource(
          pkgPath, type);
      launcher.addLocalResource(APP_PACKAGES_DIR, packageResource);
    }
  }

  /**
   * Build a map of configuration description/name to configuration key/value
   * properties, with all known tokens substituted into the property values.
   * @param appConf app configurations
   * @param internalsConf internal configurations
   * @param containerId container ID
   * @param roleName component name
   * @param roleGroup component group
   * @param amState access to AM state
   * @return configuration properties grouped by config description/name
   */
  public Map<String, Map<String, String>> buildConfigurations(
      ConfTreeOperations appConf, ConfTreeOperations internalsConf,
      String containerId, String clusterName, String roleName, String roleGroup,
      StateAccessForProviders amState) {

    Map<String, Map<String, String>> configurations = new TreeMap<>();
    Map<String, String> tokens = getStandardTokenMap(appConf,
        internalsConf, roleName, roleGroup, containerId, clusterName);

    Set<String> configs = new HashSet<>();
    configs.addAll(getApplicationConfigurationTypes(roleGroup, appConf));
    configs.addAll(getSystemConfigurationsRequested(appConf));

    for (String configType : configs) {
      addNamedConfiguration(configType, appConf.getGlobalOptions().options,
          configurations, tokens, amState);
      if (appConf.getComponent(roleGroup) != null) {
        addNamedConfiguration(configType,
            appConf.getComponent(roleGroup).options, configurations, tokens,
            amState);
      }
    }

    //do a final replacement of re-used configs
    dereferenceAllConfigs(configurations);

    return configurations;
  }

  /**
   * Substitute "site." prefixed configuration values into other configuration
   * values where needed. The format for these substitutions is that
   * {@literal ${@//site/configDN/key}} will be replaced by the value for the
   * "site.configDN.key" property.
   * @param configurations configuration properties grouped by config
   *                       description/name
   */
  public void dereferenceAllConfigs(
      Map<String, Map<String, String>> configurations) {
    Map<String, String> allConfigs = new HashMap<>();
    String lookupFormat = "${@//site/%s/%s}";
    for (String configType : configurations.keySet()) {
      Map<String, String> configBucket = configurations.get(configType);
      for (String configName : configBucket.keySet()) {
        allConfigs.put(String.format(lookupFormat, configType, configName),
            configBucket.get(configName));
      }
    }

    boolean finished = false;
    while (!finished) {
      finished = true;
      for (Map.Entry<String, String> entry : allConfigs.entrySet()) {
        String configValue = entry.getValue();
        for (Map.Entry<String, String> lookUpEntry : allConfigs.entrySet()) {
          String lookUpValue = lookUpEntry.getValue();
          if (lookUpValue.contains("${@//site/")) {
            continue;
          }
          String lookUpKey = lookUpEntry.getKey();
          if (configValue != null && configValue.contains(lookUpKey)) {
            configValue = configValue.replace(lookUpKey, lookUpValue);
          }
        }
        if (!configValue.equals(entry.getValue())) {
          finished = false;
          allConfigs.put(entry.getKey(), configValue);
        }
      }
    }

    for (String configType : configurations.keySet()) {
      Map<String, String> configBucket = configurations.get(configType);
      for (Map.Entry<String, String> entry: configBucket.entrySet()) {
        String configName = entry.getKey();
        String configValue = entry.getValue();
        for (Map.Entry<String, String> lookUpEntry : allConfigs.entrySet()) {
          String lookUpValue = lookUpEntry.getValue();
          if (lookUpValue.contains("${@//site/")) {
            continue;
          }
          String lookUpKey = lookUpEntry.getKey();
          if (configValue != null && configValue.contains(lookUpKey)) {
            configValue = configValue.replace(lookUpKey, lookUpValue);
          }
        }
        configBucket.put(configName, configValue);
      }
    }
  }

  /**
   * Return a set of configuration description/names represented in the app.
   * configuration
   * @param roleGroup component group
   * @param appConf app configurations
   * @return set of configuration description/names
   */
  public Set<String> getApplicationConfigurationTypes(String roleGroup,
      ConfTreeOperations appConf) {
    Set<String> configList = new HashSet<>();

    String prefix = OptionKeys.CONF_FILE_PREFIX;
    String suffix = OptionKeys.TYPE_SUFFIX;
    MapOperations component = appConf.getComponent(roleGroup);
    if (component != null) {
      addConfsToList(component, configList, prefix, suffix);
    }
    addConfsToList(appConf.getGlobalOptions(), configList, prefix, suffix);

    return configList;
  }

  /**
   * Finds all configuration description/names of the form
   * prefixconfigDNsuffix in the configuration (e.g. conf.configDN.type).
   * @param confMap configuration properties
   * @param confList set containing configuration description/names
   * @param prefix configuration key prefix to match
   * @param suffix configuration key suffix to match
   */
  private void addConfsToList(Map<String, String> confMap,
      Set<String> confList, String prefix, String suffix) {
    for (String key : confMap.keySet()) {
      if (key.startsWith(prefix) && key.endsWith(suffix)) {
        String confName = key.substring(prefix.length(),
            key.length() - suffix.length());
        if (!confName.isEmpty()) {
          confList.add(confName);
        }
      }
    }
  }

  /**
   * Build a map of package description/name to package key/value properties
   * (there should be two properties, type and name).
   * @param roleGroup component group
   * @param appConf app configurations
   * @return map of package description/name to package key/value properties
   * @throws BadConfigException package name or type is not specified
   */
  public Map<String, Map<String, String>> getPackages(String roleGroup,
      ConfTreeOperations appConf) throws BadConfigException {
    Map<String, Map<String, String>> packages = new HashMap<>();
    String prefix = OptionKeys.PKG_FILE_PREFIX;
    String typeSuffix = OptionKeys.TYPE_SUFFIX;
    String nameSuffix = OptionKeys.NAME_SUFFIX;
    MapOperations component = appConf.getComponent(roleGroup);
    if (component == null) {
      component = appConf.getGlobalOptions();
    }
    for (Map.Entry<String, String> entry : component.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(prefix)) {
        String confName;
        String type;
        if (key.endsWith(typeSuffix)) {
          confName = key.substring(prefix.length(), key.length() - typeSuffix.length());
          type = typeSuffix;
        } else if (key.endsWith(nameSuffix)) {
          confName = key.substring(prefix.length(), key.length() - nameSuffix.length());
          type = nameSuffix;
        } else {
          continue;
        }
        if (!packages.containsKey(confName)) {
          packages.put(confName, new HashMap<String, String>());
        }
        packages.get(confName).put(type, entry.getValue());
      }
    }

    for (Entry<String, Map<String, String>> pkg : packages.entrySet()) {
      if (!pkg.getValue().containsKey(OptionKeys.TYPE_SUFFIX)) {
        throw new BadConfigException("Package " + pkg.getKey() + " doesn't " +
            "have a package type");
      }
      if (!pkg.getValue().containsKey(OptionKeys.NAME_SUFFIX)) {
        throw new BadConfigException("Package " + pkg.getKey() + " doesn't " +
            "have a package name");
      }
    }

    return packages;
  }

  /**
   * Return system configurations requested by the app.
   * @param appConf app configurations
   * @return set of system configurations
   */
  public Set<String> getSystemConfigurationsRequested(
      ConfTreeOperations appConf) {
    Set<String> configList = new HashSet<>();

    String configTypes = appConf.get(SYSTEM_CONFIGS);
    if (configTypes != null && configTypes.length() > 0) {
      String[] configs = configTypes.split(",");
      for (String config : configs) {
        configList.add(config.trim());
      }
    }

    return configList;
  }

  /**
   * For a given config description/name, pull out its site configs from the
   * source config map, remove the site.configDN. prefix from them, and place
   * them into a new config map using the {@link #propagateSiteOptions} method
   * (with tokens substituted). This new k/v map is put as the value for the
   * configDN key in the configurations map.
   * @param configName config description/name
   * @param sourceConfig config containing site.* properties
   * @param configurations configuration map to be populated
   * @param tokens initial substitution tokens
   * @param amState access to AM state
   */
  private void addNamedConfiguration(String configName,
      Map<String, String> sourceConfig,
      Map<String, Map<String, String>> configurations,
      Map<String, String> tokens, StateAccessForProviders amState) {
    Map<String, String> config = new HashMap<>();
    if (configName.equals(GLOBAL_CONFIG_TAG)) {
      addDefaultGlobalConfig(config);
    }
    // add role hosts to tokens
    addRoleRelatedTokens(tokens, amState);
    propagateSiteOptions(sourceConfig, config, configName, tokens);

    configurations.put(configName, config);
  }

  /**
   * Get initial token map to be substituted into config values.
   * @param appConf app configurations
   * @param internals internal configurations
   * @param componentName component name
   * @param componentGroup component group
   * @param clusterName app name
   * @return tokens to replace
   */
  public Map<String, String> getStandardTokenMap(ConfTreeOperations appConf,
      ConfTreeOperations internals, String componentName,
      String componentGroup, String clusterName) {
    return getStandardTokenMap(appConf, internals, componentName,
        componentGroup, null, clusterName);
  }

  /**
   * Get initial token map to be substituted into config values.
   * @param appConf app configurations
   * @param internals internal configurations
   * @param componentName component name
   * @param componentGroup component group
   * @param containerId container ID
   * @param clusterName app name
   * @return tokens to replace
   */
  public Map<String, String> getStandardTokenMap(ConfTreeOperations appConf,
      ConfTreeOperations internals, String componentName,
      String componentGroup, String containerId, String clusterName) {

    Map<String, String> tokens = new HashMap<>();
    if (containerId != null) {
      tokens.put("${CONTAINER_ID}", containerId);
    }
    String nnuri = appConf.get("site.fs.defaultFS");
    tokens.put("${NN_URI}", nnuri);
    tokens.put("${NN_HOST}", URI.create(nnuri).getHost());
    tokens.put("${ZK_HOST}", appConf.get(OptionKeys.ZOOKEEPER_HOSTS));
    tokens.put("${DEFAULT_ZK_PATH}", appConf.get(OptionKeys.ZOOKEEPER_PATH));
    String prefix = appConf.getComponentOpt(componentGroup, ROLE_PREFIX,
        null);
    String dataDirSuffix = "";
    if (prefix == null) {
      prefix = "";
    } else {
      dataDirSuffix = "_" + SliderUtils.trimPrefix(prefix);
    }
    tokens.put("${DEFAULT_DATA_DIR}", internals.getGlobalOptions()
        .getOption(InternalKeys.INTERNAL_DATA_DIR_PATH, null) + dataDirSuffix);
    tokens.put("${JAVA_HOME}", appConf.get(JAVA_HOME));
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
  public void addRoleRelatedTokens(Map<String, String> tokens,
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
   * Add global configuration properties.
   * @param config map where default global properties will be added
   */
  private void addDefaultGlobalConfig(Map<String, String> config) {
    config.put("app_log_dir", "${LOG_DIR}");
    config.put("app_pid_dir", "${WORK_DIR}/app/run");
    config.put("app_install_dir", "${WORK_DIR}/app/install");
    config.put("app_conf_dir", "${WORK_DIR}/" + APP_CONF_DIR);
    config.put("app_input_conf_dir", "${WORK_DIR}/" + PROPAGATED_CONF_DIR_NAME);

    // add optional parameters only if they are not already provided
    if (!config.containsKey("pid_file")) {
      config.put("pid_file", "${WORK_DIR}/app/run/component.pid");
    }
    if (!config.containsKey("app_root")) {
      config.put("app_root", "${WORK_DIR}/app/install");
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
   * Return a list of hostnames based on current ClusterNodes.
   * @param values cluster nodes
   * @return list of hosts
   */
  public Iterable<String> getHostNamesList(Collection<ClusterNode> values) {
    List<String> hosts = new ArrayList<>();
    for (ClusterNode cn : values) {
      hosts.add(cn.hostname);
    }
    return hosts;
  }

  /**
   * Return a list of IPs based on current ClusterNodes.
   * @param values cluster nodes
   * @return list of hosts
   */
  public Iterable<String> getIPsList(Collection<ClusterNode> values) {
    List<String> hosts = new ArrayList<>();
    for (ClusterNode cn : values) {
      hosts.add(cn.ip);
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

  /**
   * Publish a named property bag that may contain name-value pairs for app
   * configurations such as hbase-site.
   * @param name config file identifying name
   * @param description config file description
   * @param entries config file properties
   * @param amState access to AM state
   */
  public void publishApplicationInstanceData(String name, String description,
      Iterable<Map.Entry<String, String>> entries,
      StateAccessForProviders amState) {
    PublishedConfiguration pubconf = new PublishedConfiguration(description,
        entries);
    log.info("publishing {}", pubconf);
    amState.getPublishedSliderConfigurations().put(name, pubconf);
  }

  /**
   * Publish an export group.
   * @param exportGroup export groups
   * @param amState access to AM state
   * @param roleGroup component group
   */
  public void publishExportGroup(Map<String, List<ExportEntry>> exportGroup,
      StateAccessForProviders amState, String roleGroup) {
    // Publish in old format for the time being
    Map<String, String> simpleEntries = new HashMap<>();
    for (Entry<String, List<ExportEntry>> entry : exportGroup.entrySet()) {
      List<ExportEntry> exports = entry.getValue();
      if (SliderUtils.isNotEmpty(exports)) {
        // there is no support for multiple exports per name, so extract only
        // the first one
        simpleEntries.put(entry.getKey(), entry.getValue().get(0).getValue());
      }
    }
    publishApplicationInstanceData(roleGroup, roleGroup,
        simpleEntries.entrySet(), amState);

    PublishedExports exports = new PublishedExports(roleGroup);
    exports.setUpdated(new Date().getTime());
    exports.putValues(exportGroup.entrySet());
    amState.getPublishedExportsSet().put(roleGroup, exports);
  }

  public Map<String, String> getExports(ConfTreeOperations appConf,
      String roleGroup) {
    Map<String, String> exports = new HashMap<>();
    propagateOptions(appConf.getComponent(roleGroup).options, exports,
        null, OptionKeys.EXPORT_PREFIX);
    return exports;
  }

  private static final String COMPONENT_TAG = "component";
  private static final String HOST_FOLDER_FORMAT = "%s:%s";
  private static final String CONTAINER_LOGS_TAG = "container_log_dirs";
  private static final String CONTAINER_PWDS_TAG = "container_work_dirs";

  /**
   * Format the folder locations and publish in the registry service.
   * @param folders folder information
   * @param containerId container ID
   * @param hostFqdn host FQDN
   * @param componentName component name
   */
  public void publishFolderPaths(Map<String, String> folders,
      String containerId, String componentName, String hostFqdn,
      StateAccessForProviders amState,
      Map<String, ExportEntry> logFolderExports,
      Map<String, ExportEntry> workFolderExports) {
    Date now = new Date();
    for (Map.Entry<String, String> entry : folders.entrySet()) {
      ExportEntry exportEntry = new ExportEntry();
      exportEntry.setValue(String.format(HOST_FOLDER_FORMAT, hostFqdn,
          entry.getValue()));
      exportEntry.setContainerId(containerId);
      exportEntry.setLevel(COMPONENT_TAG);
      exportEntry.setTag(componentName);
      exportEntry.setUpdatedTime(now.toString());
      if (entry.getKey().equals("AGENT_LOG_ROOT") ||
          entry.getKey().equals("LOG_DIR")) {
        synchronized (logFolderExports) {
          logFolderExports.put(containerId, exportEntry);
        }
      } else {
        synchronized (workFolderExports) {
          workFolderExports.put(containerId, exportEntry);
        }
      }
      log.info("Updating log and pwd folders for container {}", containerId);
    }

    PublishedExports exports = new PublishedExports(CONTAINER_LOGS_TAG);
    exports.setUpdated(now.getTime());
    synchronized (logFolderExports) {
      updateExportsFromList(exports, logFolderExports);
    }
    amState.getPublishedExportsSet().put(CONTAINER_LOGS_TAG, exports);

    exports = new PublishedExports(CONTAINER_PWDS_TAG);
    exports.setUpdated(now.getTime());
    synchronized (workFolderExports) {
      updateExportsFromList(exports, workFolderExports);
    }
    amState.getPublishedExportsSet().put(CONTAINER_PWDS_TAG, exports);
  }

  /**
   * Update the export data from the map.
   * @param exports published exports
   * @param folderExports folder exports
   */
  private void updateExportsFromList(PublishedExports exports,
      Map<String, ExportEntry> folderExports) {
    Map<String, List<ExportEntry>> perComponentList = new HashMap<>();
    for(Map.Entry<String, ExportEntry> logEntry : folderExports.entrySet()) {
      String componentName = logEntry.getValue().getTag();
      if (!perComponentList.containsKey(componentName)) {
        perComponentList.put(componentName, new ArrayList<ExportEntry>());
      }
      perComponentList.get(componentName).add(logEntry.getValue());
    }
    exports.putValues(perComponentList.entrySet());
  }
}
