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

package org.apache.hadoop.yarn.service.provider;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.api.records.ConfigFormat;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.containerlaunch.AbstractLauncher;
import org.apache.hadoop.yarn.service.exceptions.BadCommandArgumentsException;
import org.apache.hadoop.yarn.service.exceptions.SliderException;
import org.apache.hadoop.yarn.service.utils.PublishedConfiguration;
import org.apache.hadoop.yarn.service.utils.PublishedConfigurationOutputter;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.service.utils.SliderFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.service.api.ServiceApiConstants.*;

/**
 * This is a factoring out of methods handy for providers. It's bonded to a log
 * at construction time.
 */
public class ProviderUtils implements YarnServiceConstants {

  protected static final Logger log =
      LoggerFactory.getLogger(ProviderUtils.class);


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
      ServiceUtils.putJar(providerResources,
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
    if (ServiceUtils.isSet(libLocalSrcDir)) {
      File file = new File(libLocalSrcDir);
      if (!file.exists() || !file.isDirectory()) {
        throw new BadCommandArgumentsException(
            "Supplied lib src dir %s is not valid", libLocalSrcDir);
      }
    }
    ServiceUtils.putAllJars(providerResources, sliderFileSystem, tempPath,
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
  public static void substituteMapWithTokens(Map<String, String> configs,
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

  public static Path initCompInstanceDir(SliderFileSystem fs,
      ComponentInstance instance) {
    Path compDir = new Path(new Path(fs.getAppDir(), "components"),
        instance.getCompName());
    Path compInstanceDir = new Path(compDir, instance.getCompInstanceName());
    instance.setCompInstanceDir(compInstanceDir);
    return compInstanceDir;
  }

  // 1. Create all config files for a component on hdfs for localization
  // 2. Add the config file to localResource
  public static synchronized void createConfigFileAndAddLocalResource(
      AbstractLauncher launcher, SliderFileSystem fs, Component component,
      Map<String, String> tokensForSubstitution, ComponentInstance instance,
      ServiceContext context) throws IOException {
    Path compInstanceDir = initCompInstanceDir(fs, instance);
    if (!fs.getFileSystem().exists(compInstanceDir)) {
      log.info(instance.getCompInstanceId() + ": Creating dir on hdfs: " + compInstanceDir);
      fs.getFileSystem().mkdirs(compInstanceDir,
          new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE));
    } else {
      log.info("Component instance conf dir already exists: " + compInstanceDir);
    }

    if (log.isDebugEnabled()) {
      log.debug("Tokens substitution for component instance: " + instance
          .getCompInstanceName() + System.lineSeparator()
          + tokensForSubstitution);
    }

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
        log.info("Saving config file on hdfs for component " + instance
            .getCompInstanceName() + ": " + configFile);

        if (configFile.getSrcFile() != null) {
          // Load config file template
          switch (configFile.getType()) {
          case HADOOP_XML:
            // Hadoop_xml_template
            resolveHadoopXmlTemplateAndSaveOnHdfs(fs.getFileSystem(),
                tokensForSubstitution, configFile, remoteFile, context);
            break;
          case TEMPLATE:
            // plain-template
            resolvePlainTemplateAndSaveOnHdfs(fs.getFileSystem(),
                tokensForSubstitution, configFile, remoteFile, context);
            break;
          default:
            log.info("Not supporting loading src_file for " + configFile);
            break;
          }
        } else {
          // If src_file is not specified
          resolvePropsInConfigFileAndSaveOnHdfs(fs, tokensForSubstitution,
              instance, configFile, fileName, remoteFile);
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

  private static void resolvePropsInConfigFileAndSaveOnHdfs(SliderFileSystem fs,
      Map<String, String> tokensForSubstitution, ComponentInstance instance,
      ConfigFile configFile, String fileName, Path remoteFile)
      throws IOException {
    // substitute non-template configs
    substituteMapWithTokens(configFile.getProperties(), tokensForSubstitution);

    // write configs onto hdfs
    PublishedConfiguration publishedConfiguration =
        new PublishedConfiguration(fileName,
            configFile.getProperties().entrySet());
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
      log.info("Component instance = " + instance.getCompInstanceName()
              + ", config file already exists: " + remoteFile);
    }
  }

  // 1. substitute config template - only handle hadoop_xml format
  // 2. save on hdfs
  @SuppressWarnings("unchecked")
  private static void resolveHadoopXmlTemplateAndSaveOnHdfs(FileSystem fs,
      Map<String, String> tokensForSubstitution, ConfigFile configFile,
      Path remoteFile, ServiceContext context) throws IOException {
    Map<String, String> conf;
    try {
      conf = (Map<String, String>) context.configCache.get(configFile);
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
    for (Map.Entry<String, String> entry : configFile.getProperties().entrySet()) {
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
  private static void resolvePlainTemplateAndSaveOnHdfs(FileSystem fs,
      Map<String, String> tokensForSubstitution, ConfigFile configFile,
      Path remoteFile, ServiceContext context) {
    String content;
    try {
      content = (String) context.configCache.get(configFile);
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
   * @return tokens to replace
   */
  public static Map<String, String> initCompTokensForSubstitute(
      ComponentInstance instance, Container container) {
    Map<String, String> tokens = new HashMap<>();
    tokens.put(COMPONENT_NAME, instance.getCompSpec().getName());
    tokens
        .put(COMPONENT_NAME_LC, instance.getCompSpec().getName().toLowerCase());
    tokens.put(COMPONENT_INSTANCE_NAME, instance.getCompInstanceName());
    tokens.put(CONTAINER_ID, container.getId().toString());
    tokens.put(COMPONENT_ID,
        String.valueOf(instance.getCompInstanceId().getId()));
    tokens.putAll(instance.getComponent().getDependencyHostIpTokens());
    return tokens;
  }
}
