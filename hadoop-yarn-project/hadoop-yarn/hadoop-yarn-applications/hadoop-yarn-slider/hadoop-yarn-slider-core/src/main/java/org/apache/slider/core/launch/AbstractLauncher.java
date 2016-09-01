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

package org.apache.slider.core.launch;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.CoreFileSystem;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.MapOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.slider.providers.docker.DockerKeys.DEFAULT_DOCKER_NETWORK;

/**
 * Launcher of applications: base class
 */
public abstract class AbstractLauncher extends Configured {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractLauncher.class);
  public static final String CLASSPATH = "CLASSPATH";
  /**
   * Filesystem to use for the launch
   */
  protected final CoreFileSystem coreFileSystem;
  /**
   * Env vars; set up at final launch stage
   */
  protected final Map<String, String> envVars = new HashMap<>();
  protected final MapOperations env = new MapOperations("env", envVars);
  protected final ContainerLaunchContext containerLaunchContext =
    Records.newRecord(ContainerLaunchContext.class);
  protected final List<String> commands = new ArrayList<>(20);
  protected final Map<String, LocalResource> localResources = new HashMap<>();
  protected final Map<String, String> mountPaths = new HashMap<>();
  private final Map<String, ByteBuffer> serviceData = new HashMap<>();
  // security
  protected final Credentials credentials;
  protected LogAggregationContext logAggregationContext;
  protected boolean yarnDockerMode = false;
  protected String dockerImage;
  protected String dockerNetwork = DEFAULT_DOCKER_NETWORK;
  protected String yarnContainerMountPoints;
  protected String runPrivilegedContainer;


  /**
   * Create instance.
   * @param conf configuration
   * @param coreFileSystem filesystem
   * @param credentials initial set of credentials -null is permitted
   */
  protected AbstractLauncher(Configuration conf,
      CoreFileSystem coreFileSystem,
      Credentials credentials) {
    super(conf);
    this.coreFileSystem = coreFileSystem;
    this.credentials = credentials != null ? credentials: new Credentials();
  }

  /**
   * Get the container. Until "completed", this isn't valid to launch.
   * @return the container to launch
   */
  public ContainerLaunchContext getContainerLaunchContext() {
    return containerLaunchContext;
  }
  
  public void setYarnDockerMode(boolean yarnDockerMode){
    this.yarnDockerMode = yarnDockerMode;
  }

  /**
   * Get the env vars to work on
   * @return env vars
   */
  public MapOperations getEnv() {
    return env;
  }

  /**
   * Get the launch commands.
   * @return the live list of commands 
   */
  public List<String> getCommands() {
    return commands;
  }

  /**
   * Get the map of local resources.
   * @return the live map of local resources.
   */
  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public void addLocalResource(String subPath, LocalResource resource) {
    localResources.put(subPath, resource);
  }

  public void addLocalResource(String subPath, LocalResource resource, String mountPath) {
    localResources.put(subPath, resource);
    mountPaths.put(subPath, mountPath);
  }

  /**
   * Add a set of local resources
   * @param resourceMap map of name:resource to add
   */
  public void addLocalResources(Map<String, LocalResource> resourceMap) {
    localResources.putAll(resourceMap);
  }

  public Map<String, ByteBuffer> getServiceData() {
    return serviceData;
  }

  /**
   * Accessor to the credentials
   * @return the credentials associated with this launcher
   */
  public Credentials getCredentials() {
    return credentials;
  }

  /**
   * Add a command line. It is converted to a single command before being
   * added.
   * @param cmd
   */
  public void addCommandLine(CommandLineBuilder cmd) {
    commands.add(cmd.build());
  }

  public void addCommand(String cmd) {
    commands.add(cmd);
  }

  /**
   * Add a list of commands. Each element in the list becomes a single command
   * @param commandList list of commands
   */
  public void addCommands(List<String> commandList) {
    commands.addAll(commandList);
  }

  /**
   * Get all commands as a string, separated by ";". This is for diagnostics
   * @return a string description of the commands
   */
  public String getCommandsAsString() {
    return SliderUtils.join(getCommands(), "; ");
  }

  /**
   * Complete the launch context (copy in env vars, etc).
   * @return the container to launch
   */
  public ContainerLaunchContext completeContainerLaunch() throws IOException {
    
    String cmdStr = SliderUtils.join(commands, " ", false);
    log.debug("Completed setting up container command {}", cmdStr);
    containerLaunchContext.setCommands(commands);

    //env variables
    if (log.isDebugEnabled()) {
      log.debug("Environment variables");
      for (Map.Entry<String, String> envPair : envVars.entrySet()) {
        log.debug("    \"{}\"=\"{}\"", envPair.getKey(), envPair.getValue());
      }
    }    
    containerLaunchContext.setEnvironment(env);

    //service data
    if (log.isDebugEnabled()) {
      log.debug("Service Data size");
      for (Map.Entry<String, ByteBuffer> entry : serviceData.entrySet()) {
        log.debug("\"{}\"=> {} bytes of data", entry.getKey(),
            entry.getValue().array().length);
      }
    }
    containerLaunchContext.setServiceData(serviceData);

    // resources
    dumpLocalResources();
    containerLaunchContext.setLocalResources(localResources);

    //tokens
    log.debug("{} tokens", credentials.numberOfTokens());
    containerLaunchContext.setTokens(CredentialUtils.marshallCredentials(
        credentials));

    if(yarnDockerMode){
      Map<String, String> env = containerLaunchContext.getEnvironment();
      env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", dockerImage);
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK", dockerNetwork);
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER", runPrivilegedContainer);
      StringBuilder sb = new StringBuilder();
      for (Entry<String,String> mount : mountPaths.entrySet()) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(mount.getKey());
        sb.append(":");
        sb.append(mount.getValue());
      }
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS", sb.toString());
      log.info("yarn docker env var has been set {}", containerLaunchContext.getEnvironment().toString());
    }

    return containerLaunchContext;
  }

  /**
   * Dump local resources at debug level
   */
  private void dumpLocalResources() {
    if (log.isDebugEnabled()) {
      log.debug("{} resources: ", localResources.size());
      for (Map.Entry<String, LocalResource> entry : localResources.entrySet()) {

        String key = entry.getKey();
        LocalResource val = entry.getValue();
        log.debug(key + "=" + SliderUtils.stringify(val.getResource()));
      }
    }
  }

  /**
   * This is critical for an insecure cluster -it passes
   * down the username to YARN, and so gives the code running
   * in containers the rights it needs to work with
   * data.
   * @throws IOException problems working with current user
   */
  protected void propagateUsernameInInsecureCluster() throws IOException {
    //insecure cluster: propagate user name via env variable
    String userName = UserGroupInformation.getCurrentUser().getUserName();
    env.put(SliderKeys.HADOOP_USER_NAME, userName);
  }

  /**
   * Extract any resource requirements from this component's settings.
   * All fields that are set will override the existing values -if
   * unset that resource field will be left unchanged.
   *
   * Important: the configuration must already be fully resolved 
   * in order to pick up global options.
   * @param resource resource to configure
   * @param map map of options
   */
  public void extractResourceRequirements(Resource resource,
                                          Map<String, String> map) {

    if (map != null) {
      MapOperations options = new MapOperations("", map);
      resource.setMemory(options.getOptionInt(ResourceKeys.YARN_MEMORY,
                                              resource.getMemory()));
      resource.setVirtualCores(options.getOptionInt(ResourceKeys.YARN_CORES,
                                                    resource.getVirtualCores()));
    }
  }

  /**
   * Extract the value for option
   * {@code yarn.resourcemanager.am.retry-count-window-ms}
   * and set it on the ApplicationSubmissionContext. Use the default value
   * if option is not set.
   *
   * @param submissionContext
   * @param map
   */
  public void extractAmRetryCount(ApplicationSubmissionContext submissionContext,
                                  Map<String, String> map) {

    if (map != null) {
      MapOperations options = new MapOperations("", map);
      long amRetryCountWindow = options.getOptionLong(ResourceKeys
          .YARN_RESOURCEMANAGER_AM_RETRY_COUNT_WINDOW_MS,
          ResourceKeys.DEFAULT_AM_RETRY_COUNT_WINDOW_MS);
      log.info("Setting {} to {}",
          ResourceKeys.YARN_RESOURCEMANAGER_AM_RETRY_COUNT_WINDOW_MS,
          amRetryCountWindow);
      submissionContext.setAttemptFailuresValidityInterval(amRetryCountWindow);
    }
  }

  public void extractLogAggregationContext(Map<String, String> map) {
    if (map != null) {
      String logPatternSepStr = "\\|";
      String logPatternJoinStr = "|";
      MapOperations options = new MapOperations("", map);

      List<String> logIncludePatterns = new ArrayList<>();
      String includePatternExpression = options.getOption(
          ResourceKeys.YARN_LOG_INCLUDE_PATTERNS, "").trim();
      if (!includePatternExpression.isEmpty()) {
        String[] includePatterns = includePatternExpression
            .split(logPatternSepStr);
        for (String includePattern : includePatterns) {
          String trimmedIncludePattern = includePattern.trim();
          if (!trimmedIncludePattern.isEmpty()) {
            logIncludePatterns.add(trimmedIncludePattern);
          }
        }
      }
      String logIncludePattern = StringUtils.join(logIncludePatterns,
          logPatternJoinStr);
      log.info("Log include patterns: {}", logIncludePattern);

      List<String> logExcludePatterns = new ArrayList<>();
      String excludePatternExpression = options.getOption(
          ResourceKeys.YARN_LOG_EXCLUDE_PATTERNS, "").trim();
      if (!excludePatternExpression.isEmpty()) {
        String[] excludePatterns = excludePatternExpression
            .split(logPatternSepStr);
        for (String excludePattern : excludePatterns) {
          String trimmedExcludePattern = excludePattern.trim();
          if (!trimmedExcludePattern.isEmpty()) {
            logExcludePatterns.add(trimmedExcludePattern);
          }
        }
      }
      String logExcludePattern = StringUtils.join(logExcludePatterns,
          logPatternJoinStr);
      log.info("Log exclude patterns: {}", logExcludePattern);

      // SLIDER-810/YARN-3154 - hadoop 2.7.0 onwards a new instance method has
      // been added for log aggregation for LRS. Existing newInstance method's
      // behavior has changed and is used for log aggregation only after the
      // application has finished. This forces Slider users to move to hadoop
      // 2.7.0+ just for log aggregation, which is not very desirable. So we
      // decided to use reflection here to find out if the new 2.7.0 newInstance
      // method is available. If yes, then we use it, so log aggregation will
      // work in hadoop 2.7.0+ env. If no, then we fallback to the pre-2.7.0
      // newInstance method, which means log aggregation will work as expected
      // in hadoop 2.6 as well.
      // TODO: At some point, say 2-3 Slider releases down, when most users are
      // running hadoop 2.7.0, we should get rid of the reflection code here.
      try {
        Method logAggregationContextMethod = LogAggregationContext.class
            .getMethod("newInstance", String.class, String.class, String.class,
                String.class);
        // Need to set include/exclude patterns appropriately since by default
        // rolled log aggregation is not done for any files, so defaults are
        // - include pattern set to ""
        // - exclude pattern set to "*"
        // For Slider we want all logs to be uploaded if include/exclude
        // patterns are left empty by the app owner in resources file
        if (StringUtils.isEmpty(logIncludePattern)
            && StringUtils.isEmpty(logExcludePattern)) {
          logIncludePattern = ".*";
          logExcludePattern = "";
        } else if (StringUtils.isEmpty(logIncludePattern)
            && StringUtils.isNotEmpty(logExcludePattern)) {
          logIncludePattern = ".*";
        } else if (StringUtils.isNotEmpty(logIncludePattern)
            && StringUtils.isEmpty(logExcludePattern)) {
          logExcludePattern = "";
        }
        log.debug("LogAggregationContext newInstance method for rolled logs "
            + "include/exclude patterns is available");
        log.info("Modified log include patterns: {}", logIncludePattern);
        log.info("Modified log exclude patterns: {}", logExcludePattern);
        logAggregationContext = (LogAggregationContext) logAggregationContextMethod
            .invoke(null, null, null, logIncludePattern, logExcludePattern);
      } catch (NoSuchMethodException | SecurityException
          | IllegalAccessException | IllegalArgumentException
          | InvocationTargetException e) {
        log.debug("LogAggregationContext newInstance method for rolled logs "
            + "include/exclude patterns is not available - fallback to old one");
        log.debug(e.toString());
        logAggregationContext = LogAggregationContext.newInstance(
            logIncludePattern, logExcludePattern);
      }
    }
  }

  /**
   * Utility method to set up the classpath
   * @param classpath classpath to use
   */
  public void setClasspath(ClasspathConstructor classpath) {
    setEnv(CLASSPATH, classpath.buildClasspath());
  }

  /**
   * Set an environment variable in the launch context
   * @param var variable name
   * @param value value (must be non null)
   */
  public void setEnv(String var, String value) {
    Preconditions.checkArgument(var != null, "null variable name");
    Preconditions.checkArgument(value != null, "null value");
    env.put(var, value);
  }

  /**
   * Set an environment variable if its value is non-null.
   * @param var variable name
   * @param value value (may be null)
   */
  public void maybeSetEnv(String var, String value) {
    if (value != null) {
      setEnv(var, value);
    }
  }

  public void putEnv(Map<String, String> map) {
    env.putAll(map);
  }

  /**
   * Important: the configuration must already be fully resolved 
   * in order to pick up global options
   * Copy env vars into the launch context.
   */
  public boolean copyEnvVars(MapOperations options) {
    if (options == null) {
      return false;
    }
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(RoleKeys.ENV_PREFIX)) {
        key = key.substring(RoleKeys.ENV_PREFIX.length());
        env.put(key, entry.getValue());
      }
    }
    return true;
  }

  public String[] dumpEnvToString() {

    List<String> nodeEnv = new ArrayList<>();

    for (Map.Entry<String, String> entry : env.entrySet()) {
      String envElt = String.format("%s=\"%s\"",
                                    entry.getKey(),
                                    entry.getValue());
      log.debug(envElt);
      nodeEnv.add(envElt);
    }
    String[] envDescription = nodeEnv.toArray(new String[nodeEnv.size()]);

    return envDescription;
  }

  /**
   * Submit an entire directory
   * @param srcDir src path in filesystem
   * @param destRelativeDir relative path under destination local dir
   * @throws IOException IO problems
   */
  public void submitDirectory(Path srcDir, String destRelativeDir)
      throws IOException {
    //add the configuration resources
    Map<String, LocalResource> confResources;
    confResources = coreFileSystem.submitDirectory(
      srcDir,
      destRelativeDir);
    addLocalResources(confResources);
  }

  /**
   * Return the label expression and if not set null
   * @param map map to look up
   * @return extracted label or null
   */
  public String extractLabelExpression(Map<String, String> map) {
    if (map != null) {
      MapOperations options = new MapOperations("", map);
      return options.getOption(ResourceKeys.YARN_LABEL_EXPRESSION, null);
    }
    return null;
  }

  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public void setDockerNetwork(String dockerNetwork) {
    this.dockerNetwork = dockerNetwork;
  }

  public void setYarnContainerMountPoints(String yarnContainerMountPoints) {
    this.yarnContainerMountPoints = yarnContainerMountPoints;
  }

  public void setRunPrivilegedContainer(String runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
  }

  public void setRunPrivilegedContainer(boolean runPrivilegedContainer) {
    if (runPrivilegedContainer) {
      this.runPrivilegedContainer = Boolean.toString(true);
    } else {
      this.runPrivilegedContainer = Boolean.toString(false);
    }
  }

}
