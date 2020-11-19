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

package org.apache.hadoop.yarn.service.containerlaunch;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Launcher of applications: base class
 */
public class AbstractLauncher {
  private static final Logger log =
    LoggerFactory.getLogger(AbstractLauncher.class);
  public static final String CLASSPATH = "CLASSPATH";
  public static final String ENV_DOCKER_CONTAINER_MOUNTS =
      "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS";
  /**
   * Env vars; set up at final launch stage
   */
  protected final Map<String, String> envVars = new HashMap<>();
  protected final ContainerLaunchContext containerLaunchContext =
    Records.newRecord(ContainerLaunchContext.class);
  protected final List<String> commands = new ArrayList<>(20);
  protected final Map<String, LocalResource> localResources = new HashMap<>();
  protected final Map<String, String> mountPaths = new HashMap<>();
  private final Map<String, ByteBuffer> serviceData = new HashMap<>();
  protected boolean yarnDockerMode = false;
  protected String dockerImage;
  protected String dockerNetwork;
  protected String dockerHostname;
  protected boolean runPrivilegedContainer = false;
  private ServiceContext context;

  public AbstractLauncher(ServiceContext context) {
    this.context = context;
  }
  
  public void setYarnDockerMode(boolean yarnDockerMode){
    this.yarnDockerMode = yarnDockerMode;
  }

  /**
   * Get the env vars to work on
   * @return env vars
   */
  public Map<String, String> getEnv() {
    return envVars;
  }

  /**
   * Get the launch commands.
   * @return the live list of commands 
   */
  public List<String> getCommands() {
    return commands;
  }

  public void addLocalResource(String subPath, LocalResource resource) {
    localResources.put(subPath, resource);
  }

  public void addLocalResource(String subPath, LocalResource resource, String mountPath) {
    localResources.put(subPath, resource);
    mountPaths.put(subPath, mountPath);
  }


  public void addCommand(String cmd) {
    commands.add(cmd);
  }

  /**
   * Complete the launch context (copy in env vars, etc).
   * @return the container to launch
   */
  public ContainerLaunchContext completeContainerLaunch() throws IOException {
    
    String cmdStr = ServiceUtils.join(commands, " ", false);
    log.debug("Completed setting up container command {}", cmdStr);
    containerLaunchContext.setCommands(commands);

    //env variables
    if (log.isDebugEnabled()) {
      log.debug("Environment variables");
      for (Map.Entry<String, String> envPair : envVars.entrySet()) {
        log.debug("    \"{}\"=\"{}\"", envPair.getKey(), envPair.getValue());
      }
    }    
    containerLaunchContext.setEnvironment(envVars);

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
    if (context.tokens != null) {
      containerLaunchContext.setTokens(context.tokens.duplicate());
    }

    if(yarnDockerMode){
      Map<String, String> env = containerLaunchContext.getEnvironment();
      env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", dockerImage);
      if (ServiceUtils.isSet(dockerNetwork)) {
        env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK",
            dockerNetwork);
      }
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME",
          dockerHostname);
      if (runPrivilegedContainer) {
        env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER",
            "true");
      }
      if (!mountPaths.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        if (env.get(ENV_DOCKER_CONTAINER_MOUNTS) != null) {
          // user specified mounts in the spec
          sb.append(env.get(ENV_DOCKER_CONTAINER_MOUNTS));
        }
        for (Entry<String, String> mount : mountPaths.entrySet()) {
          if (sb.length() > 0) {
            sb.append(",");
          }
          sb.append(mount.getKey()).append(":")
              .append(mount.getValue()).append(":ro");
        }
        env.put(ENV_DOCKER_CONTAINER_MOUNTS, sb.toString());
      }
      log.info("yarn docker env var has been set {}",
          containerLaunchContext.getEnvironment().toString());
    }

    return containerLaunchContext;
  }

  public void setRetryContext(int maxRetries, int retryInterval,
      long failuresValidityInterval) {
    ContainerRetryContext retryContext = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null,
            maxRetries, retryInterval, failuresValidityInterval);
    containerLaunchContext.setContainerRetryContext(retryContext);
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
        log.debug("{} = {}", key, ServiceUtils.stringify(val.getResource()));
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
    envVars.put(YarnServiceConstants.HADOOP_USER_NAME, userName);
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
    envVars.put(var, value);
  }


  public void putEnv(Map<String, String> map) {
    envVars.putAll(map);
  }


  public void setDockerImage(String dockerImage) {
    this.dockerImage = dockerImage;
  }

  public void setDockerNetwork(String dockerNetwork) {
    this.dockerNetwork = dockerNetwork;
  }

  public void setDockerHostname(String dockerHostname) {
    this.dockerHostname = dockerHostname;
  }

  public void setRunPrivilegedContainer(boolean runPrivilegedContainer) {
    this.runPrivilegedContainer = runPrivilegedContainer;
  }

  @VisibleForTesting
  public String getDockerImage() {
    return dockerImage;
  }
}
