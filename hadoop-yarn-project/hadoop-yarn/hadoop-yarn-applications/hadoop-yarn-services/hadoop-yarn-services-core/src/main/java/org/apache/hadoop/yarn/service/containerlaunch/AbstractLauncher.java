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

import com.google.common.base.Preconditions;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryContext;
import org.apache.hadoop.yarn.api.records.ContainerRetryPolicy;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.apache.hadoop.yarn.service.utils.CoreFileSystem;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.hadoop.yarn.service.provider.docker.DockerKeys.DEFAULT_DOCKER_NETWORK;

/**
 * Launcher of applications: base class
 */
public class AbstractLauncher {
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
  protected final ContainerLaunchContext containerLaunchContext =
    Records.newRecord(ContainerLaunchContext.class);
  protected final List<String> commands = new ArrayList<>(20);
  protected final Map<String, LocalResource> localResources = new HashMap<>();
  protected final Map<String, String> mountPaths = new HashMap<>();
  private final Map<String, ByteBuffer> serviceData = new HashMap<>();
  // security
  protected final Credentials credentials;
  protected boolean yarnDockerMode = false;
  protected String dockerImage;
  protected String dockerNetwork = DEFAULT_DOCKER_NETWORK;
  protected String dockerHostname;
  protected String runPrivilegedContainer;


  /**
   * Create instance.
   * @param coreFileSystem filesystem
   * @param credentials initial set of credentials -null is permitted
   */
  public AbstractLauncher(
      CoreFileSystem coreFileSystem,
      Credentials credentials) {
    this.coreFileSystem = coreFileSystem;
    this.credentials = credentials != null ? credentials: new Credentials();
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

  /**
   * Accessor to the credentials
   * @return the credentials associated with this launcher
   */
  public Credentials getCredentials() {
    return credentials;
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
    log.debug("{} tokens", credentials.numberOfTokens());
    containerLaunchContext.setTokens(CredentialUtils.marshallCredentials(
        credentials));

    if(yarnDockerMode){
      Map<String, String> env = containerLaunchContext.getEnvironment();
      env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", dockerImage);
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK", dockerNetwork);
      env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME",
          dockerHostname);
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

  public void setRetryContext(int maxRetries, int retryInterval) {
    ContainerRetryContext retryContext = ContainerRetryContext
        .newInstance(ContainerRetryPolicy.RETRY_ON_ALL_ERRORS, null, maxRetries,
            retryInterval);
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
        log.debug(key + "=" + ServiceUtils.stringify(val.getResource()));
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
    if (runPrivilegedContainer) {
      this.runPrivilegedContainer = Boolean.toString(true);
    } else {
      this.runPrivilegedContainer = Boolean.toString(false);
    }
  }

}
