/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntime;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test container runtime delegation.
 */
public class TestDelegatingLinuxContainerRuntime {

  private DelegatingLinuxContainerRuntime delegatingLinuxContainerRuntime;
  private Configuration conf;
  private Map<String, String> env = new HashMap<>();

  @Before
  public void setUp() throws Exception {
    delegatingLinuxContainerRuntime = new DelegatingLinuxContainerRuntime();
    conf = new Configuration();
    env.clear();
  }

  @Test
  public void testIsRuntimeAllowedDefault() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        YarnConfiguration.DEFAULT_LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES[0]);
    System.out.println(conf.get(
        YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES));
    delegatingLinuxContainerRuntime.initialize(conf, null);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX));
  }

  @Test
  public void testIsRuntimeAllowedDocker() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "docker");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX));
  }

  @Test
  public void testIsRuntimeAllowedJavaSandbox() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "javasandbox");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
  }

  @Test
  public void testIsRuntimeAllowedMultiple() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "docker,javasandbox");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
  }

  @Test
  public void testIsRuntimeAllowedAll() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "default,docker,javasandbox");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.JAVASANDBOX));
  }

  @Test
  public void testJavaSandboxNotAllowedButPermissive() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "default,docker");
    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX, "permissive");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    ContainerRuntime runtime =
        delegatingLinuxContainerRuntime.pickContainerRuntime(env);
    assertTrue(runtime instanceof DefaultLinuxContainerRuntime);
  }

  @Test
  public void testJavaSandboxNotAllowedButPermissiveDockerRequested()
      throws Exception {
    env.put(ContainerRuntimeConstants.ENV_CONTAINER_TYPE, "docker");
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "default,docker");
    conf.set(YarnConfiguration.YARN_CONTAINER_SANDBOX, "permissive");
    delegatingLinuxContainerRuntime.initialize(conf, null);
    ContainerRuntime runtime =
        delegatingLinuxContainerRuntime.pickContainerRuntime(env);
    assertTrue(runtime instanceof DockerLinuxContainerRuntime);
  }
}