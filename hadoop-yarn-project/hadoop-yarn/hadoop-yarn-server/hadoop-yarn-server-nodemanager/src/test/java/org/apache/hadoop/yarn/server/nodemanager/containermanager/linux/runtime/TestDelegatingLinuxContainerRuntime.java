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
    delegatingLinuxContainerRuntime.initialize(conf);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
  }

  @Test
  public void testIsRuntimeAllowedDocker() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "docker");
    delegatingLinuxContainerRuntime.initialize(conf);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertFalse(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
  }


  @Test
  public void testIsRuntimeAllowedMultiple() throws Exception {
    conf.set(YarnConfiguration.LINUX_CONTAINER_RUNTIME_ALLOWED_RUNTIMES,
        "default,docker");
    delegatingLinuxContainerRuntime.initialize(conf);
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DOCKER));
    assertTrue(delegatingLinuxContainerRuntime.isRuntimeAllowed(
        LinuxContainerRuntimeConstants.RuntimeType.DEFAULT));
  }
}