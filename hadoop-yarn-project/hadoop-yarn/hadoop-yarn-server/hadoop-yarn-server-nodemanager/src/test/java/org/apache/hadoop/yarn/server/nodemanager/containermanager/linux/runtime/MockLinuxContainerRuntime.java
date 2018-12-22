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
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;

import java.util.Map;

public class MockLinuxContainerRuntime implements LinuxContainerRuntime {
  @Override
  public void initialize(Configuration conf, Context nmContext) {}

  @Override
  public boolean isRuntimeRequested(Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ContainerRuntimeConstants.ENV_CONTAINER_TYPE);

    return type != null && type.equals("mock");
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx) {}

  @Override
  public void launchContainer(ContainerRuntimeContext ctx) {}

  @Override
  public void relaunchContainer(ContainerRuntimeContext ctx) {}

  @Override
  public void signalContainer(ContainerRuntimeContext ctx) {}

  @Override
  public void reapContainer(ContainerRuntimeContext ctx) {}

  @Override
  public String[] getIpAndHost(Container container) {
    return new String[0];
  }

  @Override
  public String getExposedPorts(Container container) {
    return "";
  }

  @Override
  public IOStreamPair execContainer(ContainerExecContext ctx)
      throws ContainerExecutionException {
    return null;
  }
}
