/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * test for NvidiaDockerV2CommandPlugin.
 */
public class TestNvidiaDockerV2CommandPlugin {
  private Map<String, List<String>> copyCommandLine(
      Map<String, List<String>> map) {
    Map<String, List<String>> ret = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      ret.put(entry.getKey(), new ArrayList<>(entry.getValue()));
    }
    return ret;
  }

  private boolean commandlinesEquals(Map<String, List<String>> cli1,
      Map<String, List<String>> cli2) {
    if (!Sets.symmetricDifference(cli1.keySet(), cli2.keySet()).isEmpty()) {
      return false;
    }

    for (String key : cli1.keySet()) {
      List<String> value1 = cli1.get(key);
      List<String> value2 = cli2.get(key);
      if (!value1.equals(value2)) {
        return false;
      }
    }

    return true;
  }

  static class MyNvidiaDockerV2CommandPlugin
      extends NvidiaDockerV2CommandPlugin {
    private boolean requestsGpu = false;

    MyNvidiaDockerV2CommandPlugin() {}

    public void setRequestsGpu(boolean r) {
      requestsGpu = r;
    }

    @Override
    protected boolean requestsGpu(Container container) {
      return requestsGpu;
    }
  }

  @Test
  public void testPlugin() throws Exception {
    DockerRunCommand runCommand = new DockerRunCommand("container_1", "user",
        "fakeimage");

    Map<String, List<String>> originalCommandline = copyCommandLine(
        runCommand.getDockerCommandWithArguments());

    MyNvidiaDockerV2CommandPlugin
        commandPlugin = new MyNvidiaDockerV2CommandPlugin();

    Container nmContainer = mock(Container.class);

    // getResourceMapping is null, so commandline won't be updated
    commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    Assert.assertTrue(commandlinesEquals(originalCommandline,
        runCommand.getDockerCommandWithArguments()));

    // no GPU resource assigned, so commandline won't be updated
    ResourceMappings resourceMappings = new ResourceMappings();
    when(nmContainer.getResourceMappings()).thenReturn(resourceMappings);
    commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    Assert.assertTrue(commandlinesEquals(originalCommandline,
        runCommand.getDockerCommandWithArguments()));

    // Assign GPU resource
    ResourceMappings.AssignedResources assigned =
        new ResourceMappings.AssignedResources();
    assigned.updateAssignedResources(
        ImmutableList.of(new GpuDevice(0, 0), new GpuDevice(1, 1)));
    resourceMappings.addAssignedResources(ResourceInformation.GPU_URI,
        assigned);

    commandPlugin.setRequestsGpu(true);
    commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    Map<String, List<String>> newCommandLine =
        runCommand.getDockerCommandWithArguments();

    // Command line will be updated
    Assert.assertFalse(commandlinesEquals(originalCommandline, newCommandLine));
    // NVIDIA_VISIBLE_DEVICES will be set
    Assert.assertTrue(
        runCommand.getEnv().get("NVIDIA_VISIBLE_DEVICES").equals("0,1"));
    // runtime should exist
    Assert.assertTrue(newCommandLine.containsKey("runtime"));
  }
}