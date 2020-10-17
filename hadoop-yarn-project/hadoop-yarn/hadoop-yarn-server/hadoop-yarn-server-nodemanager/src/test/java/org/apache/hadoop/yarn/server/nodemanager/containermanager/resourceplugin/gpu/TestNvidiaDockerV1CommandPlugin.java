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

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.Sets;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ResourceMappings;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerVolumeCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNvidiaDockerV1CommandPlugin {
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

  static class MyHandler implements HttpHandler {
    String response = "This is the response";

    @Override
    public void handle(HttpExchange t) throws IOException {
      t.sendResponseHeaders(200, response.length());
      OutputStream os = t.getResponseBody();
      os.write(response.getBytes());
      os.close();
    }
  }

  static class MyNvidiaDockerV1CommandPlugin
      extends NvidiaDockerV1CommandPlugin {
    private boolean requestsGpu = false;

    public MyNvidiaDockerV1CommandPlugin(Configuration conf) {
      super(conf);
    }

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
    Configuration conf = new Configuration();

    DockerRunCommand runCommand = new DockerRunCommand("container_1", "user",
        "fakeimage");

    Map<String, List<String>> originalCommandline = copyCommandLine(
        runCommand.getDockerCommandWithArguments());

    MyNvidiaDockerV1CommandPlugin
        commandPlugin = new MyNvidiaDockerV1CommandPlugin(conf);

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

    // Assign GPU resource, init will be invoked
    ResourceMappings.AssignedResources assigned =
        new ResourceMappings.AssignedResources();
    assigned.updateAssignedResources(
        ImmutableList.of(new GpuDevice(0, 0), new GpuDevice(1, 1)));
    resourceMappings.addAssignedResources(ResourceInformation.GPU_URI,
        assigned);

    commandPlugin.setRequestsGpu(true);

    // Since there's no HTTP server running, so we will see exception
    boolean caughtException = false;
    try {
      commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    } catch (ContainerExecutionException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // Start HTTP server
    MyHandler handler = new MyHandler();
    HttpServer server = HttpServer.create(new InetSocketAddress(60111), 0);
    server.createContext("/test", handler);
    server.start();

    String hostName = server.getAddress().getHostName();
    int port = server.getAddress().getPort();
    String httpUrl = "http://" + hostName + ":" + port + "/test";

    conf.set(YarnConfiguration.NVIDIA_DOCKER_PLUGIN_V1_ENDPOINT, httpUrl);

    commandPlugin = new MyNvidiaDockerV1CommandPlugin(conf);

    // Start use invalid options
    handler.response = "INVALID_RESPONSE";
    try {
      commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    } catch (ContainerExecutionException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    // Start use invalid options
    handler.response = "INVALID_RESPONSE";
    try {
      commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    } catch (ContainerExecutionException e) {
      caughtException = true;
    }
    Assert.assertTrue(caughtException);

    /* Test get docker run command */
    handler.response = "--device=/dev/nvidiactl --device=/dev/nvidia-uvm "
        + "--device=/dev/nvidia0 --device=/dev/nvidia1 "
        + "--volume-driver=nvidia-docker "
        + "--volume=nvidia_driver_352.68:/usr/local/nvidia:ro";

    commandPlugin.setRequestsGpu(true);
    commandPlugin.updateDockerRunCommand(runCommand, nmContainer);
    Map<String, List<String>> newCommandLine =
        runCommand.getDockerCommandWithArguments();

    // Command line will be updated
    Assert.assertFalse(commandlinesEquals(originalCommandline, newCommandLine));
    // Volume driver should not be included by final commandline
    Assert.assertFalse(newCommandLine.containsKey("volume-driver"));
    Assert.assertTrue(newCommandLine.containsKey("devices"));
    Assert.assertTrue(newCommandLine.containsKey("mounts"));

    /* Test get docker volume command */
    commandPlugin = new MyNvidiaDockerV1CommandPlugin(conf);

    // When requests Gpu == false, returned docker volume command is null,
    Assert.assertNull(commandPlugin.getCreateDockerVolumeCommand(nmContainer));

    // set requests Gpu to true
    commandPlugin.setRequestsGpu(true);

    DockerVolumeCommand dockerVolumeCommand = commandPlugin.getCreateDockerVolumeCommand(
        nmContainer);
    Assert.assertEquals(
        "volume docker-command=volume " + "driver=nvidia-docker "
            + "sub-command=create " + "volume=nvidia_driver_352.68",
        dockerVolumeCommand.toString());
  }
}
