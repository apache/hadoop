/**
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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ProcessTreeInfo;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerExecContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerReapContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.LocalizerStartContext;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestContainersMonitorWithElasticMemory {

  static final Logger LOG = Logger
      .getLogger(TestContainersMonitorWithElasticMemory.class);

  private ContainersMonitorImpl containersMonitor;
  private MockLinuxContainerExecutor executor;
  private Configuration conf;
  private AsyncDispatcher dispatcher;
  private Context context;
  private MockContainerEventHandler containerEventHandler;
  private ConcurrentMap<ContainerId, Container> containerMap;
  private static final String MOCK_EXECUTOR = "mock-container-executor";
  private static final String MOCK_OOM_LISTENER = "mock-oom-listener";
  private String tmpMockExecutor = System.getProperty("test.build.data") +
      "/tmp-mock-container-executor";
  private String tmpMockOOMListener = System.getProperty("test.build.data") +
      "/tmp-mock-oom-listener";
  private final File mockParamFile = new File("./params.txt");

  static class MockLinuxContainerExecutor extends LinuxContainerExecutor {

    @Override
    public String[] getIpAndHost(Container container) {
      String[] ipAndHost = new String[2];
      try {
        InetAddress address = InetAddress.getLocalHost();
        ipAndHost[0] = address.getHostAddress();
        ipAndHost[1] = address.getHostName();
      } catch (UnknownHostException e) {
        LOG.error("Unable to get Local hostname and ip for " + container
            .getContainerId(), e);
      }
      return ipAndHost;
    }

    @Override
    public void startLocalizer(LocalizerStartContext ctx) {
    }

    @Override
    public int launchContainer(ContainerStartContext ctx) throws
        IOException, ConfigurationException {
      return 0;
    }

    @Override
    public int relaunchContainer(ContainerStartContext ctx) throws
        IOException, ConfigurationException {
      return 0;
    }

    @Override
    public boolean signalContainer(ContainerSignalContext ctx)
        throws IOException {
      return true;
    }

    @Override
    public boolean reapContainer(ContainerReapContext ctx) {
      return true;
    }

    @Override
    public IOStreamPair execContainer(ContainerExecContext ctx) {
      return new IOStreamPair(null, null);
    }

    @Override
    public void deleteAsUser(DeletionAsUserContext ctx) {
    }

    @Override
    public void symLink(String target, String symlink) {
    }

    @Override
    public String getProcessId(ContainerId containerId) {
      return String.valueOf(containerId.getContainerId());
    }

    @Override
    public boolean isContainerAlive(ContainerLivenessContext ctx) {
      return true;
    }

    @Override
    public void updateYarnSysFS(Context ctx, String user, String appId, String spec) {
    }

    @Override
    public String getExposedPorts(Container container) {
      return null;
    }
  }

  private static class MockContainerEventHandler implements
      EventHandler<ContainerEvent> {

    final private Set<ContainerId> killedContainer
        = new HashSet<>();

    @Override
    public void handle(ContainerEvent event) {
      if (event.getType() == ContainerEventType.KILL_CONTAINER) {
        synchronized (killedContainer) {
          killedContainer.add(event.getContainerID());
        }
      }
    }

    public boolean isContainerKilled(ContainerId containerId) {
      synchronized (killedContainer) {
        return killedContainer.contains(containerId);
      }
    }
  }

  public String prepareMockFile(String mockFile, String tmpMockFile)
      throws IOException, URISyntaxException {
    URI executorPath = getClass().getClassLoader().getResource(mockFile)
        .toURI();
    Files.copy(Paths.get(executorPath), Paths.get(tmpMockFile),
        REPLACE_EXISTING);

    File executorMockFile = new File(tmpMockFile);

    if (!FileUtil.canExecute(executorMockFile)) {
      FileUtil.setExecutable(executorMockFile, true);
    }
    return executorMockFile.getAbsolutePath();
  }

  @Before
  public void setup() throws Exception {
    dispatcher = new AsyncDispatcher();
    context = Mockito.mock(Context.class);
    containerMap = new ConcurrentSkipListMap<>();
    Container container = Mockito.mock(ContainerImpl.class);
    containerMap.put(getContainerId(1), container);
    Mockito.doReturn(containerMap).when(context).getContainers();
    conf = new Configuration();
    conf.set(
        YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
        MockResourceCalculatorPlugin.class.getCanonicalName());
    conf.set(
        YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
        MockResourceCalculatorProcessTree.class.getCanonicalName());

    String executorPath = prepareMockFile(MOCK_EXECUTOR, tmpMockExecutor);
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH,
        executorPath);
    String oomListenerPath = prepareMockFile(MOCK_OOM_LISTENER, tmpMockOOMListener);
    conf.set(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_OOM_LISTENER_PATH,
        oomListenerPath);
    dispatcher.init(conf);
    dispatcher.start();
    containerEventHandler = new MockContainerEventHandler();
    dispatcher.register(ContainerEventType.class, containerEventHandler);
  }

  private void deleteMockParamFile() {
    if(mockParamFile.exists()) {
      mockParamFile.delete();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (containersMonitor != null) {
      containersMonitor.stop();
    }
    if (dispatcher != null) {
      dispatcher.stop();
    }
    deleteMockParamFile();
  }

  @Test
  public void testContainersResourceChangeForElasticMemoryController() throws Exception {
    conf.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_ELASTIC_MEMORY_CONTROL_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENABLED, true);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED, false);
    executor = new MockLinuxContainerExecutor();
    executor.setConf(conf);

    try {
      executor.init(context);
    } catch (IOException e) {
    }

    containersMonitor = createContainersMonitor(executor);
    containersMonitor.init(conf);
    containersMonitor.start();
    // create container 1
    containersMonitor.handle(new ContainerStartMonitoringEvent(
        getContainerId(1), 2100L, 1000L, 1, 0, 0));
    assertNotNull(getProcessTreeInfo(getContainerId(1)));
    assertEquals(1000L, getProcessTreeInfo(getContainerId(1))
        .getPmemLimit());
    assertEquals(2100L, getProcessTreeInfo(getContainerId(1))
        .getVmemLimit());
    // sleep longer than the monitor interval to make sure resource
    // enforcement has started
    Thread.sleep(20000);
    MockResourceCalculatorProcessTree mockTree =
        (MockResourceCalculatorProcessTree) getProcessTreeInfo(
            getContainerId(1)).getProcessTree();
    mockTree.setRssMemorySize(2500L);
    Thread.sleep(200);
    assertFalse(containerEventHandler
        .isContainerKilled(getContainerId(1)));
    containersMonitor.stop();
  }

  private ContainersMonitorImpl createContainersMonitor(ContainerExecutor containerExecutor) {
    return new ContainersMonitorImpl(containerExecutor, dispatcher, context);
  }

  private ContainerId getContainerId(int id) {
    return ContainerId.newContainerId(ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456L, 1), 1), id);
  }

  private ProcessTreeInfo getProcessTreeInfo(ContainerId id) {
    return containersMonitor.trackingContainers.get(id);
  }
}
