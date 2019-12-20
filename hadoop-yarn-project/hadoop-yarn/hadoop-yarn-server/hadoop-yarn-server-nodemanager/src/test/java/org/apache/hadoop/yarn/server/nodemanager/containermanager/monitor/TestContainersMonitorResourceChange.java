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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ProcessTreeInfo;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TestContainersMonitorResourceChange {

  static final Logger LOG = Logger
      .getLogger(TestContainersMonitorResourceChange.class);
  private ContainersMonitorImpl containersMonitor;
  private MockExecutor executor;
  private Configuration conf;
  private AsyncDispatcher dispatcher;
  private Context context;
  private MockContainerEventHandler containerEventHandler;
  private ConcurrentMap<ContainerId, Container> containerMap;

  static final int WAIT_MS_PER_LOOP = 20; // 20 milli seconds

  private static class MockExecutor extends ContainerExecutor {
    @Override
    public void init(Context nmContext) throws IOException {
    }
    @Override
    public void startLocalizer(LocalizerStartContext ctx)
        throws IOException, InterruptedException {
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
    public boolean reapContainer(ContainerReapContext ctx)
        throws IOException {
      return true;
    }

    @Override
    public IOStreamPair execContainer(ContainerExecContext ctx)
        throws ContainerExecutionException {
      return new IOStreamPair(null, null);
    }

    @Override
    public void deleteAsUser(DeletionAsUserContext ctx)
        throws IOException, InterruptedException {
    }

    @Override
    public void symLink(String target, String symlink)
        throws IOException {

    }

    @Override
    public String getProcessId(ContainerId containerId) {
      return String.valueOf(containerId.getContainerId());
    }
    @Override
    public boolean isContainerAlive(ContainerLivenessContext ctx)
        throws IOException {
      return true;
    }
    @Override
    public void updateYarnSysFS(Context ctx, String user, String appId,
        String spec) throws IOException {
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

  @Before
  public void setup() {
    executor = new MockExecutor();
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
    dispatcher.init(conf);
    dispatcher.start();
    containerEventHandler = new MockContainerEventHandler();
    dispatcher.register(ContainerEventType.class, containerEventHandler);
  }

  @After
  public void tearDown() throws Exception {
    if (containersMonitor != null) {
      containersMonitor.stop();
    }
    if (dispatcher != null) {
      dispatcher.stop();
    }
  }

  @Test
  public void testContainersResourceChangePolling() throws Exception {
    // set container monitor interval to be 20ms
    conf.setLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS, 20L);
    conf.setBoolean(YarnConfiguration.NM_MEMORY_RESOURCE_ENFORCED, false);
    containersMonitor = createContainersMonitor(executor, dispatcher, context);
    containersMonitor.init(conf);
    containersMonitor.start();
    // create container 1
    containersMonitor.handle(new ContainerStartMonitoringEvent(
        getContainerId(1), 2100L, 1000L, 1, 0, 0));
    // verify that this container is properly tracked
    assertNotNull(getProcessTreeInfo(getContainerId(1)));
    assertEquals(1000L, getProcessTreeInfo(getContainerId(1))
        .getPmemLimit());
    assertEquals(2100L, getProcessTreeInfo(getContainerId(1))
        .getVmemLimit());
    // sleep longer than the monitor interval to make sure resource
    // enforcement has started
    Thread.sleep(200);
    // increase pmem usage, the container should be killed
    MockResourceCalculatorProcessTree mockTree =
        (MockResourceCalculatorProcessTree) getProcessTreeInfo(
            getContainerId(1)).getProcessTree();
    mockTree.setRssMemorySize(2500L);
    // verify that this container is killed
    for (int waitMs = 0; waitMs < 5000; waitMs += 50) {
      if (containerEventHandler.isContainerKilled(getContainerId(1))) {
        break;
      }
      Thread.sleep(50);
    }
    assertTrue(containerEventHandler
        .isContainerKilled(getContainerId(1)));
    // create container 2
    containersMonitor.handle(new ContainerStartMonitoringEvent(getContainerId(
        2), 2202009L, 1048576L, 1, 0, 0));
    // verify that this container is properly tracked
    assertNotNull(getProcessTreeInfo(getContainerId(2)));
    assertEquals(1048576L, getProcessTreeInfo(getContainerId(2))
        .getPmemLimit());
    assertEquals(2202009L, getProcessTreeInfo(getContainerId(2))
        .getVmemLimit());
    // trigger a change resource event, check limit after change
    containersMonitor.handle(new ChangeMonitoringContainerResourceEvent(
        getContainerId(2), Resource.newInstance(2, 1)));
    assertEquals(2097152L, getProcessTreeInfo(getContainerId(2))
        .getPmemLimit());
    assertEquals(4404019L, getProcessTreeInfo(getContainerId(2))
        .getVmemLimit());
    // sleep longer than the monitor interval to make sure resource
    // enforcement has started
    Thread.sleep(200);
    // increase pmem usage, the container should NOT be killed
    mockTree =
        (MockResourceCalculatorProcessTree) getProcessTreeInfo(
            getContainerId(2)).getProcessTree();
    mockTree.setRssMemorySize(2000000L);
    // verify that this container is not killed
    Thread.sleep(200);
    assertFalse(containerEventHandler
        .isContainerKilled(getContainerId(2)));
    containersMonitor.stop();
  }

  @Test
  public void testContainersResourceChangeIsTriggeredImmediately()
      throws Exception {
    // set container monitor interval to be 20s
    conf.setLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS, 20000L);
    containersMonitor = createContainersMonitor(executor, dispatcher, context);
    containersMonitor.init(conf);
    containersMonitor.start();
    // sleep 1 second to make sure the container monitor thread is
    // now waiting for the next monitor cycle
    Thread.sleep(1000);
    // create a container with id 3
    containersMonitor.handle(new ContainerStartMonitoringEvent(getContainerId(
        3), 2202009L, 1048576L, 1, 0, 0));
    // Verify that this container has been tracked
    assertNotNull(getProcessTreeInfo(getContainerId(3)));
    // trigger a change resource event, check limit after change
    containersMonitor.handle(new ChangeMonitoringContainerResourceEvent(
        getContainerId(3), Resource.newInstance(2, 1)));
    // verify that this container has been properly tracked with the
    // correct size
    assertEquals(2097152L, getProcessTreeInfo(getContainerId(3))
        .getPmemLimit());
    assertEquals(4404019L, getProcessTreeInfo(getContainerId(3))
        .getVmemLimit());
    containersMonitor.stop();
  }

  @Test
  public void testContainersCPUResourceForDefaultValue() throws Exception {
    Configuration newConf = new Configuration(conf);
    // set container monitor interval to be 20s
    newConf.setLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS, 20L);
    containersMonitor = createContainersMonitor(executor, dispatcher, context);
    newConf.set(YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
        MockCPUResourceCalculatorProcessTree.class.getCanonicalName());
    // set container monitor interval to be 20ms
    containersMonitor.init(newConf);
    containersMonitor.start();

    // create container 1
    containersMonitor.handle(new ContainerStartMonitoringEvent(
        getContainerId(1), 2100L, 1000L, 1, 0, 0));

    // Verify the container utilization value.
    // Since MockCPUResourceCalculatorProcessTree will return a -1 as CPU
    // utilization, containersUtilization will not be calculated and hence it
    // will be 0.
    assertEquals(
        "Resource utilization must be default with MonitorThread's first run",
        0, containersMonitor.getContainersUtilization()
            .compareTo(ResourceUtilization.newInstance(0, 0, 0.0f)));

    // Verify the container utilization value. Since atleast one round is done,
    // we can expect a non-zero value for container utilization as
    // MockCPUResourceCalculatorProcessTree#getCpuUsagePercent will return 50.
    waitForContainerResourceUtilizationChange(containersMonitor, 100);

    containersMonitor.stop();
  }

  public static void waitForContainerResourceUtilizationChange(
      ContainersMonitorImpl containersMonitor, int timeoutMsecs)
      throws InterruptedException {
    int timeWaiting = 0;
    while (0 == containersMonitor.getContainersUtilization()
        .compareTo(ResourceUtilization.newInstance(0, 0, 0.0f))) {
      if (timeWaiting >= timeoutMsecs) {
        break;
      }

      LOG.info(
          "Monitor thread is waiting for resource utlization change.");
      Thread.sleep(WAIT_MS_PER_LOOP);
      timeWaiting += WAIT_MS_PER_LOOP;
    }

    assertTrue("Resource utilization is not changed from second run onwards",
        0 != containersMonitor.getContainersUtilization()
            .compareTo(ResourceUtilization.newInstance(0, 0, 0.0f)));
  }

  private ContainersMonitorImpl createContainersMonitor(
      ContainerExecutor containerExecutor, AsyncDispatcher dispatcher,
      Context context) {
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
