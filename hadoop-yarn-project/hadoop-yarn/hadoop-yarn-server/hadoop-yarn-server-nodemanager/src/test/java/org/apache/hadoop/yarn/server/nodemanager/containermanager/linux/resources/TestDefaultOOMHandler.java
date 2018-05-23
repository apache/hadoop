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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_FILE_TASKS;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_OOM_CONTROL;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler.CGROUP_PARAM_MEMORY_USAGE_BYTES;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test default out of memory handler.
 */
public class TestDefaultOOMHandler {

  /**
   * Test an OOM situation where no containers are running.
   */
  @Test(expected = YarnRuntimeException.class)
  public void testNoContainers() throws Exception {
    Context context = mock(Context.class);

    when(context.getContainers()).thenReturn(new ConcurrentHashMap<>());

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");

    DefaultOOMHandler handler = new DefaultOOMHandler(context, false);
    handler.setCGroupsHandler(cGroupsHandler);

    handler.run();
  }

  /**
   * We have two containers, both out of limit. We should kill the later one.
   *
   * @throws Exception exception
   */
  @Test
  public void testBothContainersOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>(new LinkedHashMap<>());

    Container c1 = mock(Container.class);
    ContainerId cid1 = createContainerId(1);
    when(c1.getContainerId()).thenReturn(cid1);
    when(c1.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c1.getContainerStartTime()).thenReturn((long) 1);
    containers.put(createContainerId(1), c1);

    Container c2 = mock(Container.class);
    ContainerId cid2 = createContainerId(2);
    when(c2.getContainerId()).thenReturn(cid2);
    when(c2.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c2.getContainerStartTime()).thenReturn((long) 2);
    containers.put(cid2, c2);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    ContainerExecutor ex = mock(ContainerExecutor.class);

    runOOMHandler(containers, cGroupsHandler, ex);

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two containers, one out of limit. We should kill that one.
   * This should happen even, if it was started earlier
   *
   * @throws Exception exception
   */
  @Test
  public void testOneContainerOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>(new LinkedHashMap<>());

    Container c1 = mock(Container.class);
    ContainerId cid1 = createContainerId(1);
    when(c1.getContainerId()).thenReturn(cid1);
    when(c1.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c1.getContainerStartTime()).thenReturn((long) 2);
    containers.put(createContainerId(1), c1);

    Container c2 = mock(Container.class);
    ContainerId cid2 = createContainerId(2);
    when(c2.getContainerId()).thenReturn(cid2);
    when(c2.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c2.getContainerStartTime()).thenReturn((long) 1);
    containers.put(cid2, c2);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(11));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(11));

    ContainerExecutor ex = mock(ContainerExecutor.class);
    runOOMHandler(containers, cGroupsHandler, ex);

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  /**
   * We have two containers, neither out of limit. We should kill the later one.
   *
   * @throws Exception exception
   */
  @Test
  public void testNoContainerOOM() throws Exception {
    ConcurrentHashMap<ContainerId, Container> containers =
        new ConcurrentHashMap<>(new LinkedHashMap<>());

    Container c1 = mock(Container.class);
    ContainerId cid1 = createContainerId(1);
    when(c1.getContainerId()).thenReturn(cid1);
    when(c1.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c1.getContainerStartTime()).thenReturn((long) 1);
    containers.put(createContainerId(1), c1);

    Container c2 = mock(Container.class);
    ContainerId cid2 = createContainerId(2);
    when(c2.getContainerId()).thenReturn(cid2);
    when(c2.getResource()).thenReturn(Resource.newInstance(10, 1));
    when(c2.getContainerStartTime()).thenReturn((long) 2);
    containers.put(cid2, c2);

    CGroupsHandler cGroupsHandler = mock(CGroupsHandler.class);
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1234").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid1.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_FILE_TASKS))
        .thenReturn("1235").thenReturn("");
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_USAGE_BYTES))
        .thenReturn(getMB(9));
    when(cGroupsHandler.getCGroupParam(CGroupsHandler.CGroupController.MEMORY,
        cid2.toString(), CGROUP_PARAM_MEMORY_MEMSW_USAGE_BYTES))
        .thenReturn(getMB(9));

    ContainerExecutor ex = mock(ContainerExecutor.class);
    runOOMHandler(containers, cGroupsHandler, ex);

    verify(ex, times(1)).signalContainer(
        new ContainerSignalContext.Builder()
            .setPid("1235")
            .setContainer(c2)
            .setSignal(ContainerExecutor.Signal.KILL)
            .build()
    );
    verify(ex, times(1)).signalContainer(any());
  }

  private void runOOMHandler(
      ConcurrentHashMap<ContainerId, Container> containers,
      CGroupsHandler cGroupsHandler, ContainerExecutor ex)
      throws IOException, ResourceHandlerException {
    Context context = mock(Context.class);
    when(context.getContainers()).thenReturn(containers);

    when(ex.signalContainer(any()))
        .thenAnswer(invocation -> {
          assertEquals("Wrong pid killed", "1235",
              ((ContainerSignalContext) invocation.getArguments()[0]).getPid());
          return true;
        });

    when(cGroupsHandler.getCGroupParam(
        CGroupsHandler.CGroupController.MEMORY,
        "",
        CGROUP_PARAM_MEMORY_OOM_CONTROL))
        .thenReturn("under_oom 1").thenReturn("under_oom 0");

    when(context.getContainerExecutor()).thenReturn(ex);

    DefaultOOMHandler handler = new DefaultOOMHandler(context, false);
    handler.setCGroupsHandler(cGroupsHandler);

    handler.run();
  }

  private class AppId extends ApplicationIdPBImpl {
    AppId(long clusterTs, int appId) {
      this.setClusterTimestamp(clusterTs);
      this.setId(appId);
    }
  }

  private ContainerId createContainerId(int id) {
    ApplicationId applicationId = new AppId(1, 1);

    ApplicationAttemptId applicationAttemptId
        = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(applicationAttemptId.getAttemptId()).thenReturn(1);

    ContainerId containerId = mock(ContainerId.class);
    when(containerId.toString()).thenReturn(Integer.toString(id));
    when(containerId.getContainerId()).thenReturn(new Long(1));

    return containerId;
  }

  ContainerTokenIdentifier getToken() {
    ContainerTokenIdentifier id = mock(ContainerTokenIdentifier.class);
    when(id.getVersion()).thenReturn(1);
    return id;
  }

  String getMB(long mb) {
    return Long.toString(mb * 1024 * 1024);
  }
}