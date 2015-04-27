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

package org.apache.hadoop.yarn.server.timelineservice;


import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.*;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.collector.NodeTimelineCollectorManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class TestTimelineServiceClientIntegration {
  private static NodeTimelineCollectorManager collectorManager;
  private static PerNodeTimelineCollectorsAuxService auxService;

  @BeforeClass
  public static void setupClass() throws Exception {
    try {
      collectorManager = new MockNodeTimelineCollectorManager();
      auxService =
          PerNodeTimelineCollectorsAuxService.launchServer(new String[0],
              collectorManager);
      auxService.addApplication(ApplicationId.newInstance(0, 1));
    } catch (ExitUtil.ExitException e) {
      fail();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (auxService != null) {
      auxService.stop();
    }
  }

  @Test
  public void testPutEntities() throws Exception {
    TimelineClient client =
        TimelineClient.createTimelineClient(ApplicationId.newInstance(0, 1));
    try {
      // set the timeline service address manually
      client.setTimelineServiceAddress(
          collectorManager.getRestServerBindAddress());
      client.init(new YarnConfiguration());
      client.start();
      TimelineEntity entity = new TimelineEntity();
      entity.setType("test entity type");
      entity.setId("test entity id");
      client.putEntities(entity);
      client.putEntitiesAsync(entity);
    } finally {
      client.stop();
    }
  }

  @Test
  public void testPutExtendedEntities() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    TimelineClient client =
        TimelineClient.createTimelineClient(appId);
    try {
      // set the timeline service address manually
      client.setTimelineServiceAddress(
          collectorManager.getRestServerBindAddress());
      client.init(new YarnConfiguration());
      client.start();
      ClusterEntity cluster = new ClusterEntity();
      cluster.setId(YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
      FlowEntity flow = new FlowEntity();
      flow.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
      flow.setName("test_flow_name");
      flow.setVersion("test_flow_version");
      flow.setRunId(1L);
      flow.setParent(cluster.getType(), cluster.getId());
      ApplicationEntity app = new ApplicationEntity();
      app.setId(appId.toString());
      flow.addChild(app.getType(), app.getId());
      ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(appId, 1);
      ApplicationAttemptEntity appAttempt = new ApplicationAttemptEntity();
      appAttempt.setId(attemptId.toString());
      ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
      ContainerEntity container = new ContainerEntity();
      container.setId(containerId.toString());
      UserEntity user = new UserEntity();
      user.setId(UserGroupInformation.getCurrentUser().getShortUserName());
      QueueEntity queue = new QueueEntity();
      queue.setId("default_queue");
      client.putEntities(cluster, flow, app, appAttempt, container, user, queue);
      client.putEntitiesAsync(cluster, flow, app, appAttempt, container, user, queue);
    } finally {
      client.stop();
    }
  }

  private static class MockNodeTimelineCollectorManager extends
      NodeTimelineCollectorManager {
    public MockNodeTimelineCollectorManager() {
      super();
    }

    @Override
    protected CollectorNodemanagerProtocol getNMCollectorService() {
      CollectorNodemanagerProtocol protocol =
          mock(CollectorNodemanagerProtocol.class);
      try {
        GetTimelineCollectorContextResponse response =
            GetTimelineCollectorContextResponse.newInstance(null, null, null, 0L);
        when(protocol.getTimelineCollectorContext(any(
            GetTimelineCollectorContextRequest.class))).thenReturn(response);
      } catch (YarnException | IOException e) {
        fail();
      }
      return protocol;
    }
  }
}
