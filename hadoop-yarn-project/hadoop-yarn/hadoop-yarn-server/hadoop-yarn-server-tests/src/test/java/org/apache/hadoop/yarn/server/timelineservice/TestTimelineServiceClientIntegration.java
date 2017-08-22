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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationAttemptEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ApplicationEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ClusterEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.ContainerEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.FlowRunEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.QueueEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.api.records.timelineservice.UserEntity;
import org.apache.hadoop.yarn.client.api.TimelineV2Client;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.CollectorNodemanagerProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;
import org.apache.hadoop.yarn.server.timelineservice.collector.NodeTimelineCollectorManager;
import org.apache.hadoop.yarn.server.timelineservice.collector.PerNodeTimelineCollectorsAuxService;
import org.apache.hadoop.yarn.server.timelineservice.storage.FileSystemTimelineWriterImpl;
import org.apache.hadoop.yarn.server.timelineservice.storage.TimelineWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTimelineServiceClientIntegration {
  private static final String ROOT_DIR = new File("target",
      TestTimelineServiceClientIntegration.class.getSimpleName()).
          getAbsolutePath();
  private static NodeTimelineCollectorManager collectorManager;
  private static PerNodeTimelineCollectorsAuxService auxService;
  private static Configuration conf;

  @BeforeClass
  public static void setupClass() throws Exception {
    try {
      collectorManager = new MockNodeTimelineCollectorManager();
      conf = new YarnConfiguration();
      // enable timeline service v.2
      conf.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED, true);
      conf.setFloat(YarnConfiguration.TIMELINE_SERVICE_VERSION, 2.0f);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_WRITER_CLASS,
          FileSystemTimelineWriterImpl.class, TimelineWriter.class);
      conf.set(FileSystemTimelineWriterImpl.TIMELINE_SERVICE_STORAGE_DIR_ROOT,
          ROOT_DIR);
      auxService =
          PerNodeTimelineCollectorsAuxService.launchServer(new String[0],
              collectorManager, conf);
      auxService.addApplication(ApplicationId.newInstance(0, 1), "user");
    } catch (ExitUtil.ExitException e) {
      fail();
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (auxService != null) {
      auxService.stop();
    }
    FileUtils.deleteDirectory(new File(ROOT_DIR));
  }

  @Test
  public void testPutEntities() throws Exception {
    TimelineV2Client client =
        TimelineV2Client.createTimelineClient(ApplicationId.newInstance(0, 1));
    try {
      // Set the timeline service address manually.
      client.setTimelineCollectorInfo(CollectorInfo.newInstance(
          collectorManager.getRestServerBindAddress()));
      client.init(conf);
      client.start();
      TimelineEntity entity = new TimelineEntity();
      entity.setType("test entity type");
      entity.setId("test entity id");
      TimelineMetric metric =
          new TimelineMetric(TimelineMetric.Type.TIME_SERIES);
      metric.setId("test metric id");
      metric.addValue(1L, 1.0D);
      metric.addValue(2L, 2.0D);
      entity.addMetric(metric);
      client.putEntities(entity);
      client.putEntitiesAsync(entity);
    } finally {
      client.stop();
    }
  }

  @Test
  public void testPutExtendedEntities() throws Exception {
    ApplicationId appId = ApplicationId.newInstance(0, 1);
    TimelineV2Client client =
        TimelineV2Client.createTimelineClient(appId);
    try {
      // Set the timeline service address manually.
      client.setTimelineCollectorInfo(CollectorInfo.newInstance(
          collectorManager.getRestServerBindAddress()));
      client.init(conf);
      client.start();
      ClusterEntity cluster = new ClusterEntity();
      cluster.setId(YarnConfiguration.DEFAULT_RM_CLUSTER_ID);
      FlowRunEntity flow = new FlowRunEntity();
      flow.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
      flow.setName("test_flow_name");
      flow.setVersion("test_flow_version");
      flow.setRunId(1L);
      flow.setParent(cluster.getType(), cluster.getId());
      ApplicationEntity app = new ApplicationEntity();
      app.setId(appId.toString());
      flow.addChild(app.getType(), app.getId());
      ApplicationAttemptId attemptId =
          ApplicationAttemptId.newInstance(appId, 1);
      ApplicationAttemptEntity appAttempt = new ApplicationAttemptEntity();
      appAttempt.setId(attemptId.toString());
      ContainerId containerId = ContainerId.newContainerId(attemptId, 1);
      ContainerEntity container = new ContainerEntity();
      container.setId(containerId.toString());
      UserEntity user = new UserEntity();
      user.setId(UserGroupInformation.getCurrentUser().getShortUserName());
      QueueEntity queue = new QueueEntity();
      queue.setId("default_queue");
      client.putEntities(cluster, flow, app, appAttempt, container, user,
          queue);
      client.putEntitiesAsync(cluster, flow, app, appAttempt, container, user,
          queue);
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
            GetTimelineCollectorContextResponse.newInstance(
                UserGroupInformation.getCurrentUser().getShortUserName(),
                "test_flow_name", "test_flow_version", 1L);
        when(protocol.getTimelineCollectorContext(any(
            GetTimelineCollectorContextRequest.class))).thenReturn(response);
      } catch (YarnException | IOException e) {
        fail();
      }
      return protocol;
    }
  }
}
