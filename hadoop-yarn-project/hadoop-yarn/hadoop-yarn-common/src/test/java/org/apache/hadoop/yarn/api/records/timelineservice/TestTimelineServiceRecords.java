/*
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
package org.apache.hadoop.yarn.api.records.timelineservice;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Test;


public class TestTimelineServiceRecords {
  private static final Log LOG =
      LogFactory.getLog(TestTimelineServiceRecords.class);

  @Test
  public void testTimelineEntities() throws Exception {
    TimelineEntity entity = new TimelineEntity();
    entity.setType("test type 1");
    entity.setId("test id 1");
    entity.addInfo("test info key 1", "test info value 1");
    entity.addInfo("test info key 2", "test info value 2");
    entity.addConfig("test config key 1", "test config value 1");
    entity.addConfig("test config key 2", "test config value 2");
    TimelineMetric metric1 = new TimelineMetric();
    metric1.setId("test metric id 1");
    metric1.addInfo("test info key 1", "test info value 1");
    metric1.addInfo("test info key 2", "test info value 2");
    metric1.addTimeSeriesData(1L, "test time series 1");
    metric1.addTimeSeriesData(2L, "test time series 2");
    metric1.setStartTime(0L);
    metric1.setEndTime(1L);
    entity.addMetric(metric1);
    TimelineMetric metric2 = new TimelineMetric();
    metric2.setId("test metric id 1");
    metric2.addInfo("test info key 1", "test info value 1");
    metric2.addInfo("test info key 2", "test info value 2");
    metric2.setSingleData("test info value 3");
    metric1.setStartTime(0L);
    metric1.setEndTime(1L);
    entity.addMetric(metric2);
    TimelineEvent event1 = new TimelineEvent();
    event1.setId("test event id 1");
    event1.addInfo("test info key 1", "test info value 1");
    event1.addInfo("test info key 2", "test info value 2");
    event1.setTimestamp(0L);
    entity.addEvent(event1);
    TimelineEvent event2 = new TimelineEvent();
    event2.setId("test event id 2");
    event2.addInfo("test info key 1", "test info value 1");
    event2.addInfo("test info key 2", "test info value 2");
    event2.setTimestamp(1L);
    entity.addEvent(event2);
    entity.setCreatedTime(0L);
    entity.setModifiedTime(1L);
    entity.addRelatesToEntity("test type 2", "test id 2");
    entity.addRelatesToEntity("test type 3", "test id 3");
    entity.addIsRelatedToEntity("test type 4", "test id 4");
    entity.addIsRelatedToEntity("test type 5", "test id 5");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(entity, true));
  }

  @Test
  public void testFirstClassCitizenEntities() throws Exception {
    TimelineUser user = new TimelineUser();
    user.setId("test user id");

    TimelineQueue queue = new TimelineQueue();
    queue.setId("test queue id");


    ClusterEntity cluster = new ClusterEntity();
    cluster.setId("test cluster id");

    FlowEntity flow1 = new FlowEntity();
    flow1.setId("test flow id");
    flow1.setUser(user.getId());
    flow1.setVersion("test flow version");
    flow1.setRun("test run 1");

    FlowEntity flow2 = new FlowEntity();
    flow2.setId("test flow run id2");
    flow2.setUser(user.getId());
    flow1.setVersion("test flow version2");
    flow2.setRun("test run 2");

    ApplicationEntity app = new ApplicationEntity();
    app.setId(ApplicationId.newInstance(0, 1).toString());
    app.setQueue(queue.getId());

    ApplicationAttemptEntity appAttempt = new ApplicationAttemptEntity();
    appAttempt.setId(ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0, 1), 1).toString());

    ContainerEntity container = new ContainerEntity();
    container.setId(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1).toString());

    cluster.addChild(TimelineEntityType.YARN_FLOW.toString(), flow1.getId());
    flow1
        .setParent(TimelineEntityType.YARN_CLUSTER.toString(), cluster.getId());
    flow1.addChild(TimelineEntityType.YARN_FLOW.toString(), flow2.getId());
    flow2.setParent(TimelineEntityType.YARN_FLOW.toString(), flow1.getId());
    flow2.addChild(TimelineEntityType.YARN_APPLICATION.toString(), app.getId());
    app.setParent(TimelineEntityType.YARN_FLOW.toString(), flow2.getId());
    app.addChild(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
        appAttempt.getId());
    appAttempt
        .setParent(TimelineEntityType.YARN_APPLICATION.toString(), app.getId());
    appAttempt.addChild(TimelineEntityType.YARN_CONTAINER.toString(),
        container.getId());
    container.setParent(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
        appAttempt.getId());

    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(cluster, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(flow1, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(flow2, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(app, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(appAttempt, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(container, true));
  }

  @Test
  public void testUser() throws Exception {
    TimelineUser user = new TimelineUser();
    user.setId("test user id");
    user.addInfo("test info key 1", "test info value 1");
    user.addInfo("test info key 2", "test info value 2");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(user, true));
  }

  @Test
  public void testQueue() throws Exception {
    TimelineQueue queue = new TimelineQueue();
    queue.setId("test queue id");
    queue.addInfo("test info key 1", "test info value 1");
    queue.addInfo("test info key 2", "test info value 2");
    queue.setParent(TimelineEntityType.YARN_QUEUE.toString(),
        "test parent queue id");
    queue.addChild(TimelineEntityType.YARN_QUEUE.toString(),
        "test child queue id 1");
    queue.addChild(TimelineEntityType.YARN_QUEUE.toString(),
        "test child queue id 2");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(queue, true));
  }
}
