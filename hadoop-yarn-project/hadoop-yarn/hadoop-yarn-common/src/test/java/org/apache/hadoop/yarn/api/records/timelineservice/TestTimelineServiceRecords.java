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
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class TestTimelineServiceRecords {
  private static final Log LOG =
      LogFactory.getLog(TestTimelineServiceRecords.class);

  @Test
  public void testTimelineEntities() throws Exception {
    TimelineEntity entity = new TimelineEntity();
    entity.setType("test type 1");
    entity.setId("test id 1");
    entity.addInfo("test info key 1", "test info value 1");
    entity.addInfo("test info key 2",
        Arrays.asList("test info value 2", "test info value 3"));
    entity.addInfo("test info key 3", true);
    Assert.assertTrue(
        entity.getInfo().get("test info key 3") instanceof Boolean);
    entity.addConfig("test config key 1", "test config value 1");
    entity.addConfig("test config key 2", "test config value 2");

    TimelineMetric metric1 =
        new TimelineMetric(TimelineMetric.Type.TIME_SERIES);
    metric1.setId("test metric id 1");
    metric1.addValue(1L, 1.0F);
    metric1.addValue(3L, 3.0D);
    metric1.addValue(2L, 2);
    Assert.assertEquals(TimelineMetric.Type.TIME_SERIES, metric1.getType());
    Iterator<Map.Entry<Long, Number>> itr =
        metric1.getValues().entrySet().iterator();
    Map.Entry<Long, Number> entry = itr.next();
    Assert.assertEquals(new Long(3L), entry.getKey());
    Assert.assertEquals(3.0D, entry.getValue());
    entry = itr.next();
    Assert.assertEquals(new Long(2L), entry.getKey());
    Assert.assertEquals(2, entry.getValue());
    entry = itr.next();
    Assert.assertEquals(new Long(1L), entry.getKey());
    Assert.assertEquals(1.0F, entry.getValue());
    Assert.assertFalse(itr.hasNext());
    entity.addMetric(metric1);

    TimelineMetric metric2 =
        new TimelineMetric(TimelineMetric.Type.SINGLE_VALUE);
    metric2.setId("test metric id 1");
    metric2.addValue(3L, (short) 3);
    Assert.assertEquals(TimelineMetric.Type.SINGLE_VALUE, metric2.getType());
    Assert.assertTrue(
        metric2.getValues().values().iterator().next() instanceof Short);
    Map<Long, Number> points = new HashMap<>();
    points.put(4L, 4.0D);
    points.put(5L, 5.0D);
    try {
      metric2.setValues(points);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Values cannot contain more than one point in"));
    }
    try {
      metric2.addValues(points);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains(
          "Values cannot contain more than one point in"));
    }
    entity.addMetric(metric2);

    TimelineMetric metric3 =
        new TimelineMetric(TimelineMetric.Type.SINGLE_VALUE);
    metric3.setId("test metric id 1");
    metric3.addValue(4L, (short) 4);
    Assert.assertEquals("metric3 should equal to metric2! ", metric3, metric2);
    Assert.assertNotEquals("metric1 should not equal to metric2! ",
        metric1, metric2);

    TimelineEvent event1 = new TimelineEvent();
    event1.setId("test event id 1");
    event1.addInfo("test info key 1", "test info value 1");
    event1.addInfo("test info key 2",
        Arrays.asList("test info value 2", "test info value 3"));
    event1.addInfo("test info key 3", true);
    Assert.assertTrue(
        event1.getInfo().get("test info key 3") instanceof Boolean);
    event1.setTimestamp(1L);
    entity.addEvent(event1);

    TimelineEvent event2 = new TimelineEvent();
    event2.setId("test event id 2");
    event2.addInfo("test info key 1", "test info value 1");
    event2.addInfo("test info key 2",
        Arrays.asList("test info value 2", "test info value 3"));
    event2.addInfo("test info key 3", true);
    Assert.assertTrue(
        event2.getInfo().get("test info key 3") instanceof Boolean);
    event2.setTimestamp(2L);
    entity.addEvent(event2);

    Assert.assertFalse("event1 should not equal to event2! ",
        event1.equals(event2));
    TimelineEvent event3 = new TimelineEvent();
    event3.setId("test event id 1");
    event3.setTimestamp(1L);
    Assert.assertEquals("event1 should equal to event3! ", event3, event1);
    Assert.assertNotEquals("event1 should not equal to event2! ",
        event1, event2);

    entity.setCreatedTime(0L);
    entity.addRelatesToEntity("test type 2", "test id 2");
    entity.addRelatesToEntity("test type 3", "test id 3");
    entity.addIsRelatedToEntity("test type 4", "test id 4");
    entity.addIsRelatedToEntity("test type 5", "test id 5");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(entity, true));

    TimelineEntities entities = new TimelineEntities();
    TimelineEntity entity1 = new TimelineEntity();
    entities.addEntity(entity1);
    TimelineEntity entity2 = new TimelineEntity();
    entities.addEntity(entity2);
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(entities, true));

    Assert.assertFalse("entity 1 should not be valid without type and id",
        entity1.isValid());
    entity1.setId("test id 2");
    entity1.setType("test type 2");
    entity2.setId("test id 1");
    entity2.setType("test type 1");

    Assert.assertEquals("Timeline entity should equal to entity2! ",
        entity, entity2);
    Assert.assertNotEquals("entity1 should not equal to entity! ",
        entity1, entity);
    Assert.assertEquals("entity should be less than entity1! ",
        entity1.compareTo(entity), 1);
    Assert.assertEquals("entity's hash code should be -28727840 but not "
        + entity.hashCode(), entity.hashCode(), -28727840);
  }

  @Test
  public void testFirstClassCitizenEntities() throws Exception {
    UserEntity user = new UserEntity();
    user.setId("test user id");

    QueueEntity queue = new QueueEntity();
    queue.setId("test queue id");


    ClusterEntity cluster = new ClusterEntity();
    cluster.setId("test cluster id");

    FlowRunEntity flow1 = new FlowRunEntity();
    //flow1.setId("test flow id 1");
    flow1.setUser(user.getId());
    flow1.setName("test flow name 1");
    flow1.setVersion("test flow version 1");
    flow1.setRunId(1L);

    FlowRunEntity flow2 = new FlowRunEntity();
    //flow2.setId("test flow run id 2");
    flow2.setUser(user.getId());
    flow2.setName("test flow name 2");
    flow2.setVersion("test flow version 2");
    flow2.setRunId(2L);

    ApplicationEntity app1 = new ApplicationEntity();
    app1.setId(ApplicationId.newInstance(0, 1).toString());
    app1.setQueue(queue.getId());

    ApplicationEntity app2 = new ApplicationEntity();
    app2.setId(ApplicationId.newInstance(0, 2).toString());
    app2.setQueue(queue.getId());

    ApplicationAttemptEntity appAttempt = new ApplicationAttemptEntity();
    appAttempt.setId(ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(0, 1), 1).toString());

    ContainerEntity container = new ContainerEntity();
    container.setId(ContainerId.newContainerId(
        ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(0, 1), 1), 1).toString());

    cluster.addChild(TimelineEntityType.YARN_FLOW_RUN.toString(),
        flow1.getId());
    flow1
        .setParent(TimelineEntityType.YARN_CLUSTER.toString(), cluster.getId());
    flow1.addChild(TimelineEntityType.YARN_FLOW_RUN.toString(), flow2.getId());
    flow2.setParent(TimelineEntityType.YARN_FLOW_RUN.toString(), flow1.getId());
    flow2.addChild(TimelineEntityType.YARN_APPLICATION.toString(),
        app1.getId());
    flow2.addChild(TimelineEntityType.YARN_APPLICATION.toString(),
        app2.getId());
    app1.setParent(TimelineEntityType.YARN_FLOW_RUN.toString(), flow2.getId());
    app1.addChild(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
        appAttempt.getId());
    appAttempt
        .setParent(TimelineEntityType.YARN_APPLICATION.toString(),
            app1.getId());
    app2.setParent(TimelineEntityType.YARN_FLOW_RUN.toString(), flow2.getId());
    appAttempt.addChild(TimelineEntityType.YARN_CONTAINER.toString(),
        container.getId());
    container.setParent(TimelineEntityType.YARN_APPLICATION_ATTEMPT.toString(),
        appAttempt.getId());

    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(cluster, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(flow1, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(flow2, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(app1, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(app2, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(appAttempt, true));
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(container, true));


    // Check parent/children APIs
    Assert.assertNotNull(app1.getParent());
    Assert.assertEquals(flow2.getType(), app1.getParent().getType());
    Assert.assertEquals(flow2.getId(), app1.getParent().getId());
    app1.addInfo(ApplicationEntity.PARENT_INFO_KEY, "invalid parent object");
    try {
      app1.getParent();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YarnRuntimeException);
      Assert.assertTrue(e.getMessage().contains(
          "Parent info is invalid identifier object"));
    }

    Assert.assertNotNull(app1.getChildren());
    Assert.assertEquals(1, app1.getChildren().size());
    Assert.assertEquals(
        appAttempt.getType(), app1.getChildren().iterator().next().getType());
    Assert.assertEquals(
        appAttempt.getId(), app1.getChildren().iterator().next().getId());
    app1.addInfo(ApplicationEntity.CHILDREN_INFO_KEY,
        Collections.singletonList("invalid children set"));
    try {
      app1.getChildren();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YarnRuntimeException);
      Assert.assertTrue(e.getMessage().contains(
          "Children info is invalid identifier set"));
    }
    app1.addInfo(ApplicationEntity.CHILDREN_INFO_KEY,
        Collections.singleton("invalid child object"));
    try {
      app1.getChildren();
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof YarnRuntimeException);
      Assert.assertTrue(e.getMessage().contains(
          "Children info contains invalid identifier object"));
    }
  }

  @Test
  public void testUser() throws Exception {
    UserEntity user = new UserEntity();
    user.setId("test user id");
    user.addInfo("test info key 1", "test info value 1");
    user.addInfo("test info key 2", "test info value 2");
    LOG.info(TimelineUtils.dumpTimelineRecordtoJSON(user, true));
  }

  @Test
  public void testQueue() throws Exception {
    QueueEntity queue = new QueueEntity();
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
