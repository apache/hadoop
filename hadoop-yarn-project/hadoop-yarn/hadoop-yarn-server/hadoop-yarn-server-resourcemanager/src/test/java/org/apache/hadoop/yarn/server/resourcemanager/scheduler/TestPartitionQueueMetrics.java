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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueueMetrics;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPartitionQueueMetrics {

  static final int GB = 1024; // MB
  private static final Configuration CONF = new Configuration();

  private MetricsSystem ms;

  @Before
  public void setUp() {
    ms = new MetricsSystemImpl();
    QueueMetrics.clearQueueMetrics();
    PartitionQueueMetrics.clearQueueMetrics();
  }

  @After
  public void tearDown() {
    ms.shutdown();
  }

  /**
   * Structure:
   * Both queues, q1 & q2 has been configured to run in only 1 partition, x.
   *
   * root
   * / \
   * q1 q2
   *
   * @throws Exception
   */

  @Test
  public void testSinglePartitionWithSingleLevelQueueMetrics()
      throws Exception {

    String parentQueueName = "root";
    Queue parentQueue = mock(Queue.class);
    String user = "alice";

    QueueMetrics root = QueueMetrics.forQueue(ms, "root", null, true, CONF);
    when(parentQueue.getMetrics()).thenReturn(root);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);
    QueueMetrics q1 =
        QueueMetrics.forQueue(ms, "root.q1", parentQueue, true, CONF);
    QueueMetrics q2 =
        QueueMetrics.forQueue(ms, "root.q2", parentQueue, true, CONF);

    q1.submitApp(user);
    q1.submitAppAttempt(user);

    root.setAvailableResourcesToQueue("x",
        Resources.createResource(200 * GB, 200));
    q1.setAvailableResourcesToQueue("x",
        Resources.createResource(100 * GB, 100));

    q1.incrPendingResources("x", user, 2, Resource.newInstance(1024, 1));

    MetricsSource partitionSource = partitionSource(ms, "x");
    MetricsSource rootQueueSource = queueSource(ms, "x", parentQueueName);
    MetricsSource q1Source = queueSource(ms, "x", "root.q1");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 2 * GB, 2, 2);

    q2.incrPendingResources("x", user, 3, Resource.newInstance(1024, 1));
    MetricsSource q2Source = queueSource(ms, "x", "root.q2");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 5 * GB, 5, 5);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 5 * GB, 5, 5);
    checkResources(q2Source, 0, 0, 0, 0, 0, 3 * GB, 3, 3);
  }

  /**
   * Structure:
   * Both queues, q1 & q2 has been configured to run in both partitions, x & y.
   *
   * root
   * / \
   * q1 q2
   *
   * @throws Exception
   */
  @Test
  public void testTwoPartitionWithSingleLevelQueueMetrics() throws Exception {

    String parentQueueName = "root";
    String user = "alice";

    QueueMetrics root =
        QueueMetrics.forQueue(ms, parentQueueName, null, false, CONF);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getMetrics()).thenReturn(root);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);

    QueueMetrics q1 =
        QueueMetrics.forQueue(ms, "root.q1", parentQueue, false, CONF);
    QueueMetrics q2 =
        QueueMetrics.forQueue(ms, "root.q2", parentQueue, false, CONF);

    AppSchedulingInfo app = mockApp(user);
    q1.submitApp(user);
    q1.submitAppAttempt(user);

    root.setAvailableResourcesToQueue("x",
        Resources.createResource(200 * GB, 200));
    q1.setAvailableResourcesToQueue("x",
        Resources.createResource(100 * GB, 100));

    q1.incrPendingResources("x", user, 2, Resource.newInstance(1024, 1));

    MetricsSource xPartitionSource = partitionSource(ms, "x");
    MetricsSource xRootQueueSource = queueSource(ms, "x", parentQueueName);
    MetricsSource q1Source = queueSource(ms, "x", "root.q1");

    checkResources(xPartitionSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(xRootQueueSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 2 * GB, 2, 2);

    root.setAvailableResourcesToQueue("y",
        Resources.createResource(400 * GB, 400));
    q2.setAvailableResourcesToQueue("y",
        Resources.createResource(200 * GB, 200));

    q2.incrPendingResources("y", user, 3, Resource.newInstance(1024, 1));

    MetricsSource yPartitionSource = partitionSource(ms, "y");
    MetricsSource yRootQueueSource = queueSource(ms, "y", parentQueueName);
    MetricsSource q2Source = queueSource(ms, "y", "root.q2");

    checkResources(yPartitionSource, 0, 0, 0, 400 * GB, 400, 3 * GB, 3, 3);
    checkResources(yRootQueueSource, 0, 0, 0, 400 * GB, 400, 3 * GB, 3, 3);
    checkResources(q2Source, 0, 0, 0, 200 * GB, 200, 3 * GB, 3, 3);
  }

  /**
   * Structure:
   * Both queues, q1 has been configured to run in multiple partitions, x & y.
   *
   * root
   * /
   * q1
   *
   * @throws Exception
   */
  @Test
  public void testMultiplePartitionWithSingleQueueMetrics() throws Exception {

    String parentQueueName = "root";
    Queue parentQueue = mock(Queue.class);

    QueueMetrics root =
        QueueMetrics.forQueue(ms, parentQueueName, null, true, CONF);
    when(parentQueue.getMetrics()).thenReturn(root);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);

    QueueMetrics q1 =
        QueueMetrics.forQueue(ms, "root.q1", parentQueue, true, CONF);

    root.setAvailableResourcesToQueue("x",
        Resources.createResource(200 * GB, 200));
    root.setAvailableResourcesToQueue("y",
        Resources.createResource(300 * GB, 300));

    q1.incrPendingResources("x", "test_user", 2, Resource.newInstance(1024, 1));

    MetricsSource partitionSource = partitionSource(ms, "x");
    MetricsSource rootQueueSource = queueSource(ms, "x", parentQueueName);
    MetricsSource q1Source = queueSource(ms, "x", "root.q1");
    MetricsSource userSource = userSource(ms, "x", "test_user", "root.q1");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 0, 0, 2 * GB, 2, 2);
    checkResources(userSource, 0, 0, 0, 0, 0, 2 * GB, 2, 2);

    q1.incrPendingResources("x", "test_user", 3, Resource.newInstance(1024, 1));

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 5 * GB, 5, 5);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 5 * GB, 5, 5);
    checkResources(q1Source, 0, 0, 0, 0, 0, 5 * GB, 5, 5);
    checkResources(userSource, 0, 0, 0, 0, 0, 5 * GB, 5, 5);

    q1.incrPendingResources("x", "test_user1", 4,
        Resource.newInstance(1024, 1));
    MetricsSource userSource1 = userSource(ms, "x", "test_user1", "root.q1");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 9 * GB, 9, 9);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 9 * GB, 9, 9);
    checkResources(q1Source, 0, 0, 0, 0, 0, 9 * GB, 9, 9);
    checkResources(userSource1, 0, 0, 0, 0, 0, 4 * GB, 4, 4);

    q1.incrPendingResources("y", "test_user1", 6,
        Resource.newInstance(1024, 1));
    MetricsSource partitionSourceY = partitionSource(ms, "y");
    MetricsSource rootQueueSourceY = queueSource(ms, "y", parentQueueName);
    MetricsSource q1SourceY = queueSource(ms, "y", "root.q1");
    MetricsSource userSourceY = userSource(ms, "y", "test_user1", "root.q1");

    checkResources(partitionSourceY, 0, 0, 0, 300 * GB, 300, 6 * GB, 6, 6);
    checkResources(rootQueueSourceY, 0, 0, 0, 300 * GB, 300, 6 * GB, 6, 6);
    checkResources(q1SourceY, 0, 0, 0, 0, 0, 6 * GB, 6, 6);
    checkResources(userSourceY, 0, 0, 0, 0, 0, 6 * GB, 6, 6);
  }

  /**
   * Structure:
   * Both queues, q1 & q2 has been configured to run in both partitions, x & y.
   *
   * root
   * / \
   * q1 q2
   * q1
   * / \
   * q11 q12
   * q2
   * / \
   * q21 q22
   *
   * @throws Exception
   */

  @Test
  public void testMultiplePartitionsWithMultiLevelQueuesMetrics()
      throws Exception {

    String parentQueueName = "root";
    Queue parentQueue = mock(Queue.class);

    QueueMetrics root =
        QueueMetrics.forQueue(ms, parentQueueName, null, true, CONF);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);
    when(parentQueue.getMetrics()).thenReturn(root);

    QueueMetrics q1 =
        QueueMetrics.forQueue(ms, "root.q1", parentQueue, true, CONF);
    Queue childQueue1 = mock(Queue.class);
    when(childQueue1.getQueueName()).thenReturn("root.q1");
    when(childQueue1.getMetrics()).thenReturn(q1);

    QueueMetrics q11 =
        QueueMetrics.forQueue(ms, "root.q1.q11", childQueue1, true, CONF);
    QueueMetrics q12 =
        QueueMetrics.forQueue(ms, "root.q1.q12", childQueue1, true, CONF);

    QueueMetrics q2 =
        QueueMetrics.forQueue(ms, "root.q2", parentQueue, true, CONF);
    Queue childQueue2 = mock(Queue.class);
    when(childQueue2.getQueueName()).thenReturn("root.q2");
    when(childQueue2.getMetrics()).thenReturn(q2);

    QueueMetrics q21 =
        QueueMetrics.forQueue(ms, "root.q2.q21", childQueue2, true, CONF);
    QueueMetrics q22 =
        QueueMetrics.forQueue(ms, "root.q2.q22", childQueue2, true, CONF);

    root.setAvailableResourcesToQueue("x",
        Resources.createResource(200 * GB, 200));

    q1.setAvailableResourcesToQueue("x",
        Resources.createResource(100 * GB, 100));
    q11.setAvailableResourcesToQueue("x",
        Resources.createResource(50 * GB, 50));

    q11.incrPendingResources("x", "test_user", 2,
        Resource.newInstance(1024, 1));

    MetricsSource partitionSource = partitionSource(ms, "x");
    MetricsSource rootQueueSource = queueSource(ms, "x", parentQueueName);
    MetricsSource q1Source = queueSource(ms, "x", "root.q1");
    MetricsSource userSource = userSource(ms, "x", "test_user", "root.q1");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 2 * GB, 2, 2);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 2 * GB, 2, 2);
    checkResources(userSource, 0, 0, 0, 0 * GB, 0, 2 * GB, 2, 2);

    q11.incrPendingResources("x", "test_user", 4,
        Resource.newInstance(1024, 1));

    MetricsSource q11Source = queueSource(ms, "x", "root.q1.q11");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 6 * GB, 6, 6);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 6 * GB, 6, 6);
    checkResources(q11Source, 0, 0, 0, 50 * GB, 50, 6 * GB, 6, 6);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 6 * GB, 6, 6);
    checkResources(userSource, 0, 0, 0, 0 * GB, 0, 6 * GB, 6, 6);

    q11.incrPendingResources("x", "test_user1", 5,
        Resource.newInstance(1024, 1));

    MetricsSource q1UserSource1 = userSource(ms, "x", "test_user1", "root.q1");
    MetricsSource userSource1 =
        userSource(ms, "x", "test_user1", "root.q1.q11");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 11 * GB, 11, 11);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 11 * GB, 11, 11);
    checkResources(q11Source, 0, 0, 0, 50 * GB, 50, 11 * GB, 11, 11);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 11 * GB, 11, 11);
    checkResources(userSource, 0, 0, 0, 0 * GB, 0, 6 * GB, 6, 6);
    checkResources(q1UserSource1, 0, 0, 0, 0 * GB, 0, 5 * GB, 5, 5);
    checkResources(userSource1, 0, 0, 0, 0 * GB, 0, 5 * GB, 5, 5);

    q12.incrPendingResources("x", "test_user", 5,
        Resource.newInstance(1024, 1));
    MetricsSource q12Source = queueSource(ms, "x", "root.q1.q12");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 16 * GB, 16, 16);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 16 * GB, 16, 16);
    checkResources(q1Source, 0, 0, 0, 100 * GB, 100, 16 * GB, 16, 16);
    checkResources(q12Source, 0, 0, 0, 0, 0, 5 * GB, 5, 5);

    root.setAvailableResourcesToQueue("y",
        Resources.createResource(200 * GB, 200));
    q1.setAvailableResourcesToQueue("y",
        Resources.createResource(100 * GB, 100));
    q12.setAvailableResourcesToQueue("y",
        Resources.createResource(50 * GB, 50));

    q12.incrPendingResources("y", "test_user", 3,
        Resource.newInstance(1024, 1));

    MetricsSource yPartitionSource = partitionSource(ms, "y");
    MetricsSource yRootQueueSource = queueSource(ms, "y", parentQueueName);
    MetricsSource q1YSource = queueSource(ms, "y", "root.q1");
    MetricsSource q12YSource = queueSource(ms, "y", "root.q1.q12");

    checkResources(yPartitionSource, 0, 0, 0, 200 * GB, 200, 3 * GB, 3, 3);
    checkResources(yRootQueueSource, 0, 0, 0, 200 * GB, 200, 3 * GB, 3, 3);
    checkResources(q1YSource, 0, 0, 0, 100 * GB, 100, 3 * GB, 3, 3);
    checkResources(q12YSource, 0, 0, 0, 50 * GB, 50, 3 * GB, 3, 3);

    root.setAvailableResourcesToQueue("y",
        Resources.createResource(200 * GB, 200));
    q2.setAvailableResourcesToQueue("y",
        Resources.createResource(100 * GB, 100));
    q21.setAvailableResourcesToQueue("y",
        Resources.createResource(50 * GB, 50));

    q21.incrPendingResources("y", "test_user", 5,
        Resource.newInstance(1024, 1));
    MetricsSource q21Source = queueSource(ms, "y", "root.q2.q21");
    MetricsSource q2YSource = queueSource(ms, "y", "root.q2");

    checkResources(yPartitionSource, 0, 0, 0, 200 * GB, 200, 8 * GB, 8, 8);
    checkResources(yRootQueueSource, 0, 0, 0, 200 * GB, 200, 8 * GB, 8, 8);
    checkResources(q2YSource, 0, 0, 0, 100 * GB, 100, 5 * GB, 5, 5);
    checkResources(q21Source, 0, 0, 0, 50 * GB, 50, 5 * GB, 5, 5);

    q22.incrPendingResources("y", "test_user", 6,
        Resource.newInstance(1024, 1));
    MetricsSource q22Source = queueSource(ms, "y", "root.q2.q22");

    checkResources(yPartitionSource, 0, 0, 0, 200 * GB, 200, 14 * GB, 14, 14);
    checkResources(yRootQueueSource, 0, 0, 0, 200 * GB, 200, 14 * GB, 14, 14);
    checkResources(q22Source, 0, 0, 0, 0, 0, 6 * GB, 6, 6);
  }

  @Test
  public void testTwoLevelWithUserMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";
    String user = "alice";
    String partition = "x";

    QueueMetrics parentMetrics =
        QueueMetrics.forQueue(ms, parentQueueName, null, true, CONF);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);
    when(parentQueue.getMetrics()).thenReturn(parentMetrics);
    QueueMetrics metrics =
        QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true, CONF);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    metrics.submitAppAttempt(user);

    parentMetrics.setAvailableResourcesToQueue(partition,
        Resources.createResource(100 * GB, 100));
    metrics.setAvailableResourcesToQueue(partition,
        Resources.createResource(100 * GB, 100));
    parentMetrics.setAvailableResourcesToUser(partition, user,
        Resources.createResource(10 * GB, 10));
    metrics.setAvailableResourcesToUser(partition, user,
        Resources.createResource(10 * GB, 10));
    metrics.incrPendingResources(partition, user, 6,
        Resources.createResource(3 * GB, 3));

    MetricsSource partitionSource = partitionSource(ms, partition);
    MetricsSource parentQueueSource =
        queueSource(ms, partition, parentQueueName);
    MetricsSource queueSource = queueSource(ms, partition, leafQueueName);
    MetricsSource userSource = userSource(ms, partition, user, leafQueueName);
    MetricsSource userSource1 =
        userSource(ms, partition, user, parentQueueName);

    checkResources(queueSource, 0, 0, 0, 0, 0, 100 * GB, 100, 18 * GB, 18, 6, 0,
        0, 0);
    checkResources(parentQueueSource, 0, 0, 0, 0, 0, 100 * GB, 100, 18 * GB, 18,
        6, 0, 0, 0);
    checkResources(userSource, 0, 0, 0, 0, 0, 10 * GB, 10, 18 * GB, 18, 6, 0, 0,
        0);
    checkResources(userSource1, 0, 0, 0, 0, 0, 10 * GB, 10, 18 * GB, 18, 6, 0,
        0, 0);
    checkResources(partitionSource, 0, 0, 0, 0, 0, 100 * GB, 100, 18 * GB, 18,
        6, 0, 0, 0);

    metrics.runAppAttempt(app.getApplicationId(), user);

    metrics.allocateResources(partition, user, 3,
        Resources.createResource(1 * GB, 1), true);
    metrics.reserveResource(partition, user,
        Resources.createResource(3 * GB, 3));

    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 3 * GB, 3, 3, 3, 0, 100 * GB, 100, 15 * GB, 15,
        3, 3 * GB, 3, 1);
    checkResources(parentQueueSource, 3 * GB, 3, 3, 3, 0, 100 * GB, 100,
        15 * GB, 15, 3, 3 * GB, 3, 1);
    checkResources(partitionSource, 3 * GB, 3, 3, 3, 0, 100 * GB, 100, 15 * GB,
        15, 3, 3 * GB, 3, 1);
    checkResources(userSource, 3 * GB, 3, 3, 3, 0, 10 * GB, 10, 15 * GB, 15, 3,
        3 * GB, 3, 1);
    checkResources(userSource1, 3 * GB, 3, 3, 3, 0, 10 * GB, 10, 15 * GB, 15, 3,
        3 * GB, 3, 1);

    metrics.allocateResources(partition, user, 3,
        Resources.createResource(1 * GB, 1), true);

    checkResources(queueSource, 6 * GB, 6, 6, 6, 0, 100 * GB, 100, 12 * GB, 12,
        0, 3 * GB, 3, 1);
    checkResources(parentQueueSource, 6 * GB, 6, 6, 6, 0, 100 * GB, 100,
        12 * GB, 12, 0, 3 * GB, 3, 1);

    metrics.releaseResources(partition, user, 1,
        Resources.createResource(2 * GB, 2));
    metrics.unreserveResource(partition, user,
        Resources.createResource(3 * GB, 3));
    checkResources(queueSource, 4 * GB, 4, 5, 6, 1, 100 * GB, 100, 12 * GB, 12,
        0, 0, 0, 0);
    checkResources(parentQueueSource, 4 * GB, 4, 5, 6, 1, 100 * GB, 100,
        12 * GB, 12, 0, 0, 0, 0);
    checkResources(partitionSource, 4 * GB, 4, 5, 6, 1, 100 * GB, 100, 12 * GB,
        12, 0, 0, 0, 0);
    checkResources(userSource, 4 * GB, 4, 5, 6, 1, 10 * GB, 10, 12 * GB, 12, 0,
        0, 0, 0);
    checkResources(userSource1, 4 * GB, 4, 5, 6, 1, 10 * GB, 10, 12 * GB, 12, 0,
        0, 0, 0);

    metrics.finishAppAttempt(app.getApplicationId(), app.isPending(),
        app.getUser());

    metrics.finishApp(user, RMAppState.FINISHED);
  }

  @Test
  public void testThreeLevelWithUserMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";
    String leafQueueName1 = "root.leaf.leaf1";
    String user = "alice";
    String partitionX = "x";
    String partitionY = "y";

    QueueMetrics parentMetrics =
        QueueMetrics.forQueue(parentQueueName, null, true, CONF);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);
    when(parentQueue.getMetrics()).thenReturn(parentMetrics);
    QueueMetrics metrics =
        QueueMetrics.forQueue(leafQueueName, parentQueue, true, CONF);
    Queue leafQueue = mock(Queue.class);
    when(leafQueue.getQueueName()).thenReturn(leafQueueName);
    when(leafQueue.getMetrics()).thenReturn(metrics);
    QueueMetrics metrics1 =
        QueueMetrics.forQueue(leafQueueName1, leafQueue, true, CONF);
    AppSchedulingInfo app = mockApp(user);

    metrics1.submitApp(user);
    metrics1.submitAppAttempt(user);

    parentMetrics.setAvailableResourcesToQueue(partitionX,
        Resources.createResource(200 * GB, 200));
    parentMetrics.setAvailableResourcesToQueue(partitionY,
        Resources.createResource(500 * GB, 500));
    metrics.setAvailableResourcesToQueue(partitionX,
        Resources.createResource(100 * GB, 100));
    metrics.setAvailableResourcesToQueue(partitionY,
        Resources.createResource(400 * GB, 400));
    metrics1.setAvailableResourcesToQueue(partitionX,
        Resources.createResource(50 * GB, 50));
    metrics1.setAvailableResourcesToQueue(partitionY,
        Resources.createResource(300 * GB, 300));
    parentMetrics.setAvailableResourcesToUser(partitionX, user,
        Resources.createResource(20 * GB, 20));
    parentMetrics.setAvailableResourcesToUser(partitionY, user,
        Resources.createResource(50 * GB, 50));
    metrics.setAvailableResourcesToUser(partitionX, user,
        Resources.createResource(10 * GB, 10));
    metrics.setAvailableResourcesToUser(partitionY, user,
        Resources.createResource(40 * GB, 40));
    metrics1.setAvailableResourcesToUser(partitionX, user,
        Resources.createResource(5 * GB, 5));
    metrics1.setAvailableResourcesToUser(partitionY, user,
        Resources.createResource(30 * GB, 30));
    metrics1.incrPendingResources(partitionX, user, 6,
        Resources.createResource(3 * GB, 3));
    metrics1.incrPendingResources(partitionY, user, 6,
        Resources.createResource(4 * GB, 4));

    MetricsSource partitionSourceX =
        partitionSource(metrics1.getMetricsSystem(), partitionX);

    MetricsSource parentQueueSourceWithPartX =
        queueSource(metrics1.getMetricsSystem(), partitionX, parentQueueName);
    MetricsSource queueSourceWithPartX =
        queueSource(metrics1.getMetricsSystem(), partitionX, leafQueueName);
    MetricsSource queueSource1WithPartX =
        queueSource(metrics1.getMetricsSystem(), partitionX, leafQueueName1);
    MetricsSource parentUserSourceWithPartX = userSource(metrics1.getMetricsSystem(),
        partitionX, user, parentQueueName);
    MetricsSource userSourceWithPartX = userSource(metrics1.getMetricsSystem(),
        partitionX, user, leafQueueName);
    MetricsSource userSource1WithPartX = userSource(metrics1.getMetricsSystem(),
        partitionX, user, leafQueueName1);

    checkResources(partitionSourceX, 0, 0, 0, 0, 0, 200 * GB, 200, 18 * GB, 18,
        6, 0, 0, 0);
    checkResources(parentQueueSourceWithPartX, 0, 0, 0, 0, 0, 200 * GB, 200, 18 * GB,
        18, 6, 0, 0, 0);

    checkResources(queueSourceWithPartX, 0, 0, 0, 0, 0, 100 * GB, 100, 18 * GB, 18, 6,
        0, 0, 0);
    checkResources(queueSource1WithPartX, 0, 0, 0, 0, 0, 50 * GB, 50, 18 * GB, 18, 6,
        0, 0, 0);
    checkResources(parentUserSourceWithPartX, 0, 0, 0, 0, 0, 20 * GB, 20, 18 * GB, 18,
        6, 0, 0, 0);
    checkResources(userSourceWithPartX, 0, 0, 0, 0, 0, 10 * GB, 10, 18 * GB, 18, 6, 0,
        0, 0);
    checkResources(userSource1WithPartX, 0, 0, 0, 0, 0, 5 * GB, 5, 18 * GB, 18, 6, 0,
        0, 0);

    MetricsSource partitionSourceY =
        partitionSource(metrics1.getMetricsSystem(), partitionY);

    MetricsSource parentQueueSourceWithPartY =
        queueSource(metrics1.getMetricsSystem(), partitionY, parentQueueName);
    MetricsSource queueSourceWithPartY =
        queueSource(metrics1.getMetricsSystem(), partitionY, leafQueueName);
    MetricsSource queueSource1WithPartY =
        queueSource(metrics1.getMetricsSystem(), partitionY, leafQueueName1);
    MetricsSource parentUserSourceWithPartY = userSource(metrics1.getMetricsSystem(),
        partitionY, user, parentQueueName);
    MetricsSource userSourceWithPartY = userSource(metrics1.getMetricsSystem(),
        partitionY, user, leafQueueName);
    MetricsSource userSource1WithPartY = userSource(metrics1.getMetricsSystem(),
        partitionY, user, leafQueueName1);

    checkResources(partitionSourceY, 0, 0, 0, 0, 0, 500 * GB, 500, 24 * GB, 24,
        6, 0, 0, 0);
    checkResources(parentQueueSourceWithPartY, 0, 0, 0, 0, 0, 500 * GB, 500, 24 * GB,
        24, 6, 0, 0, 0);
    checkResources(queueSourceWithPartY, 0, 0, 0, 0, 0, 400 * GB, 400, 24 * GB, 24, 6,
        0, 0, 0);
    checkResources(queueSource1WithPartY, 0, 0, 0, 0, 0, 300 * GB, 300, 24 * GB, 24, 6,
        0, 0, 0);
    checkResources(parentUserSourceWithPartY, 0, 0, 0, 0, 0, 50 * GB, 50, 24 * GB, 24,
        6, 0, 0, 0);
    checkResources(userSourceWithPartY, 0, 0, 0, 0, 0, 40 * GB, 40, 24 * GB, 24, 6, 0,
        0, 0);
    checkResources(userSource1WithPartY, 0, 0, 0, 0, 0, 30 * GB, 30, 24 * GB, 24, 6, 0,
        0, 0);

    metrics1.finishAppAttempt(app.getApplicationId(), app.isPending(),
        app.getUser());

    metrics1.finishApp(user, RMAppState.FINISHED);
  }

  /**
   * Structure:
   * Both queues, q1 & q2 has been configured to run in only 1 partition, x
   * UserMetrics has been disabled, hence trying to access the user source
   * throws NPE from sources.
   *
   * root
   * / \
   * q1 q2
   *
   * @throws Exception
   */
  @Test(expected = NullPointerException.class)
  public void testSinglePartitionWithSingleLevelQueueMetricsWithoutUserMetrics()
      throws Exception {

    String parentQueueName = "root";
    Queue parentQueue = mock(Queue.class);
    String user = "alice";

    QueueMetrics root = QueueMetrics.forQueue("root", null, false, CONF);
    when(parentQueue.getMetrics()).thenReturn(root);
    when(parentQueue.getQueueName()).thenReturn(parentQueueName);
    CSQueueMetrics q1 =
        CSQueueMetrics.forQueue("root.q1", parentQueue, false, CONF);
    CSQueueMetrics q2 =
        CSQueueMetrics.forQueue("root.q2", parentQueue, false, CONF);

    AppSchedulingInfo app = mockApp(user);

    q1.submitApp(user);
    q1.submitAppAttempt(user);

    root.setAvailableResourcesToQueue("x",
        Resources.createResource(200 * GB, 200));

    q1.incrPendingResources("x", user, 2, Resource.newInstance(1024, 1));

    MetricsSource partitionSource = partitionSource(q1.getMetricsSystem(), "x");
    MetricsSource rootQueueSource =
        queueSource(q1.getMetricsSystem(), "x", parentQueueName);
    MetricsSource q1Source = queueSource(q1.getMetricsSystem(), "x", "root.q1");
    MetricsSource q1UserSource =
        userSource(q1.getMetricsSystem(), "x", user, "root.q1");

    checkResources(partitionSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(rootQueueSource, 0, 0, 0, 200 * GB, 200, 2 * GB, 2, 2);
    checkResources(q1Source, 0, 0, 0, 0, 0, 2 * GB, 2, 2);
    checkResources(q1UserSource, 0, 0, 0, 0, 0, 2 * GB, 2, 2);

    q2.incrPendingResources("x", user, 3, Resource.newInstance(1024, 1));
    MetricsSource q2Source = queueSource(q2.getMetricsSystem(), "x", "root.q2");
    MetricsSource q2UserSource =
        userSource(q1.getMetricsSystem(), "x", user, "root.q2");

    checkResources(partitionSource, 0, 0, 0, 0, 0, 5 * GB, 5, 5);
    checkResources(rootQueueSource, 0, 0, 0, 0, 0, 5 * GB, 5, 5);
    checkResources(q2Source, 0, 0, 0, 0, 0, 3 * GB, 3, 3);
    checkResources(q2UserSource, 0, 0, 0, 0, 0, 3 * GB, 3, 3);

    q1.finishAppAttempt(app.getApplicationId(), app.isPending(), app.getUser());
    q1.finishApp(user, RMAppState.FINISHED);
  }

  public static MetricsSource partitionSource(MetricsSystem ms,
      String partition) {
    MetricsSource s =
        ms.getSource(QueueMetrics.pSourceName(partition).toString());
    return s;
  }

  public static MetricsSource queueSource(MetricsSystem ms, String partition,
      String queue) {
    MetricsSource s = ms.getSource(QueueMetrics.pSourceName(partition)
        .append(QueueMetrics.qSourceName(queue)).toString());
    return s;
  }

  public static MetricsSource userSource(MetricsSystem ms, String partition,
      String user, String queue) {
    MetricsSource s = ms.getSource(QueueMetrics.pSourceName(partition)
        .append(QueueMetrics.qSourceName(queue)).append(",user=")
        .append(user).toString());
    return s;
  }

  public static void checkResources(MetricsSource source, long allocatedMB,
      int allocatedCores, int allocCtnrs, long availableMB, int availableCores,
      long pendingMB, int pendingCores, int pendingCtnrs) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertGauge("AllocatedMB", allocatedMB, rb);
    assertGauge("AllocatedVCores", allocatedCores, rb);
    assertGauge("AllocatedContainers", allocCtnrs, rb);
    assertGauge("AvailableMB", availableMB, rb);
    assertGauge("AvailableVCores", availableCores, rb);
    assertGauge("PendingMB", pendingMB, rb);
    assertGauge("PendingVCores", pendingCores, rb);
    assertGauge("PendingContainers", pendingCtnrs, rb);
  }

  private static AppSchedulingInfo mockApp(String user) {
    AppSchedulingInfo app = mock(AppSchedulingInfo.class);
    when(app.getUser()).thenReturn(user);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId id = BuilderUtils.newApplicationAttemptId(appId, 1);
    when(app.getApplicationAttemptId()).thenReturn(id);
    return app;
  }

  public static void checkResources(MetricsSource source, long allocatedMB,
      int allocatedCores, int allocCtnrs, long aggreAllocCtnrs,
      long aggreReleasedCtnrs, long availableMB, int availableCores,
      long pendingMB, int pendingCores, int pendingCtnrs, long reservedMB,
      int reservedCores, int reservedCtnrs) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertGauge("AllocatedMB", allocatedMB, rb);
    assertGauge("AllocatedVCores", allocatedCores, rb);
    assertGauge("AllocatedContainers", allocCtnrs, rb);
    assertCounter("AggregateContainersAllocated", aggreAllocCtnrs, rb);
    assertCounter("AggregateContainersReleased", aggreReleasedCtnrs, rb);
    assertGauge("AvailableMB", availableMB, rb);
    assertGauge("AvailableVCores", availableCores, rb);
    assertGauge("PendingMB", pendingMB, rb);
    assertGauge("PendingVCores", pendingCores, rb);
    assertGauge("PendingContainers", pendingCtnrs, rb);
    assertGauge("ReservedMB", reservedMB, rb);
    assertGauge("ReservedVCores", reservedCores, rb);
    assertGauge("ReservedContainers", reservedCtnrs, rb);
  }
}