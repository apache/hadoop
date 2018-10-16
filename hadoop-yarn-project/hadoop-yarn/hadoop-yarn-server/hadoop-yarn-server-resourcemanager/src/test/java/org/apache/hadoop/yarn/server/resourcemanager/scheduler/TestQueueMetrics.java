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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.test.MetricsAsserts;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_COMPLETED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_FAILED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_PENDING;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_RUNNING;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppMetricsChecker.AppMetricsKey.APPS_SUBMITTED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_CONTAINERS_ALLOCATED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AGGREGATE_CONTAINERS_RELEASED;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.ALLOCATED_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.AVAILABLE_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.PENDING_V_CORES;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_CONTAINERS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.RESERVED_V_CORES;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestQueueMetrics {
  private static Queue createMockQueue(QueueMetrics metrics) {
    Queue queue = mock(Queue.class);
    when(queue.getMetrics()).thenReturn(metrics);
    return queue;
  }

  private static final int GB = 1024; // MB
  private static final String USER = "alice";
  private static final String USER_2 = "dodo";
  private static final Configuration conf = new Configuration();
  private MetricsSystem ms;

  @Before
  public void setUp() {
    ms = new MetricsSystemImpl();
    QueueMetrics.clearQueueMetrics();
  }
  
  @Test
  public void testDefaultSingleQueueMetrics() {
    String queueName = "single";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false,
						 conf);
    MetricsSource queueSource= queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(USER);

    metrics.submitApp(USER);
    MetricsSource userSource = userSource(ms, queueName, USER);
    AppMetricsChecker appMetricsChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    metrics.submitAppAttempt(USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        USER, 5, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    ResourceMetricsChecker rmChecker = ResourceMetricsChecker.create()
        .gaugeLong(AVAILABLE_MB, 100 * GB)
        .gaugeInt(AVAILABLE_V_CORES, 100)
        .gaugeLong(PENDING_MB, 15 * GB)
        .gaugeInt(PENDING_V_CORES, 15)
        .gaugeInt(PENDING_CONTAINERS, 5)
        .checkAgainst(queueSource);

    metrics.runAppAttempt(app.getApplicationId(), USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 1)
        .checkAgainst(queueSource, true);

    metrics.allocateResources(RMNodeLabelsManager.NO_LABEL,
        USER, 3, Resources.createResource(2*GB, 2), true);
    rmChecker = ResourceMetricsChecker.createFromChecker(rmChecker)
        .gaugeLong(ALLOCATED_MB, 6 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 6)
        .gaugeInt(ALLOCATED_CONTAINERS, 3)
        .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
        .gaugeLong(PENDING_MB, 9 * GB)
        .gaugeInt(PENDING_V_CORES, 9)
        .gaugeInt(PENDING_CONTAINERS, 2)
        .checkAgainst(queueSource);

    metrics.releaseResources(RMNodeLabelsManager.NO_LABEL,
        USER, 1, Resources.createResource(2*GB, 2));
    rmChecker = ResourceMetricsChecker.createFromChecker(rmChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .checkAgainst(queueSource);

    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        USER, 0, Resources.createResource(2 * GB, 2));
    //nothing should change in values
    rmChecker = ResourceMetricsChecker.createFromChecker(rmChecker)
        .checkAgainst(queueSource);

    metrics.decrPendingResources(RMNodeLabelsManager.NO_LABEL,
        USER, 0, Resources.createResource(2 * GB, 2));
    //nothing should change in values
    ResourceMetricsChecker.createFromChecker(rmChecker)
        .checkAgainst(queueSource);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .counter(APPS_SUBMITTED, 1)
        .gaugeInt(APPS_RUNNING, 0)
        .checkAgainst(queueSource, true);
    metrics.finishApp(USER, RMAppState.FINISHED);
    AppMetricsChecker.createFromChecker(appMetricsChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(queueSource, true);
    assertNull(userSource);
  }
  
  @Test
  public void testQueueAppMetricsForMultipleFailures() {
    String queueName = "single";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false,
        new Configuration());
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(USER);

    metrics.submitApp(USER);
    MetricsSource userSource = userSource(ms, queueName, USER);
    AppMetricsChecker appMetricsChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    metrics.submitAppAttempt(USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 1)
        .checkAgainst(queueSource, true);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_RUNNING, 0)
        .checkAgainst(queueSource, true);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitAppAttempt(USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 1)
        .checkAgainst(queueSource, true);

    // Suppose say application has failed this time as well.
    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_RUNNING, 0)
        .checkAgainst(queueSource, true);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitAppAttempt(USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), USER);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 1)
        .checkAgainst(queueSource, true);

    // Suppose say application has failed, and there's no more retries.
    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_RUNNING, 0)
        .checkAgainst(queueSource, true);

    metrics.finishApp(USER, RMAppState.FAILED);
    AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_RUNNING, 0)
        .counter(APPS_FAILED, 1)
        .checkAgainst(queueSource, true);

    assertNull(userSource);
  }

  @Test
  public void testSingleQueueWithUserMetrics() {
    String queueName = "single2";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, true,
						 conf);
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(USER_2);

    metrics.submitApp(USER_2);
    MetricsSource userSource = userSource(ms, queueName, USER_2);

    AppMetricsChecker appMetricsQueueSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    AppMetricsChecker appMetricsUserSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(userSource, true);

    metrics.submitAppAttempt(USER_2);
    appMetricsQueueSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsQueueSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);
    appMetricsUserSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsUserSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(userSource, true);

    metrics.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    metrics.setAvailableResourcesToUser(RMNodeLabelsManager.NO_LABEL,
        USER_2, Resources.createResource(10*GB, 10));
    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        USER_2, 5, Resources.createResource(3*GB, 3));

    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    ResourceMetricsChecker resMetricsQueueSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 100 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 100)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(queueSource);
    ResourceMetricsChecker resMetricsUserSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 10 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 10)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(userSource);

    metrics.runAppAttempt(app.getApplicationId(), USER_2);
    appMetricsQueueSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsQueueSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(queueSource, true);
    appMetricsUserSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsUserSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(userSource, true);

    metrics.allocateResources(RMNodeLabelsManager.NO_LABEL,
        USER_2, 3, Resources.createResource(2*GB, 2), true);
    resMetricsQueueSourceChecker =
        ResourceMetricsChecker.createFromChecker(resMetricsQueueSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .checkAgainst(queueSource);
    resMetricsUserSourceChecker =
        ResourceMetricsChecker.createFromChecker(resMetricsUserSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .checkAgainst(userSource);

    metrics.releaseResources(RMNodeLabelsManager.NO_LABEL,
        USER_2, 1, Resources.createResource(2*GB, 2));
    ResourceMetricsChecker.createFromChecker(resMetricsQueueSourceChecker)
            .gaugeLong(ALLOCATED_MB, 4 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 4)
            .gaugeInt(ALLOCATED_CONTAINERS, 2)
            .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
            .checkAgainst(queueSource);
    ResourceMetricsChecker.createFromChecker(resMetricsUserSourceChecker)
            .gaugeLong(ALLOCATED_MB, 4 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 4)
            .gaugeInt(ALLOCATED_CONTAINERS, 2)
            .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
            .checkAgainst(userSource);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(queueSource, true);
    appMetricsUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(userSource, true);
    metrics.finishApp(USER_2, RMAppState.FINISHED);
    AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(queueSource, true);
    AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(userSource, true);
  }

  @Test
  public void testNodeTypeMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";

    QueueMetrics parentMetrics =
      QueueMetrics.forQueue(ms, parentQueueName, null, true, conf);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getMetrics()).thenReturn(parentMetrics);
    QueueMetrics metrics =
      QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true, conf);
    MetricsSource parentQueueSource = queueSource(ms, parentQueueName);
    MetricsSource queueSource = queueSource(ms, leafQueueName);
    //AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(USER);
    MetricsSource userSource = userSource(ms, leafQueueName, USER);
    MetricsSource parentUserSource = userSource(ms, parentQueueName, USER);

    metrics.incrNodeTypeAggregations(USER, NodeType.NODE_LOCAL);
    checkAggregatedNodeTypes(queueSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(userSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 0L, 0L);

    metrics.incrNodeTypeAggregations(USER, NodeType.RACK_LOCAL);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 0L);

    metrics.incrNodeTypeAggregations(USER, NodeType.OFF_SWITCH);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 1L);

    metrics.incrNodeTypeAggregations(USER, NodeType.OFF_SWITCH);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 2L);
  }

  @Test
  public void testTwoLevelWithUserMetrics() {
    AppSchedulingInfo app = mockApp(USER);

    QueueInfo root = new QueueInfo(null, "root", ms, conf, USER);
    QueueInfo leaf = new QueueInfo(root, "root.leaf", ms, conf, USER);
    leaf.queueMetrics.submitApp(USER);

    AppMetricsChecker appMetricsQueueSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(leaf.queueSource, true);
    AppMetricsChecker appMetricsParentQueueSourceChecker =
        AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(root.queueSource, true);
    AppMetricsChecker appMetricsUserSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(leaf.userSource, true);
    AppMetricsChecker appMetricsParentUserSourceChecker =
        AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(root.userSource, true);

    leaf.queueMetrics.submitAppAttempt(USER);
    appMetricsQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(leaf.queueSource, true);
    appMetricsParentQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsParentQueueSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(root.queueSource, true);
    appMetricsUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(leaf.userSource, true);
    appMetricsParentUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsParentUserSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(root.userSource, true);

    root.queueMetrics.setAvailableResourcesToQueue(
        RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    leaf.queueMetrics.setAvailableResourcesToQueue(
        RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    root.queueMetrics.setAvailableResourcesToUser(
        RMNodeLabelsManager.NO_LABEL,
        USER, Resources.createResource(10*GB, 10));
    leaf.queueMetrics.setAvailableResourcesToUser(
        RMNodeLabelsManager.NO_LABEL,
        USER, Resources.createResource(10*GB, 10));
    leaf.queueMetrics.incrPendingResources(
        RMNodeLabelsManager.NO_LABEL,
        USER, 5, Resources.createResource(3*GB, 3));

    ResourceMetricsChecker resMetricsQueueSourceChecker =
        ResourceMetricsChecker.create()
        .gaugeLong(AVAILABLE_MB, 100 * GB)
        .gaugeInt(AVAILABLE_V_CORES, 100)
        .gaugeLong(PENDING_MB, 15 * GB)
        .gaugeInt(PENDING_V_CORES, 15)
        .gaugeInt(PENDING_CONTAINERS, 5)
        .checkAgainst(leaf.queueSource);
    ResourceMetricsChecker resMetricsParentQueueSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 100 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 100)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(root.queueSource);
    ResourceMetricsChecker resMetricsUserSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 10 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 10)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(leaf.userSource);
    ResourceMetricsChecker resMetricsParentUserSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 10 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 10)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(root.userSource);

    leaf.queueMetrics.runAppAttempt(app.getApplicationId(), USER);
    appMetricsQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(leaf.queueSource, true);
    appMetricsUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(leaf.userSource, true);

    leaf.queueMetrics.allocateResources(RMNodeLabelsManager.NO_LABEL,
        USER, 3, Resources.createResource(2*GB, 2), true);
    leaf.queueMetrics.reserveResource(RMNodeLabelsManager.NO_LABEL,
        USER, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    resMetricsQueueSourceChecker =
        ResourceMetricsChecker.createFromChecker(resMetricsQueueSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .gaugeLong(RESERVED_MB, 3 * GB)
            .gaugeInt(RESERVED_V_CORES, 3)
            .gaugeInt(RESERVED_CONTAINERS, 1)
            .checkAgainst(leaf.queueSource);
    resMetricsParentQueueSourceChecker =
        ResourceMetricsChecker
            .createFromChecker(resMetricsParentQueueSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .gaugeLong(RESERVED_MB, 3 * GB)
            .gaugeInt(RESERVED_V_CORES, 3)
            .gaugeInt(RESERVED_CONTAINERS, 1)
            .checkAgainst(root.queueSource);
    resMetricsUserSourceChecker =
        ResourceMetricsChecker.createFromChecker(resMetricsUserSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .gaugeLong(RESERVED_MB, 3 * GB)
            .gaugeInt(RESERVED_V_CORES, 3)
            .gaugeInt(RESERVED_CONTAINERS, 1)
        .checkAgainst(leaf.userSource);
    resMetricsParentUserSourceChecker = ResourceMetricsChecker
        .createFromChecker(resMetricsParentUserSourceChecker)
            .gaugeLong(ALLOCATED_MB, 6 * GB)
            .gaugeInt(ALLOCATED_V_CORES, 6)
            .gaugeInt(ALLOCATED_CONTAINERS, 3)
            .counter(AGGREGATE_CONTAINERS_ALLOCATED, 3)
            .gaugeLong(PENDING_MB, 9 * GB)
            .gaugeInt(PENDING_V_CORES, 9)
            .gaugeInt(PENDING_CONTAINERS, 2)
            .gaugeLong(RESERVED_MB, 3 * GB)
            .gaugeInt(RESERVED_V_CORES, 3)
            .gaugeInt(RESERVED_CONTAINERS, 1)
            .checkAgainst(root.userSource);

    leaf.queueMetrics.releaseResources(RMNodeLabelsManager.NO_LABEL,
        USER, 1, Resources.createResource(2*GB, 2));
    leaf.queueMetrics.unreserveResource(RMNodeLabelsManager.NO_LABEL,
        USER, Resources.createResource(3*GB, 3));
    ResourceMetricsChecker.createFromChecker(resMetricsQueueSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(leaf.queueSource);
    ResourceMetricsChecker.createFromChecker(resMetricsParentQueueSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(root.queueSource);
    ResourceMetricsChecker.createFromChecker(resMetricsUserSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(leaf.userSource);
    ResourceMetricsChecker.createFromChecker(resMetricsParentUserSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(root.userSource);

    leaf.queueMetrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsQueueSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsQueueSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(leaf.queueSource, true);
    appMetricsParentQueueSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsParentQueueSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(root.queueSource, true);
    appMetricsUserSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsUserSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(leaf.userSource, true);
    appMetricsParentUserSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsParentUserSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(root.userSource, true);

    leaf.queueMetrics.finishApp(USER, RMAppState.FINISHED);
    AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(leaf.queueSource, true);
    AppMetricsChecker.createFromChecker(appMetricsParentQueueSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(root.queueSource, true);
    AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(leaf.userSource, true);
    AppMetricsChecker.createFromChecker(appMetricsParentUserSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(root.userSource, true);
  }
  
  @Test 
  public void testMetricsCache() {
    MetricsSystem ms = new MetricsSystemImpl("cache");
    ms.start();
    
    try {
      String p1 = "root1";
      String leafQueueName = "root1.leaf";

      QueueMetrics p1Metrics =
          QueueMetrics.forQueue(ms, p1, null, true, conf);
      Queue parentQueue1 = mock(Queue.class);
      when(parentQueue1.getMetrics()).thenReturn(p1Metrics);
      QueueMetrics metrics =
          QueueMetrics.forQueue(ms, leafQueueName, parentQueue1, true, conf);

      Assert.assertNotNull("QueueMetrics for A shoudn't be null", metrics);

      // Re-register to check for cache hit, shouldn't blow up metrics-system...
      // also, verify parent-metrics
      QueueMetrics alterMetrics =
          QueueMetrics.forQueue(ms, leafQueueName, parentQueue1, true, conf);

      Assert.assertNotNull("QueueMetrics for alterMetrics shoudn't be null", 
          alterMetrics);
    } finally {
      ms.shutdown();
    }
  }

  @Test
  public void testMetricsInitializedOnRMInit() {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
      FifoScheduler.class, ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    QueueMetrics metrics = rm.getResourceScheduler().getRootQueueMetrics();
    AppMetricsChecker.create()
        .checkAgainst(metrics, true);
    MetricsAsserts.assertGauge(RESERVED_CONTAINERS.getValue(), 0, metrics);
  }

  // This is to test all metrics can consistently show up if specified true to
  // collect all metrics, even though they are not modified from last time they
  // are collected. If not collecting all metrics, only modified metrics will show up.
  @Test
  public void testCollectAllMetrics() {
    String queueName = "single";
    QueueMetrics.forQueue(ms, queueName, null, false, conf);
    MetricsSource queueSource = queueSource(ms, queueName);

    AppMetricsChecker.create()
        .checkAgainst(queueSource, true);
    try {
      // do not collect all metrics
      AppMetricsChecker.create()
          .checkAgainst(queueSource, false);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertTrue(
              e.getMessage().contains("Expected exactly one metric for name "));
    }
    // collect all metrics
    AppMetricsChecker.create()
        .checkAgainst(queueSource, true);
  }

  private static void checkAggregatedNodeTypes(MetricsSource source,
      long nodeLocal, long rackLocal, long offSwitch) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("AggregateNodeLocalContainersAllocated", nodeLocal, rb);
    assertCounter("AggregateRackLocalContainersAllocated", rackLocal, rb);
    assertCounter("AggregateOffSwitchContainersAllocated", offSwitch, rb);
  }

  static AppSchedulingInfo mockApp(String user) {
    AppSchedulingInfo app = mock(AppSchedulingInfo.class);
    when(app.getUser()).thenReturn(user);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId id = BuilderUtils.newApplicationAttemptId(appId, 1);
    when(app.getApplicationAttemptId()).thenReturn(id);
    return app;
  }

  public static MetricsSource queueSource(MetricsSystem ms, String queue) {
    return ms.getSource(QueueMetrics.sourceName(queue).toString());
  }

  public static MetricsSource userSource(MetricsSystem ms, String queue,
      String user) {
    return ms.getSource(QueueMetrics.sourceName(queue).
        append(",user=").append(user).toString());
  }
}
