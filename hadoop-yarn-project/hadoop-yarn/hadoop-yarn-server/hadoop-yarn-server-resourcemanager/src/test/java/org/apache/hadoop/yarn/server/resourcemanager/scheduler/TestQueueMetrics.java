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
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AppMetricsChecker.AppMetricsKey.*;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceMetricsChecker.ResourceMetricsKey.*;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

public class TestQueueMetrics {
  private static final int GB = 1024; // MB
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
    String user = "alice";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false,
						 conf);
    MetricsSource queueSource= queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);
    AppMetricsChecker appMetricsChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    metrics.submitAppAttempt(user);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        user, 5, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    ResourceMetricsChecker rmChecker = ResourceMetricsChecker.create()
        .gaugeLong(AVAILABLE_MB, 100 * GB)
        .gaugeInt(AVAILABLE_V_CORES, 100)
        .gaugeLong(PENDING_MB, 15 * GB)
        .gaugeInt(PENDING_V_CORES, 15)
        .gaugeInt(PENDING_CONTAINERS, 5)
        .checkAgainst(queueSource);

    metrics.runAppAttempt(app.getApplicationId(), user);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 0)
        .gaugeInt(APPS_RUNNING, 1)
        .checkAgainst(queueSource, true);

    metrics.allocateResources(RMNodeLabelsManager.NO_LABEL,
        user, 3, Resources.createResource(2*GB, 2), true);
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
        user, 1, Resources.createResource(2*GB, 2));
    rmChecker = ResourceMetricsChecker.createFromChecker(rmChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .checkAgainst(queueSource);

    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        user, 0, Resources.createResource(2 * GB, 2));
    //nothing should change in values
    rmChecker = ResourceMetricsChecker.createFromChecker(rmChecker)
        .checkAgainst(queueSource);

    metrics.decrPendingResources(RMNodeLabelsManager.NO_LABEL,
        user, 0, Resources.createResource(2 * GB, 2));
    //nothing should change in values
    ResourceMetricsChecker.createFromChecker(rmChecker)
        .checkAgainst(queueSource);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .counter(APPS_SUBMITTED, 1)
        .gaugeInt(APPS_RUNNING, 0)
        .checkAgainst(queueSource, true);
    metrics.finishApp(user, RMAppState.FINISHED);
    AppMetricsChecker.createFromChecker(appMetricsChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(queueSource, true);
    assertNull(userSource);
  }
  
  @Test
  public void testQueueAppMetricsForMultipleFailures() {
    String queueName = "single";
    String user = "alice";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false,
        new Configuration());
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);
    AppMetricsChecker appMetricsChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    metrics.submitAppAttempt(user);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
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
    metrics.submitAppAttempt(user);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
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
    metrics.submitAppAttempt(user);
    appMetricsChecker = AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
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

    metrics.finishApp(user, RMAppState.FAILED);
    AppMetricsChecker.createFromChecker(appMetricsChecker)
        .gaugeInt(APPS_RUNNING, 0)
        .counter(APPS_FAILED, 1)
        .checkAgainst(queueSource, true);

    assertNull(userSource);
  }

  @Test
  public void testSingleQueueWithUserMetrics() {
    String queueName = "single2";
    String user = "dodo";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, true,
						 conf);
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);

    AppMetricsChecker appMetricsQueueSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    AppMetricsChecker appMetricsUserSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(userSource, true);

    metrics.submitAppAttempt(user);
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
        user, Resources.createResource(10*GB, 10));
    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        user, 5, Resources.createResource(3*GB, 3));

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

    metrics.runAppAttempt(app.getApplicationId(), user);
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
        user, 3, Resources.createResource(2*GB, 2), true);
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
        user, 1, Resources.createResource(2*GB, 2));
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
    metrics.finishApp(user, RMAppState.FINISHED);
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
    String user = "alice";

    QueueMetrics parentMetrics =
      QueueMetrics.forQueue(ms, parentQueueName, null, true, conf);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getMetrics()).thenReturn(parentMetrics);
    QueueMetrics metrics =
      QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true, conf);
    MetricsSource parentQueueSource = queueSource(ms, parentQueueName);
    MetricsSource queueSource = queueSource(ms, leafQueueName);
    //AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, leafQueueName, user);
    MetricsSource parentUserSource = userSource(ms, parentQueueName, user);

    metrics.incrNodeTypeAggregations(user, NodeType.NODE_LOCAL);
    checkAggregatedNodeTypes(queueSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(userSource, 1L, 0L, 0L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 0L, 0L);

    metrics.incrNodeTypeAggregations(user, NodeType.RACK_LOCAL);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 0L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 0L);

    metrics.incrNodeTypeAggregations(user, NodeType.OFF_SWITCH);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 1L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 1L);

    metrics.incrNodeTypeAggregations(user, NodeType.OFF_SWITCH);
    checkAggregatedNodeTypes(queueSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(parentQueueSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(userSource, 1L, 1L, 2L);
    checkAggregatedNodeTypes(parentUserSource, 1L, 1L, 2L);
  }

  @Test
  public void testTwoLevelWithUserMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";
    String user = "alice";

    QueueMetrics parentMetrics =
      QueueMetrics.forQueue(ms, parentQueueName, null, true, conf);
    Queue parentQueue = mock(Queue.class);
    when(parentQueue.getMetrics()).thenReturn(parentMetrics);
    QueueMetrics metrics =
      QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true, conf);
    MetricsSource parentQueueSource = queueSource(ms, parentQueueName);
    MetricsSource queueSource = queueSource(ms, leafQueueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, leafQueueName, user);
    MetricsSource parentUserSource = userSource(ms, parentQueueName, user);

    AppMetricsChecker appMetricsQueueSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(queueSource, true);
    AppMetricsChecker appMetricsParentQueueSourceChecker =
        AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(parentQueueSource, true);
    AppMetricsChecker appMetricsUserSourceChecker = AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(userSource, true);
    AppMetricsChecker appMetricsParentUserSourceChecker =
        AppMetricsChecker.create()
        .counter(APPS_SUBMITTED, 1)
        .checkAgainst(parentUserSource, true);

    metrics.submitAppAttempt(user);
    appMetricsQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(queueSource, true);
    appMetricsParentQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsParentQueueSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(parentQueueSource, true);
    appMetricsUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(userSource, true);
    appMetricsParentUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsParentUserSourceChecker)
        .gaugeInt(APPS_PENDING, 1)
        .checkAgainst(parentUserSource, true);

    parentMetrics.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    metrics.setAvailableResourcesToQueue(RMNodeLabelsManager.NO_LABEL,
        Resources.createResource(100*GB, 100));
    parentMetrics.setAvailableResourcesToUser(RMNodeLabelsManager.NO_LABEL,
        user, Resources.createResource(10*GB, 10));
    metrics.setAvailableResourcesToUser(RMNodeLabelsManager.NO_LABEL,
        user, Resources.createResource(10*GB, 10));
    metrics.incrPendingResources(RMNodeLabelsManager.NO_LABEL,
        user, 5, Resources.createResource(3*GB, 3));

    ResourceMetricsChecker resMetricsQueueSourceChecker =
        ResourceMetricsChecker.create()
        .gaugeLong(AVAILABLE_MB, 100 * GB)
        .gaugeInt(AVAILABLE_V_CORES, 100)
        .gaugeLong(PENDING_MB, 15 * GB)
        .gaugeInt(PENDING_V_CORES, 15)
        .gaugeInt(PENDING_CONTAINERS, 5)
        .checkAgainst(queueSource);
    ResourceMetricsChecker resMetricsParentQueueSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 100 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 100)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(parentQueueSource);
    ResourceMetricsChecker resMetricsUserSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 10 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 10)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(userSource);
    ResourceMetricsChecker resMetricsParentUserSourceChecker =
        ResourceMetricsChecker.create()
            .gaugeLong(AVAILABLE_MB, 10 * GB)
            .gaugeInt(AVAILABLE_V_CORES, 10)
            .gaugeLong(PENDING_MB, 15 * GB)
            .gaugeInt(PENDING_V_CORES, 15)
            .gaugeInt(PENDING_CONTAINERS, 5)
            .checkAgainst(parentUserSource);

    metrics.runAppAttempt(app.getApplicationId(), user);
    appMetricsQueueSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(queueSource, true);
    appMetricsUserSourceChecker =
        AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 1)
            .checkAgainst(userSource, true);

    metrics.allocateResources(RMNodeLabelsManager.NO_LABEL,
        user, 3, Resources.createResource(2*GB, 2), true);
    metrics.reserveResource(RMNodeLabelsManager.NO_LABEL,
        user, Resources.createResource(3*GB, 3));
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
            .checkAgainst(queueSource);
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
            .checkAgainst(parentQueueSource);
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
        .checkAgainst(userSource);
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
            .checkAgainst(parentUserSource);

    metrics.releaseResources(RMNodeLabelsManager.NO_LABEL,
        user, 1, Resources.createResource(2*GB, 2));
    metrics.unreserveResource(RMNodeLabelsManager.NO_LABEL,
          user, Resources.createResource(3*GB, 3));
    ResourceMetricsChecker.createFromChecker(resMetricsQueueSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(queueSource);
    ResourceMetricsChecker.createFromChecker(resMetricsParentQueueSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(parentQueueSource);
    ResourceMetricsChecker.createFromChecker(resMetricsUserSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(userSource);
    ResourceMetricsChecker.createFromChecker(resMetricsParentUserSourceChecker)
        .gaugeLong(ALLOCATED_MB, 4 * GB)
        .gaugeInt(ALLOCATED_V_CORES, 4)
        .gaugeInt(ALLOCATED_CONTAINERS, 2)
        .counter(AGGREGATE_CONTAINERS_RELEASED, 1)
        .gaugeLong(RESERVED_MB, 0)
        .gaugeInt(RESERVED_V_CORES, 0)
        .gaugeInt(RESERVED_CONTAINERS, 0)
        .checkAgainst(parentUserSource);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    appMetricsQueueSourceChecker = AppMetricsChecker
        .createFromChecker(appMetricsQueueSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(queueSource, true);
    appMetricsParentQueueSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsParentQueueSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(parentQueueSource, true);
    appMetricsUserSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsUserSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(userSource, true);
    appMetricsParentUserSourceChecker = AppMetricsChecker
            .createFromChecker(appMetricsParentUserSourceChecker)
            .counter(APPS_SUBMITTED, 1)
            .gaugeInt(APPS_PENDING, 0)
            .gaugeInt(APPS_RUNNING, 0)
            .checkAgainst(parentUserSource, true);

    metrics.finishApp(user, RMAppState.FINISHED);
    AppMetricsChecker.createFromChecker(appMetricsQueueSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(queueSource, true);
    AppMetricsChecker.createFromChecker(appMetricsParentQueueSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(parentQueueSource, true);
    AppMetricsChecker.createFromChecker(appMetricsUserSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(userSource, true);
    AppMetricsChecker.createFromChecker(appMetricsParentUserSourceChecker)
        .counter(APPS_COMPLETED, 1)
        .checkAgainst(parentUserSource, true);
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

  private static AppSchedulingInfo mockApp(String user) {
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

  private static MetricsSource userSource(MetricsSystem ms, String queue,
      String user) {
    return ms.getSource(QueueMetrics.sourceName(queue).
        append(",user=").append(user).toString());
  }
}
