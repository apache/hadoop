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
import static org.apache.hadoop.test.MockitoMaker.make;
import static org.apache.hadoop.test.MockitoMaker.stub;
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
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestQueueMetrics {
  static final int GB = 1024; // MB
  private static final Configuration conf = new Configuration();

  private MetricsSystem ms;

  @Before
  public void setUp() {
    ms = new MetricsSystemImpl();
    QueueMetrics.clearQueueMetrics();
  }
  
  @Test public void testDefaultSingleQueueMetrics() {
    String queueName = "single";
    String user = "alice";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false,
						 conf);
    MetricsSource queueSource= queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);

    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB, 100));
    metrics.incrPendingResources(user, 5, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 0, 0, 0, 100*GB, 100, 15*GB, 15, 5, 0, 0, 0);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB, 2), true);
    checkResources(queueSource, 6*GB, 6, 3, 3, 0, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB, 2));
    checkResources(queueSource, 4*GB, 4, 2, 3, 1, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    metrics.finishApp(user, RMAppState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0, true);
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
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);

    // Suppose say application has failed this time as well.
    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);

    // Suppose say application has failed, and there's no more retries.
    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);

    metrics.finishApp(user, RMAppState.FAILED);
    checkApps(queueSource, 1, 0, 0, 0, 1, 0, true);

    assertNull(userSource);
  }

  @Test public void testSingleQueueWithUserMetrics() {
    String queueName = "single2";
    String user = "dodo";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, true,
						 conf);
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);

    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 0, 0, 0, true);

    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 1, 0, 0, 0, 0, true);

    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB, 100));
    metrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB, 10));
    metrics.incrPendingResources(user, 5, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 0, 0, 0,  100*GB, 100, 15*GB, 15, 5, 0, 0, 0);
    checkResources(userSource, 0, 0, 0, 0, 0, 10*GB, 10, 15*GB, 15, 5, 0, 0, 0);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 1, 0, 0, 0, true);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB, 2), true);
    checkResources(queueSource, 6*GB, 6, 3, 3, 0, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);
    checkResources(userSource, 6*GB, 6, 3, 3, 0, 10*GB, 10, 9*GB, 9, 2, 0, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB, 2));
    checkResources(queueSource, 4*GB, 4, 2, 3, 1, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);
    checkResources(userSource, 4*GB, 4, 2, 3, 1, 10*GB, 10, 9*GB, 9, 2, 0, 0, 0);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 0, 0, 0, true);
    metrics.finishApp(user, RMAppState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 1, 0, 0, true);
  }

  @Test public void testTwoLevelWithUserMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";
    String user = "alice";

    QueueMetrics parentMetrics =
      QueueMetrics.forQueue(ms, parentQueueName, null, true, conf);
    Queue parentQueue = make(stub(Queue.class).returning(parentMetrics).
        from.getMetrics());
    QueueMetrics metrics =
      QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true, conf);
    MetricsSource parentQueueSource = queueSource(ms, parentQueueName);
    MetricsSource queueSource = queueSource(ms, leafQueueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, leafQueueName, user);
    MetricsSource parentUserSource = userSource(ms, parentQueueName, user);

    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(parentQueueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(parentUserSource, 1, 0, 0, 0, 0, 0, true);

    metrics.submitAppAttempt(user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0, true);
    checkApps(parentQueueSource, 1, 1, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 1, 0, 0, 0, 0, true);
    checkApps(parentUserSource, 1, 1, 0, 0, 0, 0, true);

    parentMetrics.setAvailableResourcesToQueue(Resources.createResource(100*GB, 100));
    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB, 100));
    parentMetrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB, 10));
    metrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB, 10));
    metrics.incrPendingResources(user, 5, Resources.createResource(3*GB, 3));
    checkResources(queueSource, 0, 0, 0, 0, 0, 100*GB, 100, 15*GB, 15, 5, 0, 0, 0);
    checkResources(parentQueueSource, 0, 0, 0, 0, 0, 100*GB, 100, 15*GB, 15, 5, 0, 0, 0);
    checkResources(userSource, 0, 0, 0, 0, 0, 10*GB, 10, 15*GB, 15, 5, 0, 0, 0);
    checkResources(parentUserSource, 0, 0, 0, 0, 0, 10*GB, 10, 15*GB, 15, 5, 0, 0, 0);

    metrics.runAppAttempt(app.getApplicationId(), user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 1, 0, 0, 0, true);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB, 2), true);
    metrics.reserveResource(user, Resources.createResource(3*GB, 3));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 6*GB, 6, 3, 3, 0, 100*GB, 100, 9*GB, 9, 2, 3*GB, 3, 1);
    checkResources(parentQueueSource, 6*GB, 6, 3, 3, 0,  100*GB, 100, 9*GB, 9, 2, 3*GB, 3, 1);
    checkResources(userSource, 6*GB, 6, 3, 3, 0, 10*GB, 10, 9*GB, 9, 2, 3*GB, 3, 1);
    checkResources(parentUserSource, 6*GB, 6, 3, 3, 0, 10*GB, 10, 9*GB, 9, 2, 3*GB, 3, 1);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB, 2));
    metrics.unreserveResource(user, Resources.createResource(3*GB, 3));
    checkResources(queueSource, 4*GB, 4, 2, 3, 1, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);
    checkResources(parentQueueSource, 4*GB, 4, 2, 3, 1, 100*GB, 100, 9*GB, 9, 2, 0, 0, 0);
    checkResources(userSource, 4*GB, 4, 2, 3, 1, 10*GB, 10, 9*GB, 9, 2, 0, 0, 0);
    checkResources(parentUserSource, 4*GB, 4, 2, 3, 1, 10*GB, 10, 9*GB, 9, 2, 0, 0, 0);

    metrics.finishAppAttempt(
        app.getApplicationId(), app.isPending(), app.getUser());
    checkApps(queueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(parentQueueSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 0, 0, 0, true);
    checkApps(parentUserSource, 1, 0, 0, 0, 0, 0, true);

    metrics.finishApp(user, RMAppState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0, true);
    checkApps(parentQueueSource, 1, 0, 0, 1, 0, 0, true);
    checkApps(userSource, 1, 0, 0, 1, 0, 0, true);
    checkApps(parentUserSource, 1, 0, 0, 1, 0, 0, true);
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
      Queue parentQueue1 = make(stub(Queue.class).returning(p1Metrics).
          from.getMetrics());
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
    checkApps(metrics, 0, 0, 0, 0, 0, 0, true);
    MetricsAsserts.assertGauge("ReservedContainers", 0, metrics);
  }

  // This is to test all metrics can consistently show up if specified true to
  // collect all metrics, even though they are not modified from last time they
  // are collected. If not collecting all metrics, only modified metrics will show up.
  @Test
  public void testCollectAllMetrics() {
    String queueName = "single";
    QueueMetrics.forQueue(ms, queueName, null, false, conf);
    MetricsSource queueSource = queueSource(ms, queueName);

    checkApps(queueSource, 0, 0, 0, 0, 0, 0, true);
    try {
      // do not collect all metrics
      checkApps(queueSource, 0, 0, 0, 0, 0, 0, false);
      Assert.fail();
    } catch (AssertionError e) {
      Assert.assertTrue(e.getMessage().contains(
        "Expected exactly one metric for name "));
    }
    // collect all metrics
    checkApps(queueSource, 0, 0, 0, 0, 0, 0, true);
  }

  public static void checkApps(MetricsSource source, int submitted, int pending,
      int running, int completed, int failed, int killed, boolean all) {
    MetricsRecordBuilder rb = getMetrics(source, all);
    assertCounter("AppsSubmitted", submitted, rb);
    assertGauge("AppsPending", pending, rb);
    assertGauge("AppsRunning", running, rb);
    assertCounter("AppsCompleted", completed, rb);
    assertCounter("AppsFailed", failed, rb);
    assertCounter("AppsKilled", killed, rb);
  }

  public static void checkResources(MetricsSource source, int allocatedMB,
      int allocatedCores, int allocCtnrs, long aggreAllocCtnrs,
      long aggreReleasedCtnrs, int availableMB, int availableCores, int pendingMB,
      int pendingCores, int pendingCtnrs, int reservedMB, int reservedCores,
      int reservedCtnrs) {
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

  private static AppSchedulingInfo mockApp(String user) {
    AppSchedulingInfo app = mock(AppSchedulingInfo.class);
    when(app.getUser()).thenReturn(user);
    ApplicationId appId = BuilderUtils.newApplicationId(1, 1);
    ApplicationAttemptId id = BuilderUtils.newApplicationAttemptId(appId, 1);
    when(app.getApplicationAttemptId()).thenReturn(id);
    return app;
  }

  public static MetricsSource queueSource(MetricsSystem ms, String queue) {
    MetricsSource s = ms.getSource(QueueMetrics.sourceName(queue).toString());
    return s;
  }

  public static MetricsSource userSource(MetricsSystem ms, String queue,
                                         String user) {
    MetricsSource s = ms.getSource(QueueMetrics.sourceName(queue).
        append(",user=").append(user).toString());
    return s;
  }
}
