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
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.BuilderUtils;
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

    metrics.submitApp(user, 1);
    MetricsSource userSource = userSource(ms, queueName, user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0);

    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB));
    metrics.incrPendingResources(user, 5, Resources.createResource(15*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 0, 0, 100*GB, 15*GB, 5, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    checkResources(queueSource, 6*GB, 3, 3, 0, 100*GB, 9*GB, 2, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    checkResources(queueSource, 4*GB, 2, 3, 1, 100*GB, 9*GB, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
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

    metrics.submitApp(user, 1);
    MetricsSource userSource = userSource(ms, queueName, user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FAILED);
    checkApps(queueSource, 1, 0, 0, 0, 1, 0);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitApp(user, 2);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);

    // Suppose say application has failed this time as well.
    metrics.finishApp(app, RMAppAttemptState.FAILED);
    checkApps(queueSource, 1, 0, 0, 0, 1, 0);

    // As the application has failed, framework retries the same application
    // based on configuration
    metrics.submitApp(user, 3);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);

    // Suppose say application has finished.
    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);

    assertNull(userSource);
  }

  @Test public void testSingleQueueWithUserMetrics() {
    String queueName = "single2";
    String user = "dodo";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, true,
						 conf);
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user, 1);
    MetricsSource userSource = userSource(ms, queueName, user);

    checkApps(queueSource, 1, 1, 0, 0, 0, 0);
    checkApps(userSource, 1, 1, 0, 0, 0, 0);

    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB));
    metrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB));
    metrics.incrPendingResources(user, 5, Resources.createResource(15*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 0, 0,  100*GB, 15*GB, 5, 0, 0);
    checkResources(userSource, 0, 0, 0, 0, 10*GB, 15*GB, 5, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);
    checkApps(userSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    checkResources(queueSource, 6*GB, 3, 3, 0, 100*GB, 9*GB, 2, 0, 0);
    checkResources(userSource, 6*GB, 3, 3, 0, 10*GB, 9*GB, 2, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    checkResources(queueSource, 4*GB, 2, 3, 1, 100*GB, 9*GB, 2, 0, 0);
    checkResources(userSource, 4*GB, 2, 3, 1, 10*GB, 9*GB, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
    checkApps(userSource, 1, 0, 0, 1, 0, 0);
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

    metrics.submitApp(user, 1);
    MetricsSource userSource = userSource(ms, leafQueueName, user);
    MetricsSource parentUserSource = userSource(ms, parentQueueName, user);

    checkApps(queueSource, 1, 1, 0, 0, 0, 0);
    checkApps(parentQueueSource, 1, 1, 0, 0, 0, 0);
    checkApps(userSource, 1, 1, 0, 0, 0, 0);
    checkApps(parentUserSource, 1, 1, 0, 0, 0, 0);

    parentMetrics.setAvailableResourcesToQueue(Resources.createResource(100*GB));
    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB));
    parentMetrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB));
    metrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB));
    metrics.incrPendingResources(user, 5, Resources.createResource(15*GB));
    checkResources(queueSource, 0, 0, 0, 0, 100*GB, 15*GB, 5, 0, 0);
    checkResources(parentQueueSource, 0, 0, 0, 0, 100*GB, 15*GB, 5, 0, 0);
    checkResources(userSource, 0, 0, 0, 0, 10*GB, 15*GB, 5, 0, 0);
    checkResources(parentUserSource, 0, 0, 0, 0, 10*GB, 15*GB, 5, 0, 0);

    metrics.incrAppsRunning(app, user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);
    checkApps(userSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    metrics.reserveResource(user, Resources.createResource(3*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 6*GB, 3, 3, 0, 100*GB, 9*GB, 2, 3*GB, 1);
    checkResources(parentQueueSource, 6*GB, 3, 3, 0,  100*GB, 9*GB, 2, 3*GB, 1);
    checkResources(userSource, 6*GB, 3, 3, 0, 10*GB, 9*GB, 2, 3*GB, 1);
    checkResources(parentUserSource, 6*GB, 3, 3, 0, 10*GB, 9*GB, 2, 3*GB, 1);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    metrics.unreserveResource(user, Resources.createResource(3*GB));
    checkResources(queueSource, 4*GB, 2, 3, 1, 100*GB, 9*GB, 2, 0, 0);
    checkResources(parentQueueSource, 4*GB, 2, 3, 1, 100*GB, 9*GB, 2, 0, 0);
    checkResources(userSource, 4*GB, 2, 3, 1, 10*GB, 9*GB, 2, 0, 0);
    checkResources(parentUserSource, 4*GB, 2, 3, 1, 10*GB, 9*GB, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
    checkApps(parentQueueSource, 1, 0, 0, 1, 0, 0);
    checkApps(userSource, 1, 0, 0, 1, 0, 0);
    checkApps(parentUserSource, 1, 0, 0, 1, 0, 0);
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


  public static void checkApps(MetricsSource source, int submitted, int pending,
      int running, int completed, int failed, int killed) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("AppsSubmitted", submitted, rb);
    assertGauge("AppsPending", pending, rb);
    assertGauge("AppsRunning", running, rb);
    assertCounter("AppsCompleted", completed, rb);
    assertGauge("AppsFailed", failed, rb);
    assertCounter("AppsKilled", killed, rb);
  }

  public static void checkResources(MetricsSource source, int allocatedMB,
      int allocCtnrs, long aggreAllocCtnrs, long aggreReleasedCtnrs, 
      int availableMB, int pendingMB, int pendingCtnrs,
      int reservedMB, int reservedCtnrs) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertGauge("AllocatedMB", allocatedMB, rb);
    assertGauge("AllocatedContainers", allocCtnrs, rb);
    assertCounter("AggregateContainersAllocated", aggreAllocCtnrs, rb);
    assertCounter("AggregateContainersReleased", aggreReleasedCtnrs, rb);
    assertGauge("AvailableMB", availableMB, rb);
    assertGauge("PendingMB", pendingMB, rb);
    assertGauge("PendingContainers", pendingCtnrs, rb);
    assertGauge("ReservedMB", reservedMB, rb);
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
