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

import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.apache.hadoop.test.MockitoMaker.*;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestQueueMetrics {
  static final int GB = 1024; // MB

  final MetricsSystem ms = new MetricsSystemImpl();

  @Test public void testDefaultSingleQueueMetrics() {
    String queueName = "single";
    String user = "alice";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, false);
    MetricsSource queueSource= queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);
    checkApps(queueSource, 1, 1, 0, 0, 0, 0);

    metrics.setAvailableResourcesToQueue(Resource.createResource(100*GB));
    metrics.incrPendingResources(user, 5, Resources.createResource(15*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 100, 15, 5, 0, 0);

    metrics.incrAppsRunning(user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    checkResources(queueSource, 6, 3, 100, 9, 2, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    checkResources(queueSource, 4, 2, 100, 9, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
    assertNull(userSource);
  }

  @Test public void testSingleQueueWithUserMetrics() {
    String queueName = "single2";
    String user = "dodo";

    QueueMetrics metrics = QueueMetrics.forQueue(ms, queueName, null, true);
    MetricsSource queueSource = queueSource(ms, queueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
    MetricsSource userSource = userSource(ms, queueName, user);

    checkApps(queueSource, 1, 1, 0, 0, 0, 0);
    checkApps(userSource, 1, 1, 0, 0, 0, 0);

    metrics.setAvailableResourcesToQueue(Resources.createResource(100*GB));
    metrics.setAvailableResourcesToUser(user, Resources.createResource(10*GB));
    metrics.incrPendingResources(user, 5, Resources.createResource(15*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 0, 0, 100, 15, 5, 0, 0);
    checkResources(userSource, 0, 0, 10, 15, 5, 0, 0);

    metrics.incrAppsRunning(user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);
    checkApps(userSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    checkResources(queueSource, 6, 3, 100, 9, 2, 0, 0);
    checkResources(userSource, 6, 3, 10, 9, 2, 0, 0);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    checkResources(queueSource, 4, 2, 100, 9, 2, 0, 0);
    checkResources(userSource, 4, 2, 10, 9, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
    checkApps(userSource, 1, 0, 0, 1, 0, 0);
  }

  @Test public void testTwoLevelWithUserMetrics() {
    String parentQueueName = "root";
    String leafQueueName = "root.leaf";
    String user = "alice";

    QueueMetrics parentMetrics =
        QueueMetrics.forQueue(ms, parentQueueName, null, true);
    Queue parentQueue = make(stub(Queue.class).returning(parentMetrics).
        from.getMetrics());
    QueueMetrics metrics =
        QueueMetrics.forQueue(ms, leafQueueName, parentQueue, true);
    MetricsSource parentQueueSource = queueSource(ms, parentQueueName);
    MetricsSource queueSource = queueSource(ms, leafQueueName);
    AppSchedulingInfo app = mockApp(user);

    metrics.submitApp(user);
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
    checkResources(queueSource, 0, 0, 100, 15, 5, 0, 0);
    checkResources(parentQueueSource, 0, 0, 100, 15, 5, 0, 0);
    checkResources(userSource, 0, 0, 10, 15, 5, 0, 0);
    checkResources(parentUserSource, 0, 0, 10, 15, 5, 0, 0);

    metrics.incrAppsRunning(user);
    checkApps(queueSource, 1, 0, 1, 0, 0, 0);
    checkApps(userSource, 1, 0, 1, 0, 0, 0);

    metrics.allocateResources(user, 3, Resources.createResource(2*GB));
    metrics.reserveResource(user, Resources.createResource(3*GB));
    // Available resources is set externally, as it depends on dynamic
    // configurable cluster/queue resources
    checkResources(queueSource, 6, 3, 100, 9, 2, 3, 1);
    checkResources(parentQueueSource, 6, 3, 100, 9, 2, 3, 1);
    checkResources(userSource, 6, 3, 10, 9, 2, 3, 1);
    checkResources(parentUserSource, 6, 3, 10, 9, 2, 3, 1);

    metrics.releaseResources(user, 1, Resources.createResource(2*GB));
    metrics.unreserveResource(user, Resources.createResource(3*GB));
    checkResources(queueSource, 4, 2, 100, 9, 2, 0, 0);
    checkResources(parentQueueSource, 4, 2, 100, 9, 2, 0, 0);
    checkResources(userSource, 4, 2, 10, 9, 2, 0, 0);
    checkResources(parentUserSource, 4, 2, 10, 9, 2, 0, 0);

    metrics.finishApp(app, RMAppAttemptState.FINISHED);
    checkApps(queueSource, 1, 0, 0, 1, 0, 0);
    checkApps(parentQueueSource, 1, 0, 0, 1, 0, 0);
    checkApps(userSource, 1, 0, 0, 1, 0, 0);
    checkApps(parentUserSource, 1, 0, 0, 1, 0, 0);
  }

  public static void checkApps(MetricsSource source, int submitted, int pending,
      int running, int completed, int failed, int killed) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("AppsSubmitted", submitted, rb);
    assertGauge("AppsPending", pending, rb);
    assertGauge("AppsRunning", running, rb);
    assertCounter("AppsCompleted", completed, rb);
    assertCounter("AppsFailed", failed, rb);
    assertCounter("AppsKilled", killed, rb);
  }

  public static void checkResources(MetricsSource source, int allocGB,
      int allocCtnrs, int availGB, int pendingGB, int pendingCtnrs,
      int reservedGB, int reservedCtnrs) {
    MetricsRecordBuilder rb = getMetrics(source);
    assertGauge("AllocatedGB", allocGB, rb);
    assertGauge("AllocatedContainers", allocCtnrs, rb);
    assertGauge("AvailableGB", availGB, rb);
    assertGauge("PendingGB", pendingGB, rb);
    assertGauge("PendingContainers", pendingCtnrs, rb);
    assertGauge("ReservedGB", reservedGB, rb);
    assertGauge("ReservedContainers", reservedCtnrs, rb);
  }

  private static AppSchedulingInfo mockApp(String user) {
    AppSchedulingInfo app = mock(AppSchedulingInfo.class);
    when(app.getUser()).thenReturn(user);
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
