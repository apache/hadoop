/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;

import java.util.function.Consumer;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .TestQueueMetrics.userSource;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class holds queue and user metrics for a particular queue,
 * used for testing metrics.
 * Reference for the parent queue is also stored for every queue,
 * except if the queue is root.
 */
public final class QueueInfo {
  private final QueueInfo parentQueueInfo;
  private final Queue queue;
  final QueueMetrics queueMetrics;
  final MetricsSource queueSource;
  final MetricsSource userSource;

  public QueueInfo(QueueInfo parent, String queueName, MetricsSystem ms,
      Configuration conf, String user) {
    Queue parentQueue = parent == null ? null : parent.queue;
    parentQueueInfo = parent;
    queueMetrics =
        QueueMetrics.forQueue(ms, queueName, parentQueue, true, conf);
    queue = mock(Queue.class);
    when(queue.getMetrics()).thenReturn(queueMetrics);
    queueSource = ms.getSource(QueueMetrics.sourceName(queueName).toString());

    // need to call getUserMetrics so that a non-null userSource is returned
    // with the call to userSource(..)
    queueMetrics.getUserMetrics(user);
    userSource = userSource(ms, queueName, user);
  }

  public QueueInfo getRoot() {
    QueueInfo root = this;
    while (root.parentQueueInfo != null) {
      root = root.parentQueueInfo;
    }
    return root;
  }

  public void checkAllQueueSources(Consumer<MetricsSource> consumer) {
    checkQueueSourcesRecursive(this, consumer);
  }

  private void checkQueueSourcesRecursive(QueueInfo queueInfo,
      Consumer<MetricsSource> consumer) {
    consumer.accept(queueInfo.queueSource);
    if (queueInfo.parentQueueInfo != null) {
      checkQueueSourcesRecursive(queueInfo.parentQueueInfo, consumer);
    }
  }

  public void checkAllQueueMetrics(Consumer<QueueMetrics> consumer) {
    checkAllQueueMetricsRecursive(this, consumer);
  }

  private void checkAllQueueMetricsRecursive(QueueInfo queueInfo, Consumer
      <QueueMetrics> consumer) {
    consumer.accept(queueInfo.queueMetrics);
    if (queueInfo.parentQueueInfo != null) {
      checkAllQueueMetricsRecursive(queueInfo.parentQueueInfo, consumer);
    }
  }
}