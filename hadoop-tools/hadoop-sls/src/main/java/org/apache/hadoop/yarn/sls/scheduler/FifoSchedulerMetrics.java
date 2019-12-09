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

package org.apache.hadoop.yarn.sls.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo
        .FifoScheduler;

import com.codahale.metrics.Gauge;

@Private
@Unstable
public class FifoSchedulerMetrics extends SchedulerMetrics {
  
  public FifoSchedulerMetrics() {
    super();
  }

  @Override
  protected void registerQueueMetrics(String queueName) {
    super.registerQueueMetrics(queueName);

    FifoScheduler fifo = (FifoScheduler) scheduler;
    // for FifoScheduler, only DEFAULT_QUEUE
    // here the three parameters doesn't affect results
    final QueueInfo queue = fifo.getQueueInfo(queueName, false, false);
    // track currentCapacity, maximumCapacity (always 1.0f)
    metrics.register("variable.queue." + queueName + ".currentcapacity",
      new Gauge<Float>() {
        @Override
        public Float getValue() {
          return queue.getCurrentCapacity();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".",
      new Gauge<Float>() {
        @Override
        public Float getValue() {
          return queue.getCurrentCapacity();
        }
      }
    );
  }
}
