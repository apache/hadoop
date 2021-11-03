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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import java.util.Comparator;

/**
 * The comparator used to specify the metric against which the load
 * of two Nodes are compared.
 */
public enum LoadComparator implements Comparator<ClusterNode> {
  QUEUE_LENGTH, QUEUE_WAIT_TIME;

  @Override public int compare(ClusterNode o1, ClusterNode o2) {
    if (getMetric(o1) == getMetric(o2)) {
      return (int) (o2.getTimestamp() - o1.getTimestamp());
    }
    return getMetric(o1) - getMetric(o2);
  }

  public int getMetric(ClusterNode c) {
    return (this == QUEUE_LENGTH) ?
        c.getQueueLength().get() :
        c.getQueueWaitTime().get();
  }

  /**
   * Increment the metric by a delta if it is below the threshold.
   *
   * @param c             ClusterNode
   * @param incrementSize increment size
   * @return true if the metric was below threshold and was incremented.
   */
  public boolean compareAndIncrement(ClusterNode c, int incrementSize) {
    if (this == QUEUE_LENGTH) {
      int ret = c.getQueueLength().addAndGet(incrementSize);
      if (ret <= c.getQueueCapacity()) {
        return true;
      }
      c.getQueueLength().addAndGet(-incrementSize);
      return false;
    }
    // for queue wait time, we don't have any threshold.
    return true;
  }
}
