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

import com.google.common.collect.Maps;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * This class is a main entry-point for any kind of metrics for
 * custom resources.
 * It provides increase and decrease methods for all types of metrics.
 */
public class QueueMetricsForCustomResources {
  /**
   * Class that holds metrics values for custom resources in a map keyed with
   * the name of the custom resource.
   * There are different kinds of values like allocated, available and others.
   */
  public static class QueueMetricsCustomResource {
    private final Map<String, Long> values = Maps.newHashMap();

    protected void increase(Resource res) {
      update(res, Long::sum);
    }

    void increaseWithMultiplier(Resource res, long multiplier) {
      update(res, (v1, v2) -> v1 + v2 * multiplier);
    }

    protected void decrease(Resource res) {
      update(res, (v1, v2) -> v1 - v2);
    }

    void decreaseWithMultiplier(Resource res, int containers) {
      update(res, (v1, v2) -> v1 - v2 * containers);
    }

    protected void set(Resource res) {
      update(res, (v1, v2) -> v2);
    }

    private void update(Resource res, BiFunction<Long, Long, Long> operation) {
      if (ResourceUtils.getNumberOfKnownResourceTypes() > 2) {
        ResourceInformation[] resources = res.getResources();

        for (int i = 2; i < resources.length; i++) {
          ResourceInformation resource = resources[i];

          // Map.merge only applies operation if there is
          // a value for the key in the map
          if (!values.containsKey(resource.getName())) {
            values.put(resource.getName(), 0L);
          }
          values.merge(resource.getName(),
              resource.getValue(), operation);
        }
      }
    }

    public Map<String, Long> getValues() {
      return values;
    }
  }
  private final QueueMetricsCustomResource aggregatePreemptedSeconds =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource allocated =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource available =
      new QueueMetricsCustomResource();
  private final QueueMetricsCustomResource pending =
      new QueueMetricsCustomResource();

  private final QueueMetricsCustomResource reserved =
      new QueueMetricsCustomResource();

  public void increaseReserved(Resource res) {
    reserved.increase(res);
  }

  public void decreaseReserved(Resource res) {
    reserved.decrease(res);
  }

  public void setAvailable(Resource res) {
    available.set(res);
  }

  public void increasePending(Resource res, int containers) {
    pending.increaseWithMultiplier(res, containers);
  }

  public void decreasePending(Resource res) {
    pending.decrease(res);
  }

  public void decreasePending(Resource res, int containers) {
    pending.decreaseWithMultiplier(res, containers);
  }

  public void increaseAllocated(Resource res) {
    allocated.increase(res);
  }

  public void increaseAllocated(Resource res, int containers) {
    allocated.increaseWithMultiplier(res, containers);
  }

  public void decreaseAllocated(Resource res) {
    allocated.decrease(res);
  }

  public void decreaseAllocated(Resource res, int containers) {
    allocated.decreaseWithMultiplier(res, containers);
  }

  public void increaseAggregatedPreemptedSeconds(Resource res, long seconds) {
    aggregatePreemptedSeconds.increaseWithMultiplier(res, seconds);
  }

  Map<String, Long> getAllocatedValues() {
    return allocated.getValues();
  }

  Map<String, Long> getAvailableValues() {
    return available.getValues();
  }

  Map<String, Long> getPendingValues() {
    return pending.getValues();
  }

  Map<String, Long> getReservedValues() {
    return reserved.getValues();
  }

  QueueMetricsCustomResource getAggregatePreemptedSeconds() {
    return aggregatePreemptedSeconds;
  }
}
