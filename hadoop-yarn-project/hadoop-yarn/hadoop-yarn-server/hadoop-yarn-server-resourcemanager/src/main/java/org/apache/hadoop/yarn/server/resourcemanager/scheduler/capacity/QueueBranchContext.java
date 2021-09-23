/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains all intermediate calculation values that are common for a queue
 * branch (all siblings that have a common parent).
 */
public class QueueBranchContext {
  private final Map<String, CapacitySum> sumCapacitiesByLabel = new HashMap<>();
  private final Map<String, ResourceVector> remainingResourceByLabel = new HashMap<>();

  /**
   * Returns summarized capacity values for a label.
   * @param label node label
   * @return summarized capacity values
   */
  public CapacitySum getSumByLabel(String label) {
    if (!sumCapacitiesByLabel.containsKey(label)) {
      sumCapacitiesByLabel.put(label, new CapacitySum());
    }

    return sumCapacitiesByLabel.get(label);
  }

  public void setRemainingResource(String label, ResourceVector resource) {
    remainingResourceByLabel.put(label, resource);
  }

  /**
   * Returns the remaining resources of a parent that is still available for its
   * children.
   * @param label node label
   * @return remaining resources
   */
  public ResourceVector getRemainingResource(String label) {
    return remainingResourceByLabel.get(label);
  }

  public static class CapacitySum {
    private Map<String, Map<QueueCapacityType, Float>> sum = new HashMap<>();

    public void increment(QueueCapacityVectorEntry resource) {
      Map<QueueCapacityType, Float> sumByResourceName;

      if (sum.containsKey(resource.getResourceName())) {
        sumByResourceName = sum.get(resource.getResourceName());
      } else {
        sumByResourceName = new HashMap<>();
        sum.put(resource.getResourceName(), sumByResourceName);
      }

      sumByResourceName.put(resource.getVectorResourceType(), sumByResourceName.getOrDefault(
          resource.getVectorResourceType(), 0f) + resource.getResourceValue());
    }

    public float getSum(String resourceName, QueueCapacityType resourceType) {
      return sum.getOrDefault(resourceName,
          Collections.emptyMap()).getOrDefault(resourceType, 0f);
    }
  }
}
