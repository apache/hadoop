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

import java.util.HashMap;
import java.util.Map;

/**
 * Contains all intermediate calculation values that are common for a queue
 * branch (all siblings that have a common parent).
 */
public class QueueBranchContext {
  private final Map<String, ResourceVector> remainingResourceByLabel = new HashMap<>();
  private final Map<String, ResourceVector> normalizedResourceRatio = new HashMap<>();
  private final Map<String, ResourceVector> overallRemainingResource = new HashMap<>();
  private final Map<String, Map<String, Float>> sumWeightsPerLabel = new HashMap<>();

  /**
   * Increments the aggregated weight.
   * @param label node label
   * @param resourceName resource unit name
   * @param value weight value
   */
  public void incrementWeight(String label, String resourceName, float value) {
    sumWeightsPerLabel.putIfAbsent(label, new HashMap<>());
    sumWeightsPerLabel.get(label).put(resourceName,
        sumWeightsPerLabel.get(label).getOrDefault(resourceName, 0f) + value);
  }

  /**
   * Returns the aggregated children weights.
   * @param label node label
   * @param resourceName resource unit name
   * @return aggregated weights of children
   */
  public float getSumWeightsByResource(String label, String resourceName) {
    return sumWeightsPerLabel.get(label).get(resourceName);
  }

  /**
   * Sets the remaining resource under a parent that is available for its children to
   * occupy.
   *
   * @param label node label
   * @param resource resource vector
   */
  public void setPostCalculatorRemainingResource(String label, ResourceVector resource) {
    remainingResourceByLabel.put(label, resource);
  }

  public Map<String, ResourceVector> getNormalizedResourceRatios() {
    return normalizedResourceRatio;
  }

  /**
   * Returns the remaining resources of a parent that is still available for its
   * children.
   *
   * @param label node label
   * @return remaining resources
   */
  public ResourceVector getOverallRemainingResource(String label) {
    overallRemainingResource.putIfAbsent(label, ResourceVector.newInstance());
    return overallRemainingResource.get(label);
  }
  
  /**
   * Sets the remaining resources of a parent that is still available for its children.
   *
   * @param label node label
   * @param resourceVector resource vector
   */
  public void setOverallRemainingResource(String label, ResourceVector resourceVector) {
    overallRemainingResource.put(label, resourceVector);
  }

  /**
   * Returns the remaining resources of a parent that is still available for its
   * children. Decremented only after the calculator is finished its work on the corresponding
   * resources.
   *
   * @param label node label
   * @return remaining resources
   */
  public ResourceVector getPostCalculatorRemainingResource(String label) {
    remainingResourceByLabel.putIfAbsent(label, ResourceVector.newInstance());
    return remainingResourceByLabel.get(label);
  }
}
