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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.ConfigSnapshot;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver.QueueProperties;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_SETTINGS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePrefixes.getQueuePrefix;

public final class UserWeights {
  public static final float DEFAULT_WEIGHT = 1.0F;
  /**
   * Key: Username,
   * Value: Weight as float.
   */
  private final Map<String, Float> data = new HashMap<>();

  private UserWeights() {}

  public static UserWeights createEmpty() {
    return new UserWeights();
  }

  public static UserWeights createByConfig(
      CapacitySchedulerConfiguration conf,
      ConfigSnapshot snapshot,
      QueuePath queuePath) {
    String queuePathPlusPrefix = getQueuePrefix(queuePath) + USER_SETTINGS;
    Map<String, String> props = snapshot
        .getRawPropertiesWithPrefix(queuePathPlusPrefix, false);

    UserWeights userWeights = new UserWeights();
    userWeights.data.putAll(QueueProperties.USER_WEIGHTS.read(
        conf::substituteCommonVariables, queuePath, NO_LABEL, props));
    return userWeights;
  }

  /**
   * Creates the user weights of a queue from its resolved weights.
   * @param weights user name to weight
   * @return the user weights
   */
  public static UserWeights createFrom(Map<String, Float> weights) {
    UserWeights userWeights = new UserWeights();
    userWeights.data.putAll(weights);
    return userWeights;
  }

  public float getByUser(String userName) {
    Float weight = data.get(userName);
    if (weight == null) {
      return DEFAULT_WEIGHT;
    }
    return weight;
  }

  public void validateForLeafQueue(float queueUserLimit, String queuePath) throws IOException {
    String error =
        QueueLimitChecks.checkUserWeights(queuePath, queueUserLimit, data);
    if (error != null) {
      throw new IOException(error);
    }
  }

  public void addFrom(UserWeights addFrom) {
    data.putAll(addFrom.data);
  }
}
