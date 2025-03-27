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
import java.util.regex.Matcher;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_SETTINGS;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_WEIGHT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.USER_WEIGHT_PATTERN;
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
      ConfigurationProperties configurationProperties,
      QueuePath queuePath) {
    String queuePathPlusPrefix = getQueuePrefix(queuePath) + USER_SETTINGS;
    Map<String, String> props = configurationProperties
        .getPropertiesWithPrefix(queuePathPlusPrefix);

    UserWeights userWeights = new UserWeights();
    for (Map.Entry<String, String> item: props.entrySet()) {
      Matcher m = USER_WEIGHT_PATTERN.matcher(item.getKey());
      if (m.find()) {
        String userName = item.getKey().replaceFirst("\\." + USER_WEIGHT, "");
        if (!userName.isEmpty()) {
          String value = conf.substituteCommonVariables(item.getValue());
          userWeights.data.put(userName, new Float(value));
        }
      }
    }
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
    for (Map.Entry<String, Float> e : data.entrySet()) {
      String userName = e.getKey();
      float weight = e.getValue();
      if (weight < 0.0F || weight > (100.0F / queueUserLimit)) {
        throw new IOException("Weight (" + weight + ") for user \"" + userName
            + "\" must be between 0 and" + " 100 / " + queueUserLimit + " (= " +
            100.0f / queueUserLimit + ", the number of concurrent active users in "
            + queuePath + ")");
      }
    }
  }

  public void addFrom(UserWeights addFrom) {
    data.putAll(addFrom.data);
  }
}
