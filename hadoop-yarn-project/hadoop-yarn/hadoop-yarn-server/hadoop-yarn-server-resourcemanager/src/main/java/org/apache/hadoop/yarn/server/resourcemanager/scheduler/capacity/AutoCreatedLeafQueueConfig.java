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

/**
 * Auto Created Leaf queue configurations, capacity
 */
public class AutoCreatedLeafQueueConfig {

  /**
   * Template queue capacities - contains configured and derived capacities
   * like abs capacity which are used by auto queue creation policy to manage
   * leaf queue capacities
   */
  private QueueCapacities queueCapacities;

  private CapacitySchedulerConfiguration leafQueueConfigs;

  public AutoCreatedLeafQueueConfig(Builder builder) {
    this.queueCapacities = builder.queueCapacities;
    this.leafQueueConfigs = builder.leafQueueConfigs;
  }

  public static class Builder {

    private QueueCapacities queueCapacities;
    private CapacitySchedulerConfiguration leafQueueConfigs;

    public Builder capacities(QueueCapacities capacities) {
      this.queueCapacities = capacities;
      return this;
    }

    public Builder configuration(CapacitySchedulerConfiguration conf) {
      this.leafQueueConfigs = conf;
      return this;
    }

    public AutoCreatedLeafQueueConfig build() {
      return new AutoCreatedLeafQueueConfig(this);
    }
  }

  public QueueCapacities getQueueCapacities() {
    return queueCapacities;
  }

  public CapacitySchedulerConfiguration getLeafQueueConfigs() {
    return leafQueueConfigs;
  }
}
