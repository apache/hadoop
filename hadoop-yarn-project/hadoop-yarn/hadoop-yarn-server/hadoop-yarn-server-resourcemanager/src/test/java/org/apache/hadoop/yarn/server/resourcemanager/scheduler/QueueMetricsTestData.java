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

import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;

import static org.apache.hadoop.yarn.resourcetypes.ResourceTypesTestHelper
    .extractCustomResources;

/**
 * This class is to test standard and custom resource metrics for all types.
 * Metrics types can be one of: allocated, pending, reserved
 * and other resources.
 */
public final class QueueMetricsTestData {
  public static final class Builder {
    private int containers;
    private Resource resource;
    private Resource resourceToDecrease;
    private Map<String, Long> customResourceValues;
    private int containersToDecrease;
    private String user;
    private String partition;
    private QueueInfo queueInfo;

    private Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder withContainers(int containers) {
      this.containers = containers;
      return this;
    }

    public Builder withResourceToDecrease(Resource res, int containers) {
      this.resourceToDecrease = res;
      this.containersToDecrease = containers;
      return this;
    }

    public Builder withResources(Resource res) {
      this.resource = res;
      return this;
    }

    public Builder withUser(String user) {
      this.user = user;
      return this;
    }

    public Builder withPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public Builder withLeafQueue(QueueInfo qInfo) {
      this.queueInfo = qInfo;
      return this;
    }

    public QueueMetricsTestData build() {
      this.customResourceValues = extractCustomResources(resource);
      return new QueueMetricsTestData(this);
    }
  }

  final Map<String, Long> customResourceValues;
  final int containers;
  final Resource resourceToDecrease;
  final int containersToDecrease;
  final Resource resource;
  final String partition;
  final QueueInfo leafQueue;
  final String user;

  private QueueMetricsTestData(Builder builder) {
    this.customResourceValues = builder.customResourceValues;
    this.containers = builder.containers;
    this.resourceToDecrease = builder.resourceToDecrease;
    this.containersToDecrease = builder.containersToDecrease;
    this.resource = builder.resource;
    this.partition = builder.partition;
    this.leafQueue = builder.queueInfo;
    this.user = builder.user;
  }
}
