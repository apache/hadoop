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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

@SuppressWarnings({"checkstyle:visibilitymodifier", "checkstyle:hiddenfield"})
public final class FSQueueConverterBuilder {
  FSConfigToCSConfigRuleHandler ruleHandler;
  CapacitySchedulerConfiguration capacitySchedulerConfig;
  boolean preemptionEnabled;
  boolean sizeBasedWeight;
  Resource clusterResource;
  float queueMaxAMShareDefault;
  int queueMaxAppsDefault;
  ConversionOptions conversionOptions;
  boolean drfUsed;
  boolean usePercentages;

  private FSQueueConverterBuilder() {
  }

  public static FSQueueConverterBuilder create() {
    return new FSQueueConverterBuilder();
  }

  public FSQueueConverterBuilder withRuleHandler(
      FSConfigToCSConfigRuleHandler ruleHandler) {
    this.ruleHandler = ruleHandler;
    return this;
  }

  public FSQueueConverterBuilder withCapacitySchedulerConfig(
      CapacitySchedulerConfiguration capacitySchedulerConfig) {
    this.capacitySchedulerConfig = capacitySchedulerConfig;
    return this;
  }

  public FSQueueConverterBuilder withPreemptionEnabled(
      boolean preemptionEnabled) {
    this.preemptionEnabled = preemptionEnabled;
    return this;
  }

  public FSQueueConverterBuilder withSizeBasedWeight(
      boolean sizeBasedWeight) {
    this.sizeBasedWeight = sizeBasedWeight;
    return this;
  }

  public FSQueueConverterBuilder withClusterResource(
      Resource resource) {
    this.clusterResource = resource;
    return this;
  }

  public FSQueueConverterBuilder withQueueMaxAMShareDefault(
      float queueMaxAMShareDefault) {
    this.queueMaxAMShareDefault = queueMaxAMShareDefault;
    return this;
  }

  public FSQueueConverterBuilder withQueueMaxAppsDefault(
      int queueMaxAppsDefault) {
    this.queueMaxAppsDefault = queueMaxAppsDefault;
    return this;
  }

  public FSQueueConverterBuilder withConversionOptions(
      ConversionOptions conversionOptions) {
    this.conversionOptions = conversionOptions;
    return this;
  }

  public FSQueueConverterBuilder withDrfUsed(boolean drfUsed) {
    this.drfUsed = drfUsed;
    return this;
  }

  public FSQueueConverterBuilder withPercentages(boolean usePercentages) {
    this.usePercentages = usePercentages;
    return this;
  }

  public FSQueueConverter build() {
    return new FSQueueConverter(this);
  }
}
