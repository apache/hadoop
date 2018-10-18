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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.allocationfile;

/**
 * Abstract base class for building simple queues and subqueues for testcases.
 * Currently there are two concrete types subclassed from this class:
 * {@link AllocationFileSimpleQueueBuilder} and
 * {@link AllocationFileSubQueueBuilder}.
 * The intention of having this class to group the common properties of
 * simple queues and subqueues by methods delegating calls to a
 * queuePropertiesBuilder instance.
 */
public abstract class AllocationFileQueueBuilder {
  final AllocationFileQueueProperties.Builder queuePropertiesBuilder;

  AllocationFileQueueBuilder() {
    this.queuePropertiesBuilder =
        AllocationFileQueueProperties.Builder.create();
  }

  public AllocationFileQueueBuilder parent(boolean parent) {
    this.queuePropertiesBuilder.parent(parent);
    return this;
  }

  public AllocationFileQueueBuilder minResources(String value) {
    this.queuePropertiesBuilder.minResources(value);
    return this;
  }

  public AllocationFileQueueBuilder maxResources(String value) {
    this.queuePropertiesBuilder.maxResources(value);
    return this;
  }

  public AllocationFileQueueBuilder aclAdministerApps(String value) {
    this.queuePropertiesBuilder.aclAdministerApps(value);
    return this;
  }

  public AllocationFileQueueBuilder aclSubmitApps(String value) {
    this.queuePropertiesBuilder.aclSubmitApps(value);
    return this;
  }

  public AllocationFileQueueBuilder schedulingPolicy(String value) {
    this.queuePropertiesBuilder.schedulingPolicy(value);
    return this;
  }

  public AllocationFileQueueBuilder maxRunningApps(int value) {
    this.queuePropertiesBuilder.maxRunningApps(value);
    return this;
  }

  public AllocationFileQueueBuilder maxAMShare(double value) {
    this.queuePropertiesBuilder.maxAMShare(value);
    return this;
  }

  public AllocationFileQueueBuilder minSharePreemptionTimeout(int value) {
    this.queuePropertiesBuilder.minSharePreemptionTimeout(value);
    return this;
  }

  public AllocationFileQueueBuilder maxChildResources(String value) {
    this.queuePropertiesBuilder.maxChildResources(value);
    return this;
  }

  public AllocationFileQueueBuilder fairSharePreemptionTimeout(Integer value) {
    this.queuePropertiesBuilder.fairSharePreemptionTimeout(value);
    return this;
  }

  public AllocationFileQueueBuilder fairSharePreemptionThreshold(
      double value) {
    this.queuePropertiesBuilder.fairSharePreemptionThreshold(value);
    return this;
  }

  public AllocationFileQueueBuilder maxContainerAllocation(
      String maxContainerAllocation) {
    this.queuePropertiesBuilder.maxContainerAllocation(maxContainerAllocation);
    return this;
  }

  public AllocationFileQueueBuilder subQueue(String queueName) {
    if (this instanceof AllocationFileSimpleQueueBuilder) {
      return new AllocationFileSubQueueBuilder(
          (AllocationFileSimpleQueueBuilder) this, queueName);
    } else {
      throw new IllegalStateException(
          "subQueue can only be invoked on instances of "
              + AllocationFileSimpleQueueBuilder.class);
    }
  }

  public abstract AllocationFileWriter buildQueue();

  public abstract AllocationFileSimpleQueueBuilder buildSubQueue();

  AllocationFileQueueProperties.Builder getqueuePropertiesBuilder() {
    return queuePropertiesBuilder;
  }
}
