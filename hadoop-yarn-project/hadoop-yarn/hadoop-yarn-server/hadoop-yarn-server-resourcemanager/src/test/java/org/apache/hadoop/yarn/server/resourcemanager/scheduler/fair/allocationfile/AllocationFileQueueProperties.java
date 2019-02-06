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
 * The purpose of this class is to store all properties of a queue.
 */
public class AllocationFileQueueProperties {
  private final String queueName;
  private final String minResources;
  private final String maxResources;
  private final String aclAdministerApps;
  private final String aclSubmitApps;
  private final String schedulingPolicy;
  private final Integer maxRunningApps;
  private final Double maxAMShare;
  private final Integer minSharePreemptionTimeout;
  private final Boolean parent;
  private final String maxChildResources;
  private final Integer fairSharePreemptionTimeout;
  private final Double fairSharePreemptionThreshold;
  private final String maxContainerAllocation;

  AllocationFileQueueProperties(Builder builder) {
    this.queueName = builder.queueName;
    this.parent = builder.parent;
    this.minResources = builder.minResources;
    this.maxResources = builder.maxResources;
    this.aclAdministerApps = builder.aclAdministerApps;
    this.aclSubmitApps = builder.aclSubmitApps;
    this.schedulingPolicy = builder.schedulingPolicy;
    this.maxRunningApps = builder.maxRunningApps;
    this.maxAMShare = builder.maxAMShare;
    this.minSharePreemptionTimeout = builder.minSharePreemptionTimeout;
    this.maxChildResources = builder.maxChildResources;
    this.fairSharePreemptionTimeout = builder.fairSharePreemptionTimeout;
    this.fairSharePreemptionThreshold = builder.fairSharePreemptionThreshold;
    this.maxContainerAllocation = builder.maxContainerAllocation;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getMinResources() {
    return minResources;
  }

  public String getMaxResources() {
    return maxResources;
  }

  public String getAclAdministerApps() {
    return aclAdministerApps;
  }

  public String getAclSubmitApps() {
    return aclSubmitApps;
  }

  public String getSchedulingPolicy() {
    return schedulingPolicy;
  }

  public Integer getMaxRunningApps() {
    return maxRunningApps;
  }

  public Double getMaxAMShare() {
    return maxAMShare;
  }

  public Integer getMinSharePreemptionTimeout() {
    return minSharePreemptionTimeout;
  }

  public Boolean getParent() {
    return parent;
  }

  public String getMaxChildResources() {
    return maxChildResources;
  }

  public Integer getFairSharePreemptionTimeout() {
    return fairSharePreemptionTimeout;
  }

  public Double getFairSharePreemptionThreshold() {
    return fairSharePreemptionThreshold;
  }

  public String getMaxContainerAllocation() {
    return maxContainerAllocation;
  }

  /**
   * Builder class for {@link AllocationFileQueueProperties}.
   */
  public static final class Builder {
    private String queueName;
    private Boolean parent = false;
    private String minResources;
    private String maxResources;
    private String aclAdministerApps;
    private String aclSubmitApps;
    private String schedulingPolicy;
    private Integer maxRunningApps;
    private Double maxAMShare;
    private Integer minSharePreemptionTimeout;
    private String maxChildResources;
    private Integer fairSharePreemptionTimeout;
    private Double fairSharePreemptionThreshold;
    private String maxContainerAllocation;

    Builder() {
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder queueName(String queueName) {
      this.queueName = queueName;
      return this;
    }

    public Builder minResources(String minResources) {
      this.minResources = minResources;
      return this;
    }

    public Builder maxResources(String maxResources) {
      this.maxResources = maxResources;
      return this;
    }

    public Builder aclAdministerApps(String aclAdministerApps) {
      this.aclAdministerApps = aclAdministerApps;
      return this;
    }

    public Builder aclSubmitApps(String aclSubmitApps) {
      this.aclSubmitApps = aclSubmitApps;
      return this;
    }

    public Builder schedulingPolicy(String schedulingPolicy) {
      this.schedulingPolicy = schedulingPolicy;
      return this;
    }

    public Builder maxRunningApps(Integer maxRunningApps) {
      this.maxRunningApps = maxRunningApps;
      return this;
    }

    public Builder maxAMShare(Double maxAMShare) {
      this.maxAMShare = maxAMShare;
      return this;
    }

    public Builder maxContainerAllocation(String maxContainerAllocation) {
      this.maxContainerAllocation = maxContainerAllocation;
      return this;
    }

    public Builder minSharePreemptionTimeout(
        Integer minSharePreemptionTimeout) {
      this.minSharePreemptionTimeout = minSharePreemptionTimeout;
      return this;
    }

    public Builder parent(Boolean parent) {
      this.parent = parent;
      return this;
    }

    public Builder maxChildResources(String maxChildResources) {
      this.maxChildResources = maxChildResources;
      return this;
    }

    public Builder fairSharePreemptionTimeout(
        Integer fairSharePreemptionTimeout) {
      this.fairSharePreemptionTimeout = fairSharePreemptionTimeout;
      return this;
    }

    public Builder fairSharePreemptionThreshold(
        Double fairSharePreemptionThreshold) {
      this.fairSharePreemptionThreshold = fairSharePreemptionThreshold;
      return this;
    }

    public AllocationFileQueueProperties build() {
      return new AllocationFileQueueProperties(this);
    }
  }
}
