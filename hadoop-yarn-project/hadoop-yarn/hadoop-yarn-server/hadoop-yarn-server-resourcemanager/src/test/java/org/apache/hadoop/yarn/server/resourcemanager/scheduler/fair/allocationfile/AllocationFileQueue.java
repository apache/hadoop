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

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .allocationfile.AllocationFileWriter.addIfPresent;

/**
 * DAO for Allocation File Queue.
 */
final public class AllocationFileQueue {
  private static final String DEFAULT_TAG_NAME = "queue";
  private static final String LEGACY_TAG_NAME = "pool";

  private final String queueName;
  private final String minResources;
  private final String maxResources;
  private final String aclAdministerApps;
  private final String aclSubmitApps;
  private final String aclSubmitReservations;
  private final String aclAdministerReservations;
  private final String aclListReservations;
  private final String schedulingPolicy;
  private final Integer maxRunningApps;
  private final Double maxAMShare;
  private final Boolean allowPreemptionFrom;
  private final Integer minSharePreemptionTimeout;
  private final String maxChildResources;
  private final Integer fairSharePreemptionTimeout;
  private final Double fairSharePreemptionThreshold;
  private final String maxContainerAllocation;
  private final List<AllocationFileQueue> subQueues;
  private final Float weight;
  private String tagName;

  private final boolean parent;
  private final boolean reservation;

  private AllocationFileQueue(Builder builder) {
    this.queueName = builder.name;
    this.parent = builder.parent;
    this.minResources = builder.minResources;
    this.maxResources = builder.maxResources;
    this.aclAdministerApps = builder.aclAdministerApps;
    this.aclSubmitApps = builder.aclSubmitApps;
    this.aclSubmitReservations = builder.aclSubmitReservations;
    this.aclAdministerReservations = builder.aclAdministerReservations;
    this.aclListReservations = builder.aclListReservations;
    this.schedulingPolicy = builder.schedulingPolicy;
    this.maxRunningApps = builder.maxRunningApps;
    this.maxAMShare = builder.maxAMShare;
    this.allowPreemptionFrom = builder.allowPreemptionFrom;
    this.minSharePreemptionTimeout = builder.minSharePreemptionTimeout;
    this.maxChildResources = builder.maxChildResources;
    this.fairSharePreemptionTimeout = builder.fairSharePreemptionTimeout;
    this.fairSharePreemptionThreshold = builder.fairSharePreemptionThreshold;
    this.maxContainerAllocation = builder.maxContainerAllocation;
    this.weight = builder.weight;
    this.reservation = builder.reservation;
    this.subQueues = builder.subQueues;
    this.tagName = DEFAULT_TAG_NAME;
  }

  String render() {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    printStartTag(pw);
    AllocationFileWriter.printQueues(pw, subQueues,
        tagName.equals(LEGACY_TAG_NAME));
    addIfPresent(pw, "minResources", minResources);
    addIfPresent(pw, "maxResources", maxResources);
    addIfPresent(pw, "aclAdministerApps", aclAdministerApps);
    addIfPresent(pw, "aclSubmitApps", aclSubmitApps);
    addIfPresent(pw, "aclSubmitReservations", aclSubmitReservations);
    addIfPresent(pw, "aclAdministerReservations", aclAdministerReservations);
    addIfPresent(pw, "aclListReservations", aclListReservations);
    addIfPresent(pw, "schedulingPolicy", schedulingPolicy);
    addIfPresent(pw, "maxRunningApps", maxRunningApps);
    addIfPresent(pw, "maxAMShare", maxAMShare);
    addIfPresent(pw, "allowPreemptionFrom", allowPreemptionFrom);
    addIfPresent(pw, "minSharePreemptionTimeout", minSharePreemptionTimeout);
    addIfPresent(pw, "maxChildResources", maxChildResources);
    addIfPresent(pw, "fairSharePreemptionTimeout", fairSharePreemptionTimeout);
    addIfPresent(pw, "fairSharePreemptionThreshold",
        fairSharePreemptionThreshold);
    addIfPresent(pw, "maxContainerAllocation", maxContainerAllocation);
    addIfPresent(pw, "weight", weight);
    if (reservation) {
      pw.println("<reservation></reservation>");
    }
    printEndTag(pw);
    pw.close();

    return sw.toString();
  }

  String renderWithLegacyTag() {
    this.tagName = LEGACY_TAG_NAME;
    return render();
  }

  private void printStartTag(PrintWriter pw) {
    String queueWithName = String.format("<%s name=\"%s\"", tagName, queueName);
    pw.print(queueWithName);
    if (parent) {
      pw.print(" type=\"parent\"");
    }
    pw.println(">");
  }

  private void printEndTag(PrintWriter pw) {
    pw.println("</" + tagName + ">");
  }

  /**
   * Class that can build queues (with subqueues) for testcases.
   * The intention of having this class to group the common properties of
   * simple queues and subqueues by methods delegating calls to a
   * queuePropertiesBuilder instance.
   */
  public static class Builder {
    private String name;
    private String minResources;
    private String maxResources;
    private String aclAdministerApps;
    private String aclSubmitApps;
    private String aclSubmitReservations;
    private String aclAdministerReservations;
    private String aclListReservations;
    private String schedulingPolicy;
    private Integer maxRunningApps;
    private Double maxAMShare;
    private Boolean allowPreemptionFrom;
    private Integer minSharePreemptionTimeout;
    private boolean parent;
    private String maxChildResources;
    private Integer fairSharePreemptionTimeout;
    private Double fairSharePreemptionThreshold;
    private String maxContainerAllocation;
    private boolean reservation;
    private final List<AllocationFileQueue> subQueues = Lists.newArrayList();
    private Float weight;

    public Builder(String name) {
      this.name = name;
    }

    public Builder parent(boolean value) {
      this.parent = value;
      return this;
    }

    public Builder minResources(String value) {
      this.minResources = value;
      return this;
    }

    public Builder maxResources(String value) {
      this.maxResources = value;
      return this;
    }

    public Builder aclAdministerApps(String value) {
      this.aclAdministerApps = value;
      return this;
    }

    public Builder aclSubmitApps(String value) {
      this.aclSubmitApps = value;
      return this;
    }

    public Builder aclSubmitReservations(String value) {
      this.aclSubmitReservations = value;
      return this;
    }

    public Builder aclAdministerReservations(String value) {
      this.aclAdministerReservations = value;
      return this;
    }

    public Builder aclListReservations(String value) {
      this.aclListReservations = value;
      return this;
    }

    public Builder schedulingPolicy(String value) {
      this.schedulingPolicy = value;
      return this;
    }

    public Builder maxRunningApps(int value) {
      this.maxRunningApps = value;
      return this;
    }

    public Builder maxAMShare(double value) {
      this.maxAMShare = value;
      return this;
    }

    public Builder allowPreemptionFrom(boolean value) {
      this.allowPreemptionFrom = value;
      return this;
    }

    public Builder minSharePreemptionTimeout(int value) {
      this.minSharePreemptionTimeout = value;
      return this;
    }

    public Builder maxChildResources(String value) {
      this.maxChildResources = value;
      return this;
    }

    public Builder fairSharePreemptionTimeout(Integer value) {
      this.fairSharePreemptionTimeout = value;
      return this;
    }

    public Builder fairSharePreemptionThreshold(
        double value) {
      this.fairSharePreemptionThreshold = value;
      return this;
    }

    public Builder maxContainerAllocation(String value) {
      this.maxContainerAllocation = value;
      return this;
    }

    public Builder weight(float value) {
      this.weight = value;
      return this;
    }

    public Builder reservation() {
      this.reservation = true;
      return this;
    }

    public Builder subQueue(AllocationFileQueue queue) {
      if (queue == null) {
        throw new IllegalArgumentException("Subqueue cannot be null!");
      }
      subQueues.add(queue);
      return this;
    }

    public AllocationFileQueue build() {
      return new AllocationFileQueue(this);
    }
  }
}
