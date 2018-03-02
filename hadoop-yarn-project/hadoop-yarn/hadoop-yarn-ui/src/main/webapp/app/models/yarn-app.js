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

import Converter from 'yarn-ui/utils/converter';
import DS from 'ember-data';

export default DS.Model.extend({
  appName: DS.attr("string"),
  user: DS.attr("string"),
  queue: DS.attr("string"),
  state: DS.attr("string"),
  startTime: DS.attr("string"),
  elapsedTime: DS.attr("string"),
  finalStatus: DS.attr("string"),
  finishedTime: DS.attr("finishedTime"),
  progress: DS.attr("number"),
  diagnostics: DS.attr("string"),
  amContainerLogs: DS.attr("string"),
  amHostHttpAddress: DS.attr("string"),
  masterNodeId: DS.attr("string"),
  logAggregationStatus: DS.attr("string"),
  unmanagedApplication: DS.attr("boolean"),
  amNodeLabelExpression: DS.attr("string"),
  applicationTags: DS.attr("string"),
  applicationType: DS.attr("string"),
  priority: DS.attr("string"),
  allocatedMB: DS.attr("number"),
  allocatedVCores: DS.attr("number"),
  runningContainers: DS.attr("number"),
  memorySeconds: DS.attr("number"),
  vcoreSeconds: DS.attr("number"),
  preemptedResourceMB: DS.attr("number"),
  preemptedResourceVCores: DS.attr("number"),
  numNonAMContainerPreempted: DS.attr("number"),
  numAMContainerPreempted: DS.attr("number"),
  clusterUsagePercentage: DS.attr("number"),
  queueUsagePercentage: DS.attr("number"),
  currentAppAttemptId: DS.attr("string"),
  remainingTimeoutInSeconds: DS.attr("number"),
  applicationExpiryTime: DS.attr("string"),
  resourceRequests: DS.attr("array"),

  isFailed: function() {
    return this.get("finalStatus") === "FAILED";
  }.property("finalStatus"),

  validatedFinishedTs: function() {
    if (this.get("finishedTime") < this.get("startTime")) {
      return "N/A";
    }
    return this.get("finishedTime");
  }.property("finishedTime"),

  hasFinishedTime: function() {
    return this.get("finishedTime") >= this.get("startTime");
  }.property("hasFinishedTime"),

  formattedElapsedTime: function() {
    return Converter.msToElapsedTimeUnit(this.get("elapsedTime"));
  }.property("elapsedTime"),

  allocatedResource: function() {
    return Converter.resourceToString(
      this.get("allocatedMB"),
      this.get("allocatedVCores")
    );
  }.property("allocatedMB", "allocatedVCores"),

  preemptedResource: function() {
    return Converter.resourceToString(
      this.get("preemptedResourceMB"),
      this.get("preemptedResourceVCores")
    );
  }.property("preemptedResourceMB", "preemptedResourceVCores"),

  aggregatedResourceUsage: function() {
    return (
      Converter.resourceToString(
        this.get("memorySeconds"),
        this.get("vcoreSeconds")
      ) + " (× Secs)"
    );
  }.property("memorySeconds", "vcoreSeconds"),

  masterNodeURL: function() {
    return `#/yarn-node/${this.get("masterNodeId")}/${this.get("amHostHttpAddress")}/info/`;
  }.property("masterNodeId", "amHostHttpAddress"),

  progressStyle: function() {
    return "width: " + this.get("progress") + "%";
  }.property("progress"),

  runningContainersNumber: function() {
    if (this.get("runningContainers") < 0) {
      return 0;
    }
    return this.get("runningContainers");
  }.property("progress"),

  finalStatusStyle: function() {
    var finalStatus = this.get("finalStatus");
    var style = "";

    if (finalStatus === "KILLED") {
      style = "warning";
    } else if (finalStatus === "FAILED") {
      style = "danger";
    } else if (finalStatus === "SUCCEEDED") {
      style = "success";
    } else {
      style = "default";
    }

    return "label label-" + style;
  }.property("finalStatus"),

  logAggregationStatusStyle: function() {
    const logAggregationStatus = this.get("logAggregationStatus");
    var style = "";
    if (logAggregationStatus === "FAILED" ||
        logAggregationStatus === "TIME_OUT") {
      style = "danger";
    } else if (logAggregationStatus === "RUNNING_WITH_FAILURE") {
      style = "warning";
    } else if (logAggregationStatus === "SUCCEEDED") {
      style = "success";
    } else {
      style = "default";
    }
    return "label label-" + style;
  }.property("logAggregationStatus")
});
