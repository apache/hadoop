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

import Ember from 'ember';
import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  startTime: DS.attr('string'),
  startedTime: DS.attr('string'),
  finishedTime: DS.attr('string'),
  containerId: DS.attr('string'),
  amContainerId: DS.attr('string'),
  nodeHttpAddress: DS.attr('string'),
  exposedPorts: DS.attr('string'),
  nodeId: DS.attr('string'),
  hosts: DS.attr('string'),
  logsLink: DS.attr('string'),
  state: DS.attr('string'),
  appAttemptId: DS.attr('string'),
  diagnosticsInfo: DS.attr('string'),

  appId: Ember.computed("id",function () {
    var id = this.get("id");
    id = id.split("_");

    id[0] = "application";
    id.pop();

    return id.join("_");
  }),

  attemptStartedTime: function() {
    var startTime = this.get("startTime");
    // If startTime variable is not present, get from startedTime
    if (startTime === undefined ||
      startTime === "Invalid date") {
      startTime = this.get("startedTime");
    }

    return startTime;
  }.property("startedTime"),

  startTs: function() {
    return Converter.dateToTimeStamp(this.get('attemptStartedTime'));
  }.property("startTime"),

  finishedTs: function() {
    var ts = Converter.dateToTimeStamp(this.get("finishedTime"));
    return ts;
  }.property("finishedTime"),

  validatedFinishedTs: function() {
    if (this.get("finishedTs") < this.get("startTs")) {
      return "";
    }
    return this.get("finishedTime");
  }.property("finishedTime"),

  shortAppAttemptId: function() {
    if (!this.get("containerId")) {
      return this.get("id");
    }
    return "attempt_" +
           parseInt(Converter.containerIdToAttemptId(this.get("containerId")).split("_")[3]);
  }.property("containerId"),

  appMasterContainerId: function() {
    var id = this.get("containerId");
    // If containerId variable is not present, get from amContainerId
    if (id === undefined) {
      id = this.get("amContainerId");
    }
    return id;
  }.property("amContainerId"),

  IsAmNodeUrl: function() {
    var url = this.get("nodeHttpAddress");
      // If nodeHttpAddress variable is not present, hardcode it.
    if (url === undefined) {
      url = "Not Available";
    }
    return url !== "Not Available";
  }.property("nodeHttpAddress"),

  amNodeId : function() {
    var id = this.get("nodeId");
    // If nodeId variable is not present, get from host
    if (id === undefined) {
      id = this.get("hosts");
    }
    return id;
  }.property("nodeId"),

  IsLinkAvailable: function() {
    var url = this.get("logsLink");
    // If logsLink variable is not present, hardcode its.
    if (url === undefined) {
      url = "Not Available";
    }
    return url !== "Not Available";
  }.property("logsLink"),

  elapsedTime: function() {
    var elapsedMs = this.get("finishedTs") - this.get("startTs");
    if (elapsedMs <= 0) {
      elapsedMs = Date.now() - this.get("startTs");
    }
    return Converter.msToElapsedTimeUnit(elapsedMs);
  }.property(),

  tooltipLabel: function() {
    return "<p>Id:" + this.get("id") +
           "</p><p>ElapsedTime:" +
           String(this.get("elapsedTime")) + "</p>";
  }.property(),

  link: function() {
    return "/yarn-app-attempt/" + this.get("id");
  }.property(),

  linkname: function() {
    return "yarn-app-attempt";
  }.property(),

  attemptState: function() {
    return this.get("state");
  }.property(),

  masterNodeURL: function() {
    var addr = encodeURIComponent(this.get("nodeHttpAddress"));
    return `#/yarn-node/${this.get("nodeId")}/${addr}/info/`;
  }.property("nodeId", "nodeHttpAddress"),

  appAttemptContainerLogsURL: function() {
    const attemptId = this.get("id");
    const containerId = this.get("appMasterContainerId");
    const appId = Converter.attemptIdToAppId(attemptId);
    return `#/yarn-app/${appId}/logs?attempt=${attemptId}&containerid=${containerId}`;
  }.property("id", "appMasterContainerId")
});
