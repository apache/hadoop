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

import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  startTime: DS.attr('string'),
  finishedTime: DS.attr('string'),
  containerId: DS.attr('string'),
  nodeHttpAddress: DS.attr('string'),
  nodeId: DS.attr('string'),
  logsLink: DS.attr('string'),

  startTs: function() {
    return Converter.dateToTimeStamp(this.get("startTime"));
  }.property("startTime"),

  finishedTs: function() {
    var ts = Converter.dateToTimeStamp(this.get("finishedTime"));
    return ts;
  }.property("finishedTime"),

  shortAppAttemptId: function() {
    return "attempt_" + 
           parseInt(Converter.containerIdToAttemptId(this.get("containerId")).split("_")[3]);
  }.property("containerId"),

  elapsedTime: function() {
    var elapsedMs = this.get("finishedTs") - this.get("startTs");
    if (elapsedMs <= 0) {
      elapsedMs = Date.now() - this.get("startTs");
    }

    return Converter.msToElapsedTime(elapsedMs);
  }.property(),

  tooltipLabel: function() {
    return "<p>Id:" + this.get("id") + 
           "</p><p>ElapsedTime:" + 
           String(this.get("elapsedTime")) + "</p>";
  }.property(),

  link: function() {
    return "/yarnAppAttempt/" + this.get("id");
  }.property(),
});