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

export default DS.Model.extend({
  name: DS.attr('string'),
  children: DS.attr('array'),
  parent: DS.attr('string'),
  maxApps: DS.attr('number'),
  minResources: DS.attr(),
  maxResources: DS.attr(),
  usedResources: DS.attr(),
  demandResources: DS.attr(),
  steadyFairResources: DS.attr(),
  fairResources: DS.attr(),
  clusterResources: DS.attr(),
  pendingContainers: DS.attr('number'),
  allocatedContainers: DS.attr('number'),
  reservedContainers: DS.attr('number'),
  schedulingPolicy: DS.attr('string'),
  preemptable: DS.attr('number'),
  numPendingApplications: DS.attr('number'),
  numActiveApplications: DS.attr('number'),
  type: DS.attr('string'),

  isLeafQueue: function() {
    var len = this.get("children.length");
    if (!len) {
      return true;
    }
    return len <= 0;
  }.property("children"),

  capacitiesBarChartData: function() {
    return [
      {
        label: "Steady Fair Memory",
        value: this.get("steadyFairResources.memory")
      },
      {
        label: "Used Memory",
        value: this.get("usedResources.memory")
      },
      {
        label: "Maximum Memory",
        value: this.get("maxResources.memory")
      }
    ];
  }.property("maxResources.memory", "usedResources.memory", "maxResources.memory"),

  numOfApplicationsDonutChartData: function() {
    return [
      {
        label: "Pending Apps",
        value: this.get("numPendingApplications") || 0 // TODO, fix the REST API so root will return #applications as well.
      },
      {
        label: "Active Apps",
        value: this.get("numActiveApplications") || 0
      }
    ];
  }.property()
});
