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
  name: DS.attr('string'),
  children: DS.attr('array'),
  parent: DS.attr('string'),
  capacity: DS.attr('number'),
  maxCapacity: DS.attr('number'),
  usedCapacity: DS.attr('number'),
  absCapacity: DS.attr('number'),
  absMaxCapacity: DS.attr('number'),
  absUsedCapacity: DS.attr('number'),
  state: DS.attr('string'),
  userLimit: DS.attr('number'),
  userLimitFactor: DS.attr('number'),
  preemptionDisabled: DS.attr('number'),
  numPendingApplications: DS.attr('number'),
  numActiveApplications: DS.attr('number'),
  users: DS.hasMany('YarnUser'),
  type: DS.attr('string'),
  resources: DS.attr('object'),

  isLeafQueue: function() {
    var len = this.get("children.length");
    if (!len) {
      return true;
    }
    return len <= 0;
  }.property("children"),

  capacitiesBarChartData: function() {
    var floatToFixed = Converter.floatToFixed;
    return [
      {
        label: "Absolute Capacity",
        value: this.get("name") === "root" ? 100 : floatToFixed(this.get("absCapacity"))
      },
      {
        label: "Absolute Used",
        value: this.get("name") === "root" ? floatToFixed(this.get("usedCapacity")) : floatToFixed(this.get("absUsedCapacity"))
      },
      {
        label: "Absolute Max Capacity",
        value: this.get("name") === "root" ? 100 : floatToFixed(this.get("absMaxCapacity"))
      }
    ];
  }.property("absCapacity", "usedCapacity", "absMaxCapacity"),

  userUsagesDonutChartData: function() {
    var data = [];
    if (this.get("users")) {
      this.get("users").forEach(function(o) {
        data.push({
          label: o.get("name"),
          value: o.get("usedMemoryMB")
        });
      });
    }

    return data;
  }.property("users"),

  hasUserUsages: function() {
    return this.get("userUsagesDonutChartData").length > 0;
  }.property(),

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
  }.property("numPendingApplications", "numActiveApplications"),
});
