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
  appsSubmitted: DS.attr('number'),
  appsCompleted: DS.attr('number'),
  appsPending: DS.attr('number'),
  appsRunning: DS.attr('number'),
  appsFailed: DS.attr('number'),
  appsKilled: DS.attr('number'),
  reservedMB: DS.attr('number'),
  availableMB: DS.attr('number'),
  allocatedMB: DS.attr('number'),
  reservedVirtualCores: DS.attr('number'),
  availableVirtualCores: DS.attr('number'),
  allocatedVirtualCores: DS.attr('number'),
  containersAllocated: DS.attr('number'),
  containersReserved: DS.attr('number'),
  containersPending: DS.attr('number'),
  totalMB: DS.attr('number'),
  totalVirtualCores: DS.attr('number'),
  totalNodes: DS.attr('number'),
  lostNodes: DS.attr('number'),
  unhealthyNodes: DS.attr('number'),
  decommissioningNodes: DS.attr('number'),
  decommissionedNodes: DS.attr('number'),
  rebootedNodes: DS.attr('number'),
  activeNodes: DS.attr('number'),
  totalUsedResourcesAcrossPartition: DS.attr('object'),
  totalClusterResourcesAcrossPartition: DS.attr('object'),

  getFinishedAppsDataForDonutChart: function() {
    var arr = [];
    arr.push({
      label: "Completed",
      value: this.get("appsCompleted")
    });
    arr.push({
      label: "Killed",
      value: this.get("appsKilled")
    });
    arr.push({
      label: "Failed",
      value: this.get("appsFailed")
    });

    return arr;
  }.property("appsCompleted", "appsKilled", "appsFailed"),

  getRunningAppsDataForDonutChart: function() {
    var arr = [];

    arr.push({
      label: "Pending",
      value: this.get("appsPending")
    });
    arr.push({
      label: "Running",
      value: this.get("appsRunning")
    });

    return arr;
  }.property("appsPending", "appsRunning"),

  getNodesDataForDonutChart: function() {
    var arr = [];
    arr.push({
      label: "Active",
      value: this.get("activeNodes")
    });
    arr.push({
      label: "Unhealthy",
      value: this.get("unhealthyNodes")
    });
    arr.push({
      label: "Decommissioning",
      value: this.get("decommissioningNodes")
    });
    arr.push({
      label: "Decomissioned",
      value: this.get("decommissionedNodes")
    });
    return arr;
  }.property("activeNodes", "unhealthyNodes", "decommissioningNodes", "decommissionedNodes"),

  getMemoryDataForDonutChart: function() {
    var type = "MB";
    var arr = [];
    arr.push({
      label: "Allocated",
      value: this.get("allocated" + type)
    });
    arr.push({
      label: "Reserved",
      value: this.get("reserved" + type)
    });
    arr.push({
      label: "Available",
      value: this.get("available" + type)
    });

    return arr;
  }.property("allocatedMB", "reservedMB", "availableMB"),

  getVCoreDataForDonutChart: function() {
    var type = "VirtualCores";
    var arr = [];
    arr.push({
      label: "Allocated",
      value: this.get("allocated" + type)
    });
    arr.push({
      label: "Reserved",
      value: this.get("reserved" + type)
    });
    arr.push({
      label: "Available",
      value: Math.max(this.get("available" + type), 0)
    });

    return arr;
  }.property("allocatedVirtualCores", "reservedVirtualCores", "availableVirtualCores"),

  getResourceTypes: function() {
    var types = [];
    if (this.get("totalClusterResourcesAcrossPartition")) {

      console.log(types);
    }
  }.property("totalClusterResourcesAcrossPartition"),

  /*
   * Returned format
   * [
   *     {
   *         name: <resource-name>
   *         unit: <resource-unit>
   *         [
   *            {
   *               label: <label>
   *               value: <value>
   *            },
   *            {
   *            }
   *            ...
   *         ],
   *     }
   * ]
   */
  getAllResourceTypesDonutChart: function() {
    if (this.get("totalClusterResourcesAcrossPartition")
      && this.get("totalUsedResourcesAcrossPartition")) {
      var usages = [];

      var clusterResourceInformations = this.get("totalClusterResourcesAcrossPartition").resourcesInformations;
      var usedResourceInformations = this.get("totalUsedResourcesAcrossPartition").resourcesInformations;

      clusterResourceInformations.forEach(function(cluster) {
        var perResourceTypeUsage = {
          name: cluster.name,
          unit: cluster.units,
          data: []
        };

        usedResourceInformations.forEach(function (used) {
          if (used.name === perResourceTypeUsage.name) {
            var usedValue = used.value;
            perResourceTypeUsage.data.push({
              label: "Used",
              value: usedValue
            }, {
              label: "Available",
              value: cluster.value - usedValue
            });
          }
        });

        usages.push(perResourceTypeUsage);

        // Make sure id is a valid w3c ID
        perResourceTypeUsage.id = perResourceTypeUsage.name.replace('/', '-');
        perResourceTypeUsage.id = perResourceTypeUsage.id.replace('.', '-');
      });

      console.log(usages);
      return usages;
    }
    return null;
  }.property()
});
