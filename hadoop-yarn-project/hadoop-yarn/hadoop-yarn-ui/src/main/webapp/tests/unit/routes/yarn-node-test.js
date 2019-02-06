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

import { moduleFor, test } from 'ember-qunit';
import Ember from 'ember';

moduleFor('route:yarn-node', 'Unit | Route | Node', {
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
});

test('Test getting a node', function(assert) {
  var nodeResponse =
      {healthReport: "Healthy", totalVmemAllocatedContainersMB: 344064,
      totalPmemAllocatedContainersMB: 163840,
      totalVCoresAllocatedContainers: 160,
      vmemCheckEnabled: true, pmemCheckEnabled: true,
      lastNodeUpdateTime: 1456250210310, nodeHealthy: true,
      nodeManagerVersion: "3.0.0-SNAPSHOT",
      nodeManagerBuildVersion: "3.0.0-SNAPSHOT",
      nodeManagerVersionBuiltOn: "2000-01-01T00:00Z",
      hadoopVersion: "3.0.0-SNAPSHOT",
      hadoopBuildVersion: "3.0.0-SNAPSHOT",
      hadoopVersionBuiltOn: "2000-01-01T00:00Z",
      id: "localhost:64318", nodeHostName: "192.168.0.102",
      nmStartupTime: 1456250208231};
  var rmNodeResponse =
      {rack: "/default-rack", state: "RUNNING", id: "localhost:64318",
      nodeHostName: "localhost", nodeHTTPAddress: "localhost:8042",
      lastHealthUpdate: 1456251290905, version: "3.0.0-SNAPSHOT",
      healthReport: "", numContainers: 0, usedMemoryMB: 0,
      availMemoryMB: 163840, usedVirtualCores: 0,
      availableVirtualCores: 160,
      resourceUtilization: {
      nodePhysicalMemoryMB: 4549, nodeVirtualMemoryMB: 4549,
      nodeCPUUsage: 0.14995001256465912,
      aggregatedContainersPhysicalMemoryMB: 0,
      aggregatedContainersVirtualMemoryMB: 0,
      containersCPUUsage: 0
      }};

  // Create store which returns appropriate responses.
  var store = {
    findRecord: function(type) {
      if (type === 'yarn-node') {
        return new Ember.RSVP.Promise(function(resolve) {
          resolve(nodeResponse);
        });
      } else if (type === 'yarn-rm-node') {
        return new Ember.RSVP.Promise(function(resolve) {
          resolve(rmNodeResponse);
        });
      }
    }
  };
  var route = this.subject();
  assert.expect(4);
  route.set('store', store);
  route.model({
    node_addr:"localhost:8042",
    node_id:"localhost:64318"
  }).then(function(model) {
    assert.ok(model.node);
    assert.deepEqual(model.node, nodeResponse);
    assert.ok(model.rmNode);
    assert.deepEqual(model.rmNode, rmNodeResponse);
  });
});
