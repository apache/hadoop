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

moduleFor('route:yarn-nodes', 'Unit | Route | Nodes', {
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
});

test('Test getting nodes', function(assert) {
  var response = [{
      rack: "/default-rack", state: "RUNNING", id: "192.168.1.1:64318",
      nodeHostName: "192.168.1.1", nodeHTTPAddress: "192.168.1.1:8042",
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
      }},
      {rack: "/default-rack", state: "RUNNING", id: "192.168.1.2:64318",
      nodeHostName: "192.168.1.2", nodeHTTPAddress: "192.168.1.2:8042",
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
      }}];
  var store = {
    findAll: function(type) {
      return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
    }
  };
  var route = this.subject();
  route.set('store', store);
  var model = route.model()._result;
  assert.expect(4);
  assert.ok(model);
  assert.equal(model.length, 2);
  assert.deepEqual(response[0], model[0]);
  assert.deepEqual(response[1], model[1]);
});
