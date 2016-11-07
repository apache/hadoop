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

moduleFor('serializer:yarn-rm-node', 'Unit | Serializer | RMNode', {
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
  assert.ok(serializer.internalNormalizeSingleResponse);
});

test('normalizeArrayResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-rm-node"
  },
  payload = {
    nodes: {
      node: [{
        rack: "/default-rack", state: "RUNNING", id: "192.168.1.1:64318",
        nodeHostName: "192.168.1.1", nodeHTTPAddress: "192.168.1.1:8042",
        lastHealthUpdate: 1456251290905, version: "3.0.0-SNAPSHOT",
        healthReport: "", numContainers: 0, usedMemoryMB: 2048,
        availMemoryMB: 161792, usedVirtualCores: 2,
        availableVirtualCores: 158, nodeLabels: ["x"],
        resourceUtilization: {
          nodePhysicalMemoryMB: 4549, nodeVirtualMemoryMB: 4549,
          nodeCPUUsage: 0.14995001256465912,
          aggregatedContainersPhysicalMemoryMB: 0,
          aggregatedContainersVirtualMemoryMB: 0,
          containersCPUUsage: 0
        }
      },{
        rack: "/default-rack", state: "RUNNING", id: "192.168.1.2:64318",
        nodeHostName: "192.168.1.2", nodeHTTPAddress: "192.168.1.2:8042",
        lastHealthUpdate: 1456251290905, version: "3.0.0-SNAPSHOT",
        healthReport: "", numContainers: 0, usedMemoryMB: 0,
        availMemoryMB: 163840, usedVirtualCores: 0,
        availableVirtualCores: 160, nodeLabels: ["y"],
        resourceUtilization: {
          nodePhysicalMemoryMB: 4549, nodeVirtualMemoryMB: 4549,
          nodeCPUUsage: 0.14995001256465912,
          aggregatedContainersPhysicalMemoryMB: 0,
          aggregatedContainersVirtualMemoryMB: 0,
          containersCPUUsage: 0
        }
      }]
    }
  };
  assert.expect(12);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload, null, null);
  assert.ok(response.data);
  assert.equal(response.data.length, 2);
  assert.equal(response.data[0].id, "192.168.1.1:64318");
  assert.equal(response.data[1].id, "192.168.1.2:64318");
  for (var i = 0; i < 2; i++) {
    assert.equal(response.data[i].type, modelClass.modelName);
    assert.equal(response.data[i].attributes.nodeHostName,
        payload.nodes.node[i].nodeHostName);
    assert.equal(response.data[i].attributes.nodeHTTPAddress,
        payload.nodes.node[i].nodeHTTPAddress);
    assert.deepEqual(response.data[i].attributes.nodeLabels,
        payload.nodes.node[i].nodeLabels);
  }
});

test('normalizeArrayResponse no nodes test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-rm-node"
  },
  payload = { nodes: null };
  assert.expect(5);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload, null, null);
  console.log(response);
  assert.ok(response.data);
  assert.equal(response.data.length, 1);
  assert.equal(response.data[0].type, modelClass.modelName);
  assert.equal(response.data[0].id, "dummy");
  assert.equal(response.data[0].attributes.nodeHostName, undefined);
});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-rm-node"
  },
  payload = {
    node: {
      rack: "/default-rack", state: "RUNNING", id: "192.168.1.1:64318",
      nodeHostName: "192.168.1.1", nodeHTTPAddress: "192.168.1.1:8042",
      lastHealthUpdate: 1456251290905, version: "3.0.0-SNAPSHOT",
      healthReport: "", numContainers: 0, usedMemoryMB: 2048,
      availMemoryMB: 161792, usedVirtualCores: 2,
      availableVirtualCores: 158, nodeLabels: ["x"],
      resourceUtilization: {
        nodePhysicalMemoryMB: 4549, nodeVirtualMemoryMB: 4549,
        nodeCPUUsage: 0.14995001256465912,
        aggregatedContainersPhysicalMemoryMB: 0,
        aggregatedContainersVirtualMemoryMB: 0,
        containersCPUUsage: 0
      }
    }
  };
  assert.expect(13);
  var id = "localhost:64318";
  var response =
      serializer.normalizeSingleResponse({}, modelClass, payload, id, null);
  assert.ok(response.data);
  assert.equal(response.data.id, id);
  assert.equal(response.data.type, modelClass.modelName);
  assert.equal(response.data.attributes.rack, payload.node.rack);
  assert.equal(response.data.attributes.state, payload.node.state);
  assert.equal(response.data.attributes.nodeHostName,
      payload.node.nodeHostName);
  assert.equal(response.data.attributes.nodeHTTPAddress,
      payload.node.nodeHTTPAddress);
  assert.equal(response.data.attributes.version, payload.node.version);
  assert.equal(response.data.attributes.availMemoryMB,
      payload.node.availMemoryMB);
  assert.equal(response.data.attributes.usedMemoryMB,
      payload.node.usedMemoryMB);
  assert.equal(response.data.attributes.availableVirtualCores,
      payload.node.availableVirtualCores);
  assert.equal(response.data.attributes.usedVirtualCores,
      payload.node.usedVirtualCores);
  assert.deepEqual(response.data.attributes.nodeLabels,
      payload.node.nodeLabels);
});

