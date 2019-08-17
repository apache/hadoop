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

moduleFor('serializer:yarn-node-container', 'Unit | Serializer | NodeContainer', {
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
    modelName: "yarn-node-container"
  },
  payload = {
    containers: {
      container: [{
        id: "container_e32_1456000363780_0002_01_000001", state: "RUNNING",
        exitCode:-1000,diagnostics:"",user:"root",totalMemoryNeededMB:2048,
        totalVCoresNeeded:1,containerLogsLink: "http://localhost:8042/node/" +
        "containerlogs/container_e32_1456000363780_0002_01_000001/root",
        nodeId: "localhost:64318", containerLogFiles:["syslog","stderr",
        "stdout"]
      },{
        id:"container_e32_1456000363780_0002_01_000003", state:"RUNNING",
        exitCode:-1000, diagnostics:"", user:"root", totalMemoryNeededMB:1024,
        totalVCoresNeeded:1,containerLogsLink:"http://localhost:8042/node" +
        "/containerlogs/container_e32_1456000363780_0002_01_000003/root",
        nodeId:"localhost:64318",containerLogFiles:["syslog","stderr",
        "syslog.shuffle","stdout"]
      }]
    }
  };
  assert.expect(14);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload);
  assert.ok(response.data);
  assert.equal(response.data.length, 2);
  assert.equal(response.data[0].id,
      "container_e32_1456000363780_0002_01_000001");
  assert.equal(response.data[1].id,
      "container_e32_1456000363780_0002_01_000003");
  assert.equal(response.data[0].attributes.containerLogFiles.length, 3);
  assert.equal(response.data[1].attributes.containerLogFiles.length, 4);
  for (var i = 0; i < 2; i++) {
    assert.equal(response.data[i].type, modelClass.modelName);
    assert.deepEqual(response.data[i].attributes.containerLogFiles,
        payload.containers.container[i].containerLogFiles);
    assert.equal(response.data[i].attributes.state,
        payload.containers.container[i].state);
    assert.equal(response.data[i].attributes.user,
        payload.containers.container[i].user);
  }
});

test('normalizeArrayResponse no containers test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-node-container"
  },
  payload = { containers: null };
  assert.expect(2);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload);
  assert.ok(response.data);
  assert.equal(response.data.length, 0);
});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-node-container"
  },
  payload = {
    container: {
      id: "container_e32_1456000363780_0002_01_000001", state: "RUNNING",
      exitCode:-1000,diagnostics:"",user:"root",totalMemoryNeededMB:2048,
      totalVCoresNeeded:1,containerLogsLink: "http://localhost:8042/node/" +
      "containerlogs/container_e32_1456000363780_0002_01_000001/root",
      nodeId: "localhost:64318", containerLogFiles:["syslog","stderr",
      "stdout"]
    }
  };
  assert.expect(11);
  var response =
      serializer.normalizeSingleResponse({}, modelClass, payload);
  assert.ok(response.data);
  assert.equal(response.data.id, payload.container.id);
  assert.equal(response.data.type, modelClass.modelName);
  assert.equal(response.data.attributes.containerId, payload.container.id);
  assert.equal(response.data.attributes.state, payload.container.state);
  assert.equal(response.data.attributes.user, payload.container.user);
  assert.equal(response.data.attributes.exitCode, payload.container.exitCode);
  assert.equal(response.data.attributes.totalMemoryNeededMB,
      payload.container.totalMemoryNeeded);
  assert.equal(response.data.attributes.totalVCoresNeeded,
      payload.container.totalVCoresNeeded);
  assert.equal(response.data.attributes.containerLogFiles.length, 3);
  assert.deepEqual(response.data.attributes.containerLogFiles,
      payload.container.containerLogFiles);
});

