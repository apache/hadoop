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

import { moduleForModel, test } from 'ember-qunit';

moduleForModel('yarn-rm-node', 'Unit | Model | RMNode', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model._notifyProperties);
  assert.ok(model.didLoad);
  assert.ok(model.rack);
  assert.ok(model.state);
  assert.ok(model.nodeHostName);
  assert.ok(model.nodeHTTPAddress);
  assert.ok(model.lastHealthUpdate);
  assert.ok(model.healthReport);
  assert.ok(model.numContainers);
  assert.ok(model.usedMemoryMB);
  assert.ok(model.availMemoryMB);
  assert.ok(model.usedVirtualCores);
  assert.ok(model.availableVirtualCores);
  assert.ok(model.version);
  assert.ok(model.nodeLabels);
  assert.ok(model.nodeLabelsAsString);
  assert.ok(model.nodeStateStyle);
  assert.ok(model.isDummyNode);
  assert.ok(model.getMemoryDataForDonutChart);
  assert.ok(model.getVCoreDataForDonutChart);
});

test('test fields', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("rack", "/default-rack");
    model.set("state", "RUNNING");
    model.set("nodeHostName", "localhost");
    model.set("id", "localhost:64318");
    model.set("nodeHTTPAddress", "localhost:8042");
    model.set("usedMemoryMB", 1024);
    model.set("availMemoryMB", 7168);
    model.set("usedVirtualCores", 1);
    model.set("availableVirtualCores", 7);
    model.set("nodeLabels", ["x"]);
    assert.equal(model.get("rack"), "/default-rack");
    assert.equal(model.get("state"), "RUNNING");
    assert.equal(model.get("nodeHostName"), "localhost");
    assert.equal(model.get("id"), "localhost:64318");
    assert.equal(model.get("nodeHTTPAddress"), "localhost:8042");
    assert.equal(model.get("usedMemoryMB"), 1024);
    assert.equal(model.get("availMemoryMB"), 7168);
    assert.equal(model.get("usedVirtualCores"), 1);
    assert.equal(model.get("availableVirtualCores"), 7);
    assert.equal(model.get("isDummyNode"), false);
    assert.deepEqual(model.get("nodeLabels"), ["x"]);
    assert.equal(model.get("nodeLabelsAsString"), "x");
    assert.deepEqual(model.get("nodeStateStyle"), "label label-success");
    assert.deepEqual(model.get("getMemoryDataForDonutChart"),
        [{label: "Used", value: 1024}, {label: "Available", value: 7168}]);
    assert.deepEqual(model.get("getVCoreDataForDonutChart"),
        [{label: "Used", value: 1}, {label: "Available", value: 7}]);
    model.set("state", "SHUTDOWN");
    assert.deepEqual(model.get("nodeStateStyle"), "label label-danger");
    model.set("state", "REBOOTED");
    assert.deepEqual(model.get("nodeStateStyle"), "label label-warning");
    model.set("state", "NEW");
    assert.deepEqual(model.get("nodeStateStyle"), "label label-default");
    model.set("nodeLabels", ["x","y"]);
    assert.equal(model.get("nodeLabelsAsString"), "x");
    model.set("nodeLabels", undefined);
    assert.equal(model.get("nodeLabelsAsString"), "");
  });
});

