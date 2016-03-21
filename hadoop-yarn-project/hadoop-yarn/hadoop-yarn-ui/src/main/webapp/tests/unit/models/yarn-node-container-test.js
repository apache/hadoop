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

moduleForModel('yarn-node-container', 'Unit | Model | NodeContainer', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model._notifyProperties);
  assert.ok(model.didLoad);
  assert.ok(model.containerId);
  assert.ok(model.state);
  assert.ok(model.user);
  assert.ok(model.exitCode);
  assert.ok(model.totalMemoryNeeded);
  assert.ok(model.totalVCoresNeeded);
  assert.ok(model.containerLogFiles);
  assert.ok(model.isDummyContainer);
  assert.ok(model.containerStateStyle);
});

test('test fields', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("containerId", "container_e32_1456000363780_0002_01_000003");
    model.set("state", "RUNNING");
    model.set("exitCode", "-1000");
    model.set("user", "hadoop");
    model.set("id", "container_e32_1456000363780_0002_01_000003");
    model.set("totalMemoryNeeded", 1024);
    model.set("totalVCoresNeeded", 1);
    model.set("containerLogFiles", ["syslog", "stderr", "stdout"]);
    assert.equal(model.get("containerId"), "container_e32_1456000363780_0002_01_000003");
    assert.equal(model.get("id"), "container_e32_1456000363780_0002_01_000003");
    assert.equal(model.get("totalMemoryNeeded"), 1024);
    assert.equal(model.get("totalVCoresNeeded"), 1);
    assert.equal(model.get("user"), "hadoop");
    assert.equal(model.get("exitCode"), "-1000");
    assert.equal(model.get("containerLogFiles").length, 3);
    assert.deepEqual(model.get("containerLogFiles"), ["syslog", "stderr", "stdout"]);
    assert.equal(model.get("isDummyContainer"), false);
    assert.equal(model.get("containerStateStyle"), "label label-primary");
    model.set("id", "dummy");
    assert.equal(model.get("isDummyContainer"), true);
    model.set("state", "EXITED_WITH_SUCCESS");
    assert.equal(model.get("containerStateStyle"), "label label-success");
    model.set("state", "EXITED_WITH_FAILURE");
    assert.equal(model.get("containerStateStyle"), "label label-danger");
    model.set("state", "DONE");
    model.set("exitCode", "0");
    assert.equal(model.get("containerStateStyle"), "label label-success");
    model.set("exitCode", "-105");
    assert.equal(model.get("containerStateStyle"), "label label-danger");
  });
});

