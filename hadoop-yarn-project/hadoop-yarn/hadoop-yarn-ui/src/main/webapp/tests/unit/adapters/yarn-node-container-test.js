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

moduleFor('adapter:yarn-node-container', 'Unit | Adapter | NodeContainer', {
  unit: true
});

test('Basic creation', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  assert.expect(11);
  assert.ok(adapter);
  assert.ok(adapter.urlForQueryRecord);
  assert.ok(adapter.queryRecord);
  assert.ok(adapter.urlForQuery);
  assert.ok(adapter.query);
  assert.ok(adapter.ajax);
  assert.ok(adapter.headers);
  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.equal("application/json", adapter.headers.Accept);
  assert.equal("{nodeAddress}/ws/v1/node", adapter.namespace);
});

test('urlForQueryRecord test', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  let host = adapter.host;
  assert.equal(host + "/localhost:8042/ws/v1/node/containers/" +
      "container_e27_11111111111_0001_01_000001",
      adapter.urlForQueryRecord(
      {nodeHttpAddr: "localhost:8042",
      containerId: "container_e27_11111111111_0001_01_000001"}));
});

test('urlForQuery test', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  let host = adapter.host;
  assert.equal(host + "/localhost:8042/ws/v1/node/containers",
      adapter.urlForQuery({nodeHttpAddr: "localhost:8042"}));
});

test('query test', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  let testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {nodeHttpAddr: "localhost:8042"};
  let host = adapter.host;
  assert.expect(3);

  adapter.ajax = function (url, method, hash) {
    assert.equal(host + "/localhost:8042/ws/v1/node/containers", url);
    assert.equal('GET', method);
    assert.equal(null, hash.data);
  };

  adapter.query(testStore, testModel, testQuery);
});

test('queryRecord test', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  let testModel = { modelName: "testModel" },
      testStore = {},
      testQuery = {
        nodeHttpAddr: "localhost:8042",
        containerId: "container_e27_11111111111_0001_01_000001"
      };
  let host = adapter.host;
  assert.expect(3);

  adapter.ajax = function (url, method, hash) {
    assert.equal(host + "/localhost:8042/ws/v1/node/containers/" +
        "container_e27_11111111111_0001_01_000001", url);
    assert.equal('GET', method);
    assert.equal(null, hash.data);
  };

  adapter.queryRecord(testStore, testModel, testQuery);
});
