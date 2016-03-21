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
import Constants from 'yarn-ui/constants';

moduleFor('adapter:yarn-container-log', 'Unit | Adapter | ContainerLog', {
});

test('Basic creation', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.ok(adapter.urlForFindRecord);
  assert.ok(adapter.ajax);
  assert.ok(adapter.headers);
  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.equal(adapter.headers.Accept, "text/plain");
  assert.equal(adapter.namespace, "ws/v1/node");
});

test('urlForFindRecord test', function(assert) {
  let adapter = this.subject();
  let host = adapter.host;
  assert.equal(adapter.urlForFindRecord("localhost:8042" +
      Constants.PARAM_SEPARATOR + "container_e27_11111111111_0001_01_000001" +
      Constants.PARAM_SEPARATOR + "syslog"),
      host + "localhost:8042/ws/v1/node/containerlogs/" +
      "container_e27_11111111111_0001_01_000001/syslog");
});

test('ajaxOptions test', function(assert) {
  let adapter = this.subject();
  var hash = adapter.ajaxOptions('/containerlogs', 'type', {});
  assert.equal(hash.dataType, 'text');
});

test('findRecord test', function(assert) {
  let adapter = this.subject(),
      testModel = { modelName: "testModel" },
      testStore = {},
      testSnapshot = {};
  let host = adapter.host;
  let testId = "localhost:8042" + Constants.PARAM_SEPARATOR +
      "container_e27_11111111111_0001_01_000001" + Constants.PARAM_SEPARATOR +
      "syslog";
  assert.expect(2);

  adapter.ajax = function (url, method) {
    assert.equal(url, host + "localhost:8042/ws/v1/node/containerlogs/" +
        "container_e27_11111111111_0001_01_000001/syslog");
    assert.equal(method, 'GET');
  };

  adapter.findRecord(testStore, testModel, testId, testSnapshot);
});

