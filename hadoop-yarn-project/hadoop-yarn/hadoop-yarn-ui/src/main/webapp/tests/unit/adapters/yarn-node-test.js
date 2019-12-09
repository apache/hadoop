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

moduleFor('adapter:yarn-node', 'Unit | Adapter | Node', {
  unit: true
});

test('Basic creation', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });

  assert.ok(adapter);
  assert.ok(adapter.urlForFindRecord);
  assert.ok(adapter.ajax);
  assert.ok(adapter.headers);
  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.equal(adapter.headers.Accept, "application/json");
  assert.equal(adapter.namespace, "{nodeAddress}/ws/v1/node");
});

test('urlForFindRecord test', function(assert) {
  let adapter = this.subject({
    host: "localhost:4200",
    namespace: "{nodeAddress}/ws/v1/node"
  });
  let host = adapter.host;
  assert.equal(adapter.urlForFindRecord("localhost:8042"),
      host + "/localhost:8042/ws/v1/node");
});
