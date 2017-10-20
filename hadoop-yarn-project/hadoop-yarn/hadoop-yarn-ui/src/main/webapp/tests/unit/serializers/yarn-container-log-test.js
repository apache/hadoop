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

moduleFor('serializer:yarn-container-log', 'Unit | Serializer | ContainerLog', {
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-container-log"
  },
  payload = "This is syslog";
  var id = "localhost:64318!container_e32_1456000363780_0002_01_000001!syslog";
  assert.expect(6);
  var response =
      serializer.normalizeSingleResponse({}, modelClass, payload, id, null);
  assert.ok(response.data);
  assert.equal(response.data.id, id);
  assert.equal(response.data.type, modelClass.modelName);
  assert.equal(response.data.attributes.logs, payload);
  assert.equal(response.data.attributes.containerID,
      "container_e32_1456000363780_0002_01_000001");
  assert.equal(response.data.attributes.logFileName, "syslog");
});

