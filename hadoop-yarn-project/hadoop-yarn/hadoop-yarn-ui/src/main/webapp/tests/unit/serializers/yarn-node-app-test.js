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

moduleFor('serializer:yarn-node-app', 'Unit | Serializer | NodeApp', {
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
    modelName: "yarn-node-app"
  },
  payload = {
    apps: {
      app: [{
        id:"application_1456251210105_0001", state:"FINISHED", user:"root"
      },{
        id:"application_1456251210105_0002", state:"RUNNING",user:"root",
        containerids:["container_e38_1456251210105_0002_01_000001",
        "container_e38_1456251210105_0002_01_000002"]
      }]
    }
  };
  assert.expect(15);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload, null, null);
  assert.ok(response.data);
  assert.equal(response.data.length, 2);
  assert.equal(response.data[0].attributes.containers, undefined);
  assert.equal(response.data[1].attributes.containers.length, 2);
  assert.deepEqual(response.data[1].attributes.containers,
      payload.apps.app[1].containerids);
  for (var i = 0; i < 2; i++) {
    assert.equal(response.data[i].type, modelClass.modelName);
    assert.equal(response.data[i].id, payload.apps.app[i].id);
    assert.equal(response.data[i].attributes.appId, payload.apps.app[i].id);
    assert.equal(response.data[i].attributes.state, payload.apps.app[i].state);
    assert.equal(response.data[i].attributes.user, payload.apps.app[i].user);
  }
});

test('normalizeArrayResponse no apps test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-node-app"
  },
  payload = { apps: null };
  assert.expect(5);
  var response =
      serializer.normalizeArrayResponse({}, modelClass, payload, null, null);
  assert.ok(response.data);
  assert.equal(response.data.length, 1);
  assert.equal(response.data[0].type, modelClass.modelName);
  assert.equal(response.data[0].id, "dummy");
  assert.equal(response.data[0].attributes.appId, undefined);
});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-node-app"
  },
  payload = {
    app: {id:"application_1456251210105_0001", state:"FINISHED", user:"root"}
  };
  assert.expect(7);
  var response =
      serializer.normalizeSingleResponse({}, modelClass, payload, null, null);
  assert.ok(response.data);
  assert.equal(payload.app.id, response.data.id);
  assert.equal(modelClass.modelName, response.data.type);
  assert.equal(payload.app.id, response.data.attributes.appId);
  assert.equal(payload.app.state, response.data.attributes.state);
  assert.equal(payload.app.user, response.data.attributes.user);
  assert.equal(response.data.attributes.containers, undefined);
});

