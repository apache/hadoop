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

moduleFor('route:yarn-container-log', 'Unit | Route | ContainerLog', {
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
});

test('Test getting container log', function(assert) {
  var response = {
      logs: "This is syslog",
      containerID: "container_e32_1456000363780_0002_01_000001",
      logFileName: "syslog"};
  var store = {
    findRecord: function() {
      return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
    }
  };
  assert.expect(6);
  var route = this.subject();
  route.set('store', store);
  var model = route.model({node_id: "localhost:64318",
      node_addr: "localhost:8042",
      container_id: "container_e32_1456000363780_0002_01_000001",
      filename: "syslog"});
   model.then(function(value) {
     assert.ok(value);
     assert.ok(value.containerLog);
     assert.deepEqual(value.containerLog, response);
     assert.ok(value.nodeInfo);
     assert.equal(value.nodeInfo.addr, 'localhost:8042');
     assert.equal(value.nodeInfo.id, 'localhost:64318');
   });
});

/**
 * This can happen when an empty response is sent from server
 */
test('Test non HTTP error while getting container log', function(assert) {
  var error = {};
  var response = {
      logs: "",
      containerID: "container_e32_1456000363780_0002_01_000001",
      logFileName: "syslog"};
  var store = {
    findRecord: function() {
      return new Ember.RSVP.Promise(function(resolve, reject) {
        reject(error);
      });
    }
  };
  assert.expect(6);
  var route = this.subject();
  route.set('store', store);
  var model = route.model({node_id: "localhost:64318",
      node_addr: "localhost:8042",
      container_id: "container_e32_1456000363780_0002_01_000001",
      filename: "syslog"});
   model.then(function(value) {
     assert.ok(value);
     assert.ok(value.containerLog);
     assert.deepEqual(value.containerLog, response);
     assert.ok(value.nodeInfo);
     assert.equal(value.nodeInfo.addr, 'localhost:8042');
     assert.equal(value.nodeInfo.id, 'localhost:64318');
   });
});

test('Test HTTP error while getting container log', function(assert) {
  var error = {errors: [{status: 404, responseText: 'Not Found'}]};
  var store = {
    findRecord: function() {
      return new Ember.RSVP.Promise(function(resolve, reject) {
        reject(error);
      });
    }
  };
  assert.expect(5);
  var route = this.subject();
  route.set('store', store);
  var model = route.model({node_id: "localhost:64318",
      node_addr: "localhost:8042",
      container_id: "container_e32_1456000363780_0002_01_000001",
      filename: "syslog"});
   model.then(function(value) {
     assert.ok(value);
     assert.ok(value.errors);
     assert.equal(value.errors.length, 1);
     assert.equal(value.errors[0].status, 404);
     assert.equal(value.errors[0].responseText, 'Not Found');
   });
});
