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

moduleFor('route:yarn-node-app', 'Unit | Route | NodeApp', {
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
});

test('Test getting specific app on a node', function(assert) {
  var response =
      {id:"application_1456251210105_0001", state:"FINISHED", user:"root"};
  var store = {
    queryRecord: function(type, query) {
      return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
    }
  };
  assert.expect(6);
  var route = this.subject();
  route.set('store', store);
  var model =
      route.model({node_id:"localhost:64318", node_addr:"localhost:8042",
          app_id:"application_1456251210105_0001"}).
      then(
        function(value){
          assert.ok(value);
          assert.ok(value.nodeApp);
          assert.deepEqual(value.nodeApp, response);
          assert.ok(value.nodeInfo);
          assert.equal(value.nodeInfo.addr, 'localhost:8042');
          assert.equal(value.nodeInfo.id, 'localhost:64318');
        }
      );
});
