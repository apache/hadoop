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

moduleFor('route:yarn-node-containers', 'Unit | Route | NodeContainers', {
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
});

test('Test getting apps on a node', function(assert) {
  var response =
      [{id: "container_e32_1456000363780_0002_01_000001", state: "RUNNING",
      exitCode:-1000,diagnostics:"",user:"root",totalMemoryNeededMB:2048,
      totalVCoresNeeded:1,containerLogsLink: "http://localhost:8042/node/" +
      "containerlogs/container_e32_1456000363780_0002_01_000001/root",
      nodeId: "localhost:64318", containerLogFiles:["syslog","stderr",
      "stdout"]},
      {id:"container_e32_1456000363780_0002_01_000003", state:"RUNNING",
      exitCode:-1000, diagnostics:"", user:"root", totalMemoryNeededMB:1024,
      totalVCoresNeeded:1,containerLogsLink:"http://localhost:8042/node" +
      "/containerlogs/container_e32_1456000363780_0002_01_000003/root",
      nodeId:"localhost:64318",containerLogFiles:["syslog","stderr",
      "syslog.shuffle","stdout"]}];
  var store = {
    query: function(type, query) {
      return new Ember.RSVP.Promise(function(resolve) {
        resolve(response.slice());
      });
    }
  };
  assert.expect(8);
  var route = this.subject();
  route.set('store', store);
  var model =
      route.model({node_id:"localhost:64318", node_addr:"localhost:8042"}).
      then(
        function(value){
          assert.ok(value);
          assert.ok(value.containers);
          assert.equal(value.containers.length, 2);
          assert.deepEqual(value.containers[0], response[0]);
          assert.deepEqual(value.containers[1], response[1]);
          assert.ok(value.nodeInfo);
          assert.equal(value.nodeInfo.addr, 'localhost:8042');
          assert.equal(value.nodeInfo.id, 'localhost:64318');
        }
      );
});
