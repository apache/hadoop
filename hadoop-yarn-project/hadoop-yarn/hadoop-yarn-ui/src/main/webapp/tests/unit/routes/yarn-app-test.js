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

moduleFor('route:yarn-app', 'Unit | Route | yarn app', {
  unit: true
});

test('Basic creation test', function(assert) {
  let route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
  assert.ok(route.unloadAll);
});

test('Test getting yarn application data', function(assert) {
	var response = {
   "app" : {
      "finishedTime" : 1326824991300,
      "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326821518301_0005_01_000001",
      "trackingUI" : "History",
      "state" : "FINISHED",
      "user" : "user1",
      "id" : "application_1326821518301_0005",
      "clusterId" : 1326821518301,
      "finalStatus" : "SUCCEEDED",
      "amHostHttpAddress" : "host.domain.com:8042",
      "amRPCAddress" : "host.domain.com:4201",
      "progress" : 100,
      "name" : "Sleep job",
      "applicationType" : "Yarn",
      "startedTime" : 1326824544552,
      "elapsedTime" : 446748,
      "diagnostics" : "",
      "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5",
      "queue" : "a1",
      "memorySeconds" : 151730,
      "vcoreSeconds" : 103,
      "unmanagedApplication" : "false",
      "applicationPriority" : 0,
      "appNodeLabelExpression" : "",
      "amNodeLabelExpression" : ""
   }
	};
	var store = {
		find: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
		},
		findAll: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve({});
      });
		},
		query: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve([]);
      });
		}
	};
	var appId = "application_1326821518301_0005";
	var route = this.subject();
	route.set('store', store);
	assert.expect(3);
	route.model({app_id: appId}).then(function(model) {
		assert.ok(model);
		assert.ok(model.app);
		assert.deepEqual(model.app, response);
	});
});

test('Test getting yarm rm nodes data', function(assert) {
	var response = {
	  "nodes": {
	    "node": [{
        "rack":"\/default-rack",
        "state":"NEW",
        "id":"h2:1235",
        "nodeHostName":"h2",
        "nodeHTTPAddress":"h2:2",
        "healthStatus":"Healthy",
        "lastHealthUpdate":1324056895432,
        "healthReport":"Healthy",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8
      },
      {
        "rack":"\/default-rack",
        "state":"NEW",
        "id":"h1:1234",
        "nodeHostName":"h1",
        "nodeHTTPAddress":"h1:2",
        "healthStatus":"Healthy",
        "lastHealthUpdate":1324056895092,
        "healthReport":"Healthy",
        "numContainers":0,
        "usedMemoryMB":0,
        "availMemoryMB":8192,
        "usedVirtualCores":0,
        "availableVirtualCores":8
      }]
	  }
	};
	var store = {
		find: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve({});
      });
		},
		findAll: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve(response);
      });
		},
		query: function() {
			return new Ember.RSVP.Promise(function(resolve) {
        resolve([]);
      });
		}
	};
	var appId = "application_1326821518301_0005";
	var route = this.subject();
	route.set('store', store);
	assert.expect(4);
	route.model({app_id: appId}).then(function(model) {
		assert.ok(model);
		assert.ok(model.nodes);
		assert.deepEqual(model.nodes.nodes.node[0], response.nodes.node[0]);
		assert.deepEqual(model.nodes.nodes.node[1], response.nodes.node[1]);
	});
});