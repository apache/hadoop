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

moduleFor('route:yarn-apps', 'Unit | Route | yarn apps', {
  unit: true
});

test('Basic creation test', function(assert) {
  var route = this.subject();
  assert.ok(route);
  assert.ok(route.model);
  assert.ok(route.unloadAll);
});

test("Test getting yarn applications data", function(assert) {
	var response = {
	  "apps": {
	    "app": [{
		"finishedTime" : 1326815598530,
        "amContainerLogs" : "http://host.domain.com:8042/node/containerlogs/container_1326815542473_0001_01_000001",
        "trackingUI" : "History",
        "state" : "FINISHED",
        "user" : "user1",
        "id" : "application_1326815542473_0001",
        "clusterId" : 1326815542473,
        "finalStatus" : "SUCCEEDED",
        "amHostHttpAddress" : "host.domain.com:8042",
        "amRPCAddress" : "host.domain.com:4201",
        "progress" : 100,
        "name" : "word count",
        "startedTime" : 1326815573334,
        "elapsedTime" : 25196,
        "diagnostics" : "",
        "trackingUrl" : "http://host.domain.com:8088/proxy/application_1326815542473_0001/jobhistory/job/job_1326815542473",
        "queue" : "default",
        "allocatedMB" : 0,
        "allocatedVCores" : 0,
        "runningContainers" : 0,
        "applicationType" : "MAPREDUCE",
        "applicationTags" : "",
        "memorySeconds" : 151730,
        "vcoreSeconds" : 103,
        "unmanagedApplication" : "false",
        "applicationPriority" : 0,
        "appNodeLabelExpression" : "",
        "amnodeLabelExpression" : ""
	    }]
	  }
	};
	var store = {
		findAll: function(type) {
			return new Ember.RSVP.Promise(function(resolve) {
				if (type === 'yarn-app') {
					resolve(response);
				} else {
					resolve({});
				}
      });
		}
	};
	var route = this.subject();
	route.set('store', store);
	assert.expect(3);
	route.model().then(function(model) {
		assert.ok(model);
		assert.ok(model.apps);
		assert.deepEqual(model.apps, response);
	});
});

test('Test getting cluster metrics data', function(assert) {
	var response = {
		"clusterMetrics": {
	    "appsSubmitted": 0,
	    "appsCompleted": 0,
	    "appsPending": 0,
	    "appsRunning": 0,
	    "appsFailed": 0,
	    "appsKilled": 0,
	    "reservedMB": 0,
	    "availableMB": 17408,
	    "allocatedMB": 0,
	    "reservedVirtualCores": 0,
	    "availableVirtualCores": 7,
	    "allocatedVirtualCores": 1,
	    "containersAllocated": 0,
	    "containersReserved": 0,
	    "containersPending": 0,
	    "totalMB": 17408,
	    "totalVirtualCores": 8,
	    "totalNodes": 1,
	    'lostNodes': 0,
	    "unhealthyNodes": 0,
	    "decommissionedNodes": 0,
	    'rebootedNodes': 0,
	    "activeNodes": 1
	  }
	};
	var store = {
		findAll: function(type) {
			return new Ember.RSVP.Promise(function(resolve) {
				if (type === 'ClusterMetric') {
					resolve(response);
				} else {
					resolve({});
				}
      });
		}
	};
	var route = this.subject();
	route.set('store', store);
	assert.expect(3);
	route.model().then(function(model) {
		assert.ok(model);
		assert.ok(model.clusterMetrics);
		assert.deepEqual(model.clusterMetrics, response);
	});
});