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

moduleFor('serializer:cluster-metric', 'Unit | Serializer | cluster metric', {
  unit: true
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();
  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
});

test('Test normalizeArrayResponse', function(assert) {
	let serializer = this.subject(),
	modelClass = {
		modelName: 'cluster-metric'
	},
	payload = {
	  "clusterMetrics": {
	    "appsSubmitted":0,
	    "appsCompleted":0,
	    "appsPending":0,
	    "appsRunning":0,
	    "appsFailed":0,
	    "appsKilled":0,
	    "reservedMB":0,
	    "availableMB":17408,
	    "allocatedMB":0,
	    "reservedVirtualCores":0,
	    "availableVirtualCores":7,
	    "allocatedVirtualCores":1,
	    "containersAllocated":0,
	    "containersReserved":0,
	    "containersPending":0,
	    "totalMB":17408,
	    "totalVirtualCores":8,
	    "totalNodes":1,
	    "lostNodes":0,
	    "unhealthyNodes":0,
	    "decommissionedNodes":0,
	    "rebootedNodes":0,
	    "activeNodes":1
	  }
	};
	let normalized = serializer.normalizeArrayResponse({}, modelClass, payload, 1, null);
	assert.expect(20);
	assert.ok(normalized.data);
	assert.ok(normalized.data[0]);
	assert.equal(normalized.data[0].id, 1);
	assert.equal(normalized.data[0].type, modelClass.modelName);
	assert.equal(normalized.data[0].attributes.appsSubmitted, payload.clusterMetrics.appsSubmitted);
	assert.equal(normalized.data[0].attributes.appsCompleted, payload.clusterMetrics.appsCompleted);
	assert.equal(normalized.data[0].attributes.appsPending, payload.clusterMetrics.appsPending);
	assert.equal(normalized.data[0].attributes.appsRunning, payload.clusterMetrics.appsRunning);
	assert.equal(normalized.data[0].attributes.appsFailed, payload.clusterMetrics.appsFailed);
	assert.equal(normalized.data[0].attributes.appsKilled, payload.clusterMetrics.appsKilled);
	assert.equal(normalized.data[0].attributes.reservedMB, payload.clusterMetrics.reservedMB);
	assert.equal(normalized.data[0].attributes.availableMB, payload.clusterMetrics.availableMB);
	assert.equal(normalized.data[0].attributes.allocatedMB, payload.clusterMetrics.allocatedMB);
	assert.equal(normalized.data[0].attributes.totalMB, payload.clusterMetrics.totalMB);
	assert.equal(normalized.data[0].attributes.reservedVirtualCores,
		payload.clusterMetrics.reservedVirtualCores);
	assert.equal(normalized.data[0].attributes.availableVirtualCores,
		payload.clusterMetrics.availableVirtualCores);
	assert.equal(normalized.data[0].attributes.allocatedVirtualCores,
		payload.clusterMetrics.allocatedVirtualCores);
	assert.equal(normalized.data[0].attributes.totalVirtualCores,
		payload.clusterMetrics.totalVirtualCores);
	assert.equal(normalized.data[0].attributes.activeNodes, payload.clusterMetrics.activeNodes);
	assert.equal(normalized.data[0].attributes.totalNodes, payload.clusterMetrics.totalNodes);
});