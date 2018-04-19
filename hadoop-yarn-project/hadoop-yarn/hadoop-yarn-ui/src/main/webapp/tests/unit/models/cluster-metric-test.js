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

import { moduleForModel, test } from 'ember-qunit';

moduleForModel('cluster-metric', 'Unit | Model | cluster metric', {
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();
  assert.ok(model);
  assert.ok(model.appsSubmitted);
  assert.ok(model.appsCompleted);
  assert.ok(model.appsPending);
  assert.ok(model.appsRunning);
  assert.ok(model.appsFailed);
  assert.ok(model.appsKilled);
  assert.ok(model.reservedMB);
  assert.ok(model.availableMB);
  assert.ok(model.allocatedMB);
  assert.ok(model.reservedVirtualCores);
  assert.ok(model.availableVirtualCores);
  assert.ok(model.allocatedVirtualCores);
  assert.ok(model.containersAllocated);
  assert.ok(model.containersReserved);
  assert.ok(model.containersPending);
  assert.ok(model.totalMB);
  assert.ok(model.totalVirtualCores);
  assert.ok(model.totalNodes);
  assert.ok(model.lostNodes);
  assert.ok(model.unhealthyNodes);
  assert.ok(model.decommissionedNodes);
  assert.ok(model.rebootedNodes);
  assert.ok(model.activeNodes);
});

test('Testing fields', function(assert) {
	let model = this.subject({
		"appsCompleted": 0,
    "appsPending": 0,
    "appsRunning": 0,
    "appsFailed": 0,
    "appsKilled": 0,
    "reservedMB": 0,
    "availableMB": 32768,
    "allocatedMB": 0,
    "activeNodes": 4,
    "unhealthyNodes": 0,
    "decommissionedNodes": 0,
    "reservedVirtualCores": 0,
    "availableVirtualCores": 32,
    "allocatedVirtualCores": 0
	});

	assert.deepEqual(model.get('getFinishedAppsDataForDonutChart'),
		[{label: "Completed", value: 0}, {label: "Killed", value: 0}, {label: "Failed", value: 0}]);
	assert.deepEqual(model.get('getRunningAppsDataForDonutChart'),
		[{label: "Pending", value: 0}, {label: "Running", value: 0}]);
	assert.deepEqual(model.get('getNodesDataForDonutChart'),
		[{label: "Active", value: 4}, {label: "Unhealthy", value: 0}, {label: "Decomissioned", value: 0}]);
	assert.deepEqual(model.get('getMemoryDataForDonutChart'),
		[{label: "Allocated", value: 0}, {label: "Reserved", value: 0}, {label: "Available", value: 32768}]);
	assert.deepEqual(model.get('getVCoreDataForDonutChart'),
		[{label: "Allocated", value: 0}, {label: "Reserved", value: 0}, {label: "Available", value: 32}]);
});
