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

moduleForModel('yarn-app', 'Unit | Model | yarn app', {
  needs: []
});

test('Basic creation test', function(assert) {
  var model = this.subject();
  assert.ok(model);
  assert.ok(model.appName);
  assert.ok(model.user);
  assert.ok(model.queue);
  assert.ok(model.state);
  assert.ok(model.startTime);
  assert.ok(model.elapsedTime);
  assert.ok(model.finalStatus);
  assert.ok(model.finishedTime);
  assert.ok(model.progress);
  assert.ok(model.diagnostics);
  assert.ok(model.amContainerLogs);
  assert.ok(model.amHostHttpAddress);
  assert.ok(model.logAggregationStatus);
  assert.ok(model.unmanagedApplication);
  assert.ok(model.amNodeLabelExpression);
  assert.ok(model.applicationTags);
  assert.ok(model.applicationType);
  assert.ok(model.priority);
  assert.ok(model.allocatedMB);
  assert.ok(model.allocatedVCores);
  assert.ok(model.runningContainers);
  assert.ok(model.memorySeconds);
  assert.ok(model.vcoreSeconds);
  assert.ok(model.preemptedResourceMB);
  assert.ok(model.preemptedResourceVCores);
  assert.ok(model.numNonAMContainerPreempted);
  assert.ok(model.numAMContainerPreempted);
  assert.ok(model.clusterUsagePercentage);
  assert.ok(model.queueUsagePercentage);
  assert.ok(model.currentAppAttemptId);
});

test('Testing fields', function(assert) {
	let model = this.subject({
		"finalStatus": "SUCCEEDED",
		"startedTime": 1479280923314,
    "finishedTime": 1479280966402,
    "allocatedMB": 0,
    "allocatedVCores": 0,
    "preemptedResourceMB": 0,
    "preemptedResourceVCores": 0,
    "memorySeconds": 93406,
    "vcoreSeconds": 49,
    "progress": 100,
    "runningContainers": 0
	});

	assert.equal(model.get('isFailed'), false);
	assert.equal(model.get('validatedFinishedTs'), 1479280966402);
	assert.equal(model.get('allocatedResource'), '0 MBs, 0 VCores');
	assert.equal(model.get('preemptedResource'), '0 MBs, 0 VCores');
	assert.equal(model.get('aggregatedResourceUsage'), '93406 MBs, 49 VCores (× Secs)');
	assert.equal(model.get('progressStyle'), 'width: 100%');
	assert.equal(model.get('runningContainersNumber'), 0);
	assert.equal(model.get('finalStatusStyle'), 'label label-success');
});
