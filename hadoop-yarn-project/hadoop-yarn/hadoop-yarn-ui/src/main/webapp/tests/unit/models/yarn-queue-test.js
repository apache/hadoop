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
import Ember from 'ember';

moduleForModel('yarn-queue', 'Unit | Model | yarn queue', {
  needs: ['model:yarn-user']
});

test('Basic creation test', function(assert) {
  let model = this.subject();
  assert.ok(model);
  assert.ok(model.name);
  assert.ok(model.children);
  assert.ok(model.parent);
  assert.ok(model.capacity);
  assert.ok(model.maxCapacity);
  assert.ok(model.usedCapacity);
  assert.ok(model.absCapacity);
  assert.ok(model.absMaxCapacity);
  assert.ok(model.absUsedCapacity);
  assert.ok(model.state);
  assert.ok(model.userLimit);
  assert.ok(model.userLimitFactor);
  assert.ok(model.preemptionDisabled);
  assert.ok(model.numPendingApplications);
  assert.ok(model.numActiveApplications);
  assert.ok(model.users);
});

test('Test fields', function(assert) {
	let model = this.subject();
	Ember.run(function() {
		model.set('name', 'default');
		model.set('children', []);
		model.set('parent', 'root');
		model.set('capacity', 100);
		model.set('maxCapacity', 100);
		model.set('usedCapacity', 0);
		model.set('absCapacity', 100);
		model.set('absMaxCapacity', 100);
		model.set('absUsedCapacity', 0);
		model.set('state', 'RUNNING');
		model.set('userLimit', 100);
		model.set('userLimitFactor', 1.0);
		model.set('preemptionDisabled', true);
		model.set('numPendingApplications', 0);
		model.set('numActiveApplications', 0);

		assert.equal(model.get('isLeafQueue'), true);
		assert.deepEqual(model.get('capacitiesBarChartData'),
			[{label: "Absolute Capacity", value: 100},
			{label: "Absolute Used", value: 0},
			{label: "Absolute Max Capacity", value: 100}]);
		assert.deepEqual(model.get('userUsagesDonutChartData'), []);
		assert.equal(model.get('hasUserUsages'), false);
		assert.deepEqual(model.get('numOfApplicationsDonutChartData'),
			[{label: "Pending Apps", value: 0}, {label: "Active Apps", value: 0}]);

    var user1 = model.store.createRecord('yarn-user', {
      name: 'user1',
      usedMemoryMB: 2048
    });
    model.set('users', [user1]);
    assert.deepEqual(model.get('userUsagesDonutChartData'),
      [{label: 'user1', value: 2048}]);
    assert.equal(model.get('hasUserUsages'), true);

    model.set('name', 'root');
    let child = this.subject({
      name: 'default'
    });
    model.set('children', [child]);
    model.set('parent', '');
    model.set('capacity', 100);
		model.set('maxCapacity', 100);
		model.set('usedCapacity', 0);
		model.set('absCapacity', 100);
		model.set('absMaxCapacity', 100);
		model.set('absUsedCapacity', 0);
		model.set('state', 'RUNNING');
    model.set('numPendingApplications', 0);
		model.set('numActiveApplications', 0);

    assert.equal(model.get('isLeafQueue'), false);
    assert.deepEqual(model.get('capacitiesBarChartData'),
      [
        {label: "Absolute Capacity", value: 100},
        {label: "Absolute Used", value: 0},
        {label: "Absolute Max Capacity", value: 100}
      ]);
    assert.deepEqual(model.get('numOfApplicationsDonutChartData'),
      [
        {label: "Pending Apps", value: 0},
        {label: "Active Apps", value: 0}
      ]);
	}.bind(this));
});

test('Test relationship with yarn-user', function(assert) {
	let YarnQueue = this.store().modelFor('yarn-queue');
	let relationship = Ember.get(YarnQueue, 'relationshipsByName').get('users');
	assert.expect(2);
	assert.equal(relationship.key, 'users');
  assert.equal(relationship.kind, 'hasMany');
});
