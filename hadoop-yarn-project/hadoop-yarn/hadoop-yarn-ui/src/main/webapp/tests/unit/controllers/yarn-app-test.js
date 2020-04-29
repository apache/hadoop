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

moduleFor('controller:yarn-app', 'Unit | Controller | yarn app', {
  unit: true
});

test('Basic creation test', function(assert) {
  let controller = this.subject();
  assert.ok(controller);
  assert.ok(controller.amHostHttpAddressFormatted);
  var app = Ember.Object.create({
	amHostHttpAddress: 'localhost:8042'
  });
  controller.set('model', Ember.Object.create({app: app}));
  assert.equal(controller.get('amHostHttpAddressFormatted'), 'http://localhost:8042');
});
