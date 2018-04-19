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

moduleForModel('cluster-info', 'Unit | Model | cluster info', {
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();
  assert.ok(model);
  assert.ok(model.startedOn);
  assert.ok(model.state);
  assert.ok(model.haState);
  assert.ok(model.rmStateStoreName);
  assert.ok(model.resourceManagerVersion);
  assert.ok(model.resourceManagerBuildVersion);
  assert.ok(model.hadoopVersion);
  assert.ok(model.hadoopBuildVersion);
  assert.ok(model.hadoopVersionBuiltOn);
});
