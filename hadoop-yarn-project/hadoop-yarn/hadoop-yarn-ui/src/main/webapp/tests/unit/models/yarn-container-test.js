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
import Converter from 'yarn-ui/utils/converter';

moduleForModel('yarn-container', 'Unit | Model | yarn container', {
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();
  assert.ok(model);
  assert.ok(model.allocatedMB);
  assert.ok(model.allocatedVCores);
  assert.ok(model.assignedNodeId);
  assert.ok(model.priority);
  assert.ok(model.startedTime);
  assert.ok(model.finishedTime);
  assert.ok(model.logUrl);
  assert.ok(model.containerExitStatus);
  assert.ok(model.containerState);
  assert.ok(model.nodeHttpAddress);
});

test('Testing fields', function(assert) {
  let model = this.subject({
    startedTime: Converter.timeStampToDate(1481696493793),
    finishedTime: Converter.timeStampToDate(1481696501857)
  });
  assert.equal(model.get('startTs'), 1481696493000);
  assert.equal(model.get('finishedTs'), 1481696501000);
  assert.equal(model.get('elapsedTime'), '8 Secs');
});
