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

moduleForModel('yarn-app-attempt', 'Unit | Model | yarn app attempt', {
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();
  assert.ok(model);
  assert.ok(model.startTime);
  assert.ok(model.startedTime);
  assert.ok(model.finishedTime);
  assert.ok(model.containerId);
  assert.ok(model.amContainerId);
  assert.ok(model.nodeHttpAddress);
  assert.ok(model.nodeId);
  assert.ok(model.hosts);
  assert.ok(model.logsLink);
  assert.ok(model.state);
  assert.ok(model.appAttemptId);
});

test('Testing fields', function(assert) {
	let model = this.subject({
		"id": "appattempt_1479277364592_0001_000001",
		"startedTime": Converter.timeStampToDate("1479280913398"),
		"finishedTime": Converter.timeStampToDate("1479280966401"),
    "amContainerId": "container_e01_1479277364592_0001_01_000001",
    "hosts": "N/A",
    "state": "FINISHED",
    "appAttemptId": "appattempt_1479277364592_0001_000001"
	});

	assert.equal(model.get('appId'), "application_1479277364592_0001");
	assert.equal(model.get('startTs'), 1479280913000);
	assert.equal(model.get('finishedTs'), 1479280966000);
	assert.equal(model.get('shortAppAttemptId'), "appattempt_1479277364592_0001_000001");
	assert.equal(model.get('appMasterContainerId'), "container_e01_1479277364592_0001_01_000001");
	assert.equal(model.get('IsAmNodeUrl'), false);
	assert.equal(model.get('amNodeId'), "N/A");
	assert.equal(model.get('IsLinkAvailable'), false);
	assert.equal(model.get('elapsedTime'), "53 Secs");
	assert.equal(model.get('link'), "/yarn-app-attempt/appattempt_1479277364592_0001_000001");
	assert.equal(model.get('linkname'), "yarn-app-attempt");
	assert.equal(model.get('attemptState'), "FINISHED");
});
