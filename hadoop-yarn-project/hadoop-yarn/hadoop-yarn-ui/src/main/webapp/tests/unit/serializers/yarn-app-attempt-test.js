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
import Converter from 'yarn-ui/utils/converter';

moduleFor('serializer:yarn-app-attempt', 'Unit | Serializer | yarn app attempt', {
  unit: true
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();
  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
});

test('Test normalizeSingleResponse', function(assert) {
	let serializer = this.subject(),
	payload = {
	  "appAttemptId": "appattempt_1479277364592_0001_000001",
	  "host": "N/A",
	  "rpcPort": "-1",
	  "trackingUrl": "http://ctr-e46-1478293962054-1774-01-000007.hwx.site:25005/proxy/application_1479277364592_0001/",
	  "originalTrackingUrl": "http://ctr-e46-1478293962054-1774-01-000004.hwx.site:19888/jobhistory/job/job_1479277364592_0001",
	  "diagnosticsInfo": "Attempt recovered after RM restart",
	  "appAttemptState": "FINISHED",
	  "amContainerId": "container_e01_1479277364592_0001_01_000001",
	  "startedTime": "1479280923398",
	  "finishedTime": "1479280966401"
	},
	modelClass = {
		modelName: 'yarn-app-attempt'
	};
	var response = serializer.normalizeSingleResponse({}, modelClass, payload, payload.appAttemptId);
	assert.ok(response);
	assert.ok(response.data);
	assert.equal(response.data.id, payload.appAttemptId);
	assert.equal(response.data.type, modelClass.modelName);
	assert.equal(response.data.attributes.appAttemptId, payload.appAttemptId);
	assert.equal(response.data.attributes.hosts, payload.host);
	assert.equal(response.data.attributes.amContainerId, payload.amContainerId);
	assert.equal(response.data.attributes.state, payload.appAttemptState);
	assert.equal(response.data.attributes.startedTime, Converter.timeStampToDate(payload.startedTime));
	assert.equal(response.data.attributes.finishedTime, Converter.timeStampToDate(payload.finishedTime));
});

test('Test normalizeArrayResponse', function(assert) {
	let serializer = this.subject(),
	modelClass = {
		modelName: 'yarn-app-attempt'
	},
	payload = {
	  "appAttempts": {
	    "appAttempt": [
	      {
	        "id": 1,
	        "startTime": 1479280923398,
	        "finishedTime": 1479280966401,
	        "containerId": "container_e01_1479277364592_0001_01_000001",
	        "nodeHttpAddress": "ctr-e46-1478293962054-1774-01-000004.hwx.site:25008",
	        "nodeId": "ctr-e46-1478293962054-1774-01-000004.hwx.site:25006",
	        "logsLink": "http://ctr-e46-1478293962054-1774-01-000004.hwx.site:25008/node/containerlogs/container_e01_1479277364592_0001_01_000001/user1",
	        "blacklistedNodes": "",
	        "nodesBlacklistedBySystem": "",
	        "appAttemptId": "appattempt_1479277364592_0001_000001"
	      }
	    ]
	  }
	};
	let response = serializer.normalizeArrayResponse({}, modelClass, payload);
	assert.ok(response);
	assert.ok(response.data);
	assert.ok(response.data[0]);
	assert.equal(response.data[0].id, payload.appAttempts.appAttempt[0].appAttemptId);
	assert.equal(response.data[0].type, modelClass.modelName);
	assert.equal(response.data[0].attributes.appAttemptId, payload.appAttempts.appAttempt[0].appAttemptId);
	assert.equal(response.data[0].attributes.containerId, payload.appAttempts.appAttempt[0].containerId);
	assert.equal(response.data[0].attributes.nodeHttpAddress, payload.appAttempts.appAttempt[0].nodeHttpAddress);
	assert.equal(response.data[0].attributes.nodeId, payload.appAttempts.appAttempt[0].nodeId);
	assert.equal(response.data[0].attributes.logsLink, payload.appAttempts.appAttempt[0].logsLink);
	assert.equal(response.data[0].attributes.startTime,
		Converter.timeStampToDate(payload.appAttempts.appAttempt[0].startTime));
	assert.equal(response.data[0].attributes.finishedTime,
		Converter.timeStampToDate(payload.appAttempts.appAttempt[0].finishedTime));
});