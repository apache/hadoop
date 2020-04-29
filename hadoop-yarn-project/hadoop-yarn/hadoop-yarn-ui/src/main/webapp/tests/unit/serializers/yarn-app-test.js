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

moduleFor('serializer:yarn-app', 'Unit | Serializer | yarn app', {
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.internalNormalizeSingleResponse);
});

// Replace this with your real tests.
test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-app"
  },
  payload = {
	   app : {
	      finishedTime : 1326824991300,
				amContainerLogs : "localhost:8042/node/containerlogs/container_1326821518301_0005_01_000001",
				trackingUI : "History",
				state : "FINISHED",
				user : "user1",
				id : "application_1326821518301_0005",
				clusterId : 1326821518301,
				finalStatus : "SUCCEEDED",
				amHostHttpAddress : "localhost:8042",
				amRPCAddress : "localhost:4201",
				progress : 100,
				name : "Sleep job",
				applicationType : "Yarn",
				startedTime : 1326824544552,
				elapsedTime : 446748,
				diagnostics : "",
				trackingUrl : "localhost:8088/proxy/application_1326821518301_0005/jobhistory/job/job_1326821518301_5_5",
				queue : "a1",
				memorySeconds : 151730,
				vcoreSeconds : 103,
				unmanagedApplication : "false",
				applicationPriority : 0,
				appNodeLabelExpression : "",
				amNodeLabelExpression : ""
	   }
	},
	id = "application_1326821518301_0005";
	var response = serializer.normalizeSingleResponse({}, modelClass, payload, id);
	assert.equal(response.data.id, id);
  assert.equal(response.data.type, modelClass.modelName);
  assert.equal(response.data.attributes.appName, payload.app.name);
  assert.equal(response.data.attributes.user, payload.app.user);
  assert.equal(response.data.attributes.state, payload.app.state);
  assert.equal(response.data.attributes.finalStatus, payload.app.finalStatus);
  assert.equal(response.data.attributes.queue, payload.app.queue);
  assert.equal(response.data.attributes.applicationType, payload.app.applicationType);
  assert.equal(response.data.attributes.progress, payload.app.progress);
});
