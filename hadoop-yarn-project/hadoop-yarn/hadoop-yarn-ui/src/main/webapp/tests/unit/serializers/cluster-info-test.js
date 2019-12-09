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

moduleFor('serializer:cluster-info', 'Unit | Serializer | cluster info', {
  unit: true
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();
  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.normalizeArrayResponse);
});

test('Test normalizeArrayResponse', function(assert) {
	var serializer = this.subject(),
  modelClass = {
    modelName: "cluster-info"
  },
	payload = {
	  "clusterInfo": {
	    "id":1324053971963,
	    "startedOn":1324053971963,
	    "state":"STARTED",
	    "resourceManagerVersion":"0.23.1-SNAPSHOT",
	    "resourceManagerBuildVersion":"0.23.1-SNAPSHOT from 1214049 by user1 source checksum 050cd664439d931c8743a6428fd6a693",
	    "resourceManagerVersionBuiltOn":"Tue Dec 13 22:12:48 CST 2011",
	    "hadoopVersion":"0.23.1-SNAPSHOT",
	    "hadoopBuildVersion":"0.23.1-SNAPSHOT from 1214049 by user1 source checksum 11458df3bb77342dca5f917198fad328",
	    "hadoopVersionBuiltOn":"Tue Dec 13 22:12:26 CST 2011"
	  }
	};
	var id = 1324053971963;
	var normalized = serializer.normalizeArrayResponse({}, modelClass, payload, id, null);
	assert.expect(12);
	assert.ok(normalized.data);
	assert.ok(normalized.data[0]);
	assert.equal(normalized.data.length, 1);
	assert.equal(normalized.data[0].id, id);
	assert.equal(normalized.data[0].type, modelClass.modelName);
	assert.equal(normalized.data[0].attributes.startedOn, payload.clusterInfo.startedOn);
	assert.equal(normalized.data[0].attributes.state, payload.clusterInfo.state);
	assert.equal(normalized.data[0].attributes.resourceManagerVersion,
		payload.clusterInfo.resourceManagerVersion);
	assert.equal(normalized.data[0].attributes.resourceManagerBuildVersion,
		payload.clusterInfo.resourceManagerBuildVersion);
	assert.equal(normalized.data[0].attributes.hadoopVersion,
		payload.clusterInfo.hadoopVersion);
	assert.equal(normalized.data[0].attributes.hadoopBuildVersion,
		payload.clusterInfo.hadoopBuildVersion);
	assert.equal(normalized.data[0].attributes.hadoopVersionBuiltOn,
		payload.clusterInfo.hadoopVersionBuiltOn);
});