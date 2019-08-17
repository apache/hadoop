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

moduleFor('adapter:yarn-app-attempt', 'Unit | Adapter | yarn app attempt', {
  unit: true
});

test('Basic creation test', function(assert) {
  let adapter = this.subject({
    host: "localhost:8088",
    namespace: "ws/v1/cluster"
  });
  assert.ok(adapter);
  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.ok(adapter.urlForQuery);
  assert.ok(adapter.urlForFindRecord);
  assert.equal(adapter.namespace, "ws/v1/cluster");
});

test("urlForQuery test", function(assert) {
	let adapter = this.subject({
    host: "localhost:8088",
    namespace: "ws/v1/cluster"
  });
  var host = adapter.host;
  assert.equal(adapter.urlForQuery({appId: "application_1472139065385_0007"}),
    host + "/ws/v1/cluster/apps/application_1472139065385_0007/appattempts");
});

test("urlForFindRecord test", function(assert) {
	let adapter = this.subject({
    host: "localhost:8088",
    namespace: "ws/v1/cluster"
  });
  var host = adapter.host;
  var attemptId = "appattempt_1472139065385_0007_000001";
	assert.equal(adapter.urlForFindRecord(attemptId),
		host + "/ws/v1/cluster/apps/" +
		Converter.attemptIdToAppId(attemptId) + "/appattempts/" + attemptId);
});