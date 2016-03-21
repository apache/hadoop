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

import converter from '../../../utils/converter';
import { module, test } from 'qunit';

module('Unit | Utility | Converter');

// Replace this with your real tests.
test('it works', function(assert) {
  assert.ok(converter);
  assert.ok(converter.splitForContainerLogs);
});

test('split for container logs', function(assert) {
  var id = "localhost:64318!container_e32_1456000363780_0002_01_000001!" +
      "syslog";
  var arr = converter.splitForContainerLogs(id);
  assert.ok(arr);
  assert.deepEqual(arr, ["localhost:64318",
      "container_e32_1456000363780_0002_01_000001", "syslog"]);
  id = "localhost:64318!container_e32_1456000363780_0002_01_000001!" +
      "syslog!logs";
  arr = converter.splitForContainerLogs(id);
  assert.ok(arr);
  assert.deepEqual(arr, ["localhost:64318",
      "container_e32_1456000363780_0002_01_000001", "syslog!logs"]);
  id = "localhost:64318!container_e32_1456000363780_0002_01_000001";
  arr = converter.splitForContainerLogs(id);
  assert.notOk(arr);
  id = null;
  arr = converter.splitForContainerLogs(id);
  assert.notOk(arr);
  id = undefined;
  arr = converter.splitForContainerLogs(id);
  assert.notOk(arr);
});
