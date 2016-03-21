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

moduleFor('serializer:yarn-node', 'Unit | Serializer | Node', {
});

test('Basic creation test', function(assert) {
  let serializer = this.subject();

  assert.ok(serializer);
  assert.ok(serializer.normalizeSingleResponse);
  assert.ok(serializer.internalNormalizeSingleResponse);
});

test('normalizeSingleResponse test', function(assert) {
  let serializer = this.subject(),
  modelClass = {
    modelName: "yarn-node"
  },
  payload = {
    nodeInfo: {
      healthReport: "Healthy", totalVmemAllocatedContainersMB: 344064,
      totalPmemAllocatedContainersMB: 163840,
      totalVCoresAllocatedContainers: 160,
      vmemCheckEnabled: true, pmemCheckEnabled: true,
      lastNodeUpdateTime: 1456250210310, nodeHealthy: true,
      nodeManagerVersion: "3.0.0-SNAPSHOT",
      nodeManagerBuildVersion: "3.0.0-SNAPSHOT",
      nodeManagerVersionBuiltOn: "2000-01-01T00:00Z",
      hadoopVersion: "3.0.0-SNAPSHOT",
      hadoopBuildVersion: "3.0.0-SNAPSHOT",
      hadoopVersionBuiltOn: "2000-01-01T00:00Z",
      id: "localhost:64318", nodeHostName: "192.168.0.102",
      nmStartupTime: 1456250208231
    }
  };
  assert.expect(6);
  var id = "localhost:64318";
  var response = serializer.normalizeSingleResponse({}, modelClass, payload, id, null);
  assert.equal(response.data.id, id);
  assert.equal(response.data.type, modelClass.modelName);
  assert.equal(response.data.attributes.totalVmemAllocatedContainersMB,
      payload.nodeInfo.totalVmemAllocatedContainersMB);
  assert.equal(response.data.attributes.totalPmemAllocatedContainersMB,
      payload.nodeInfo.totalPmemAllocatedContainersMB);
  assert.equal(response.data.attributes.totalVCoresAllocatedContainers,
      payload.nodeInfo.totalVCoresAllocatedContainers);
  assert.equal(response.data.attributes.nmStartupTime,
      Converter.timeStampToDate(payload.nodeInfo.nmStartupTime));
});

