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

import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
  internalNormalizeSingleResponse(store, primaryModelClass, payload, id) {
    if (payload.nodeInfo) {
      payload = payload.nodeInfo;
    }

    var fixedPayload = {
      id: id,
      type: primaryModelClass.modelName,
      attributes: {
        totalVmemAllocatedContainersMB: payload.totalVmemAllocatedContainersMB,
        totalPmemAllocatedContainersMB: payload.totalPmemAllocatedContainersMB,
        totalVCoresAllocatedContainers: payload.totalVCoresAllocatedContainers,
        vmemCheckEnabled: payload.vmemCheckEnabled,
        pmemCheckEnabled: payload.pmemCheckEnabled,
        nodeHealthy: payload.nodeHealthy,
        lastNodeUpdateTime: Converter.timeStampToDate(payload.lastNodeUpdateTime),
        healthReport: payload.healthReport || 'N/A',
        nmStartupTime: payload.nmStartupTime? Converter.timeStampToDate(payload.nmStartupTime) : '',
        nodeManagerBuildVersion: payload.nodeManagerBuildVersion,
        hadoopBuildVersion: payload.hadoopBuildVersion
      }
    };
    return fixedPayload;
  },

  normalizeSingleResponse(store, primaryModelClass, payload, id/*, requestType*/) {
    // payload is of the form {"nodeInfo":{}}
    var p = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload, id);
    return { data: p };
  },
});
