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
    if (payload.node) {
      payload = payload.node;
    }

    var fixedPayload = {
      id: id,
      type: primaryModelClass.modelName,
      attributes: {
        rack: payload.rack,
        state: payload.state,
        nodeHostName: payload.nodeHostName,
        nodeHTTPAddress: payload.nodeHTTPAddress,
        lastHealthUpdate: Converter.timeStampToDate(payload.lastHealthUpdate),
        healthReport: payload.healthReport || 'N/A',
        numContainers: payload.numContainers,
        usedMemoryMB: payload.usedMemoryMB,
        availMemoryMB: payload.availMemoryMB,
        usedVirtualCores: payload.usedVirtualCores,
        availableVirtualCores: payload.availableVirtualCores,
        version: payload.version,
        nodeLabels: payload.nodeLabels,
        usedResource: payload.usedResource,
        availableResource: payload.availableResource
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

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    // expected response is of the form { data: [ {}, {} ] }
    var normalizedArrayResponse = {};
    if (payload.nodes && payload.nodes.node) {
      // payload is of the form { "nodes": { "node": [ {},{},{} ]  } }
      normalizedArrayResponse.data = payload.nodes.node.map(singleNode => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass,
          singleNode, singleNode.id);
          }, this);
    } else {
      normalizedArrayResponse.data = [];
    }
    return normalizedArrayResponse;
  }
});
