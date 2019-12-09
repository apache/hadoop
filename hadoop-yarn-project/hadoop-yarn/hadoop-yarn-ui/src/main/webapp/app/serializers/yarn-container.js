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
    internalNormalizeSingleResponse(store, primaryModelClass, payload) {

      var fixedPayload = {
        id: payload.containerId,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          allocatedMB: payload.allocatedMB,
          allocatedVCores: payload.allocatedVCores,
          assignedNodeId: payload.assignedNodeId,
          priority: payload.priority,
          startedTime: Converter.timeStampToDate(payload.startedTime),
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          elapsedTime: payload.elapsedTime,
          logUrl: payload.logUrl,
          containerExitStatus: payload.containerExitStatus + '',
          containerState: payload.containerState,
          nodeId : payload.nodeId,
          nodeHttpAddress: payload.nodeHttpAddress
        }
      };

      return fixedPayload;
    },

    normalizeSingleResponse(store, primaryModelClass, payload/*, id, requestType*/) {
      var p = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      if (payload && payload.container) {
        if (Array.isArray(payload.container)) {
          // payload has apps : { app: [ {},{},{} ]  }
          // need some error handling for ex apps or app may not be defined.
          normalizedArrayResponse.data = payload.container.map(singleContainer => {
            return this.internalNormalizeSingleResponse(store, primaryModelClass,
              singleContainer);
          }, this);
        } else {
          normalizedArrayResponse.data = [this.internalNormalizeSingleResponse(
            store, primaryModelClass, payload.container)];
        }
        return normalizedArrayResponse;
      } else {
        normalizedArrayResponse.data = [];
      }

      return normalizedArrayResponse;
    }
});
