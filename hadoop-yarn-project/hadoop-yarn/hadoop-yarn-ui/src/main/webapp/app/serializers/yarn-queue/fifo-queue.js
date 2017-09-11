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

export default DS.JSONAPISerializer.extend({

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName,
        attributes: {
          name: id,
          capacity: payload.capacity * 100,
          usedCapacity: payload.usedCapacity * 100,
          usedNodeCapacity: payload.usedNodeCapacity,
          availNodeCapacity: payload.availNodeCapacity,
          totalNodeCapacity: payload.totalNodeCapacity,
          numNodes: payload.numNodes,
          numContainers: payload.numContainers,
          state: payload.qstate,
          minQueueMemoryCapacity: payload.minQueueMemoryCapacity,
          maxQueueMemoryCapacity: payload.maxQueueMemoryCapacity,
          type: "fifo",
        },

      };

      return this._super(store, primaryModelClass, fixedPayload, id,
        requestType);
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id, requestType) {
      var normalizedArrayResponse = {};
      normalizedArrayResponse.data = [
        this.normalizeSingleResponse(store, primaryModelClass,
          payload.scheduler.schedulerInfo, "root", requestType)
      ];

      return normalizedArrayResponse;
    }
});
