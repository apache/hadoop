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
      var children = [];
      if (payload.childQueues) {
        payload.childQueues.queue.forEach(function(queue) {
          children.push(queue.queueName);
        });
      }

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName,
        attributes: {
          name: payload.queueName,
          parent: payload.myParent,
          children: children,
          maxApps: payload.maxApps,
          minResources: payload.minResources,
          maxResources: payload.maxResources,
          usedResources: payload.usedResources,
          demandResources: payload.demandResources,
          steadyFairResources: payload.steadyFairResources,
          fairResources: payload.fairResources,
          clusterResources: payload.clusterResources,
          pendingContainers: payload.pendingContainers,
          allocatedContainers: payload.allocatedContainers,
          reservedContainers: payload.reservedContainers,
          schedulingPolicy: payload.schedulingPolicy,
          preemptable: payload.preemptable,
          numPendingApplications: payload.numPendingApps,
          numActiveApplications: payload.numActiveApps,
          type: "fair",
        },
      };
      return this._super(store, primaryModelClass, fixedPayload, id, requestType);
    },

    handleQueue(store, primaryModelClass, payload, id, requestType) {
      var data = [];
      var includedData = [];
      if(!payload) return data;
      var result = this.normalizeSingleResponse(store, primaryModelClass,
        payload, id, requestType);

      data.push(result);

      if (payload.childQueues) {
        for (var i = 0; i < payload.childQueues.queue.length; i++) {
          var queue = payload.childQueues.queue[i];
          queue.myParent = payload.queueName;
          var childResult = this.handleQueue(store, primaryModelClass, queue,
            queue.queueName,
            requestType);

          data = data.concat(childResult);
        }
      }

      return data;
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id, requestType) {
      var normalizedArrayResponse = {};
      var result = this.handleQueue(store, primaryModelClass,
        payload.scheduler.schedulerInfo.rootQueue, "root", requestType);

      normalizedArrayResponse.data = result;
      return normalizedArrayResponse;
    }
});
