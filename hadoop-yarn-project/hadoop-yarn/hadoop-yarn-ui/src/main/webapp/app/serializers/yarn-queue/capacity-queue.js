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
      if (payload.queues) {
        payload.queues.queue.forEach(function(queue) {
          children.push(queue.queueName);
        });
      }

      var includedData = [];
      var relationshipUserData = [];

      // update user models
      if (payload.users && payload.users.user) {
        payload.users.user.forEach(function(u) {
          includedData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queueName,
            attributes: {
              name: u.username,
              queueName: payload.queueName,
              usedMemoryMB: u.resourcesUsed.memory || 0,
              usedVCore: u.resourcesUsed.vCores || 0,
            }
          });

          relationshipUserData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queueName,
          });
        });
      }

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-queue
        attributes: {
          name: payload.queueName,
          parent: payload.myParent,
          children: children,
          capacity: payload.capacity,
          usedCapacity: payload.usedCapacity,
          maxCapacity: payload.maxCapacity,
          absCapacity: payload.absoluteCapacity,
          absMaxCapacity: payload.absoluteMaxCapacity,
          absUsedCapacity: payload.absoluteUsedCapacity,
          state: payload.state,
          userLimit: payload.userLimit,
          userLimitFactor: payload.userLimitFactor,
          preemptionDisabled: payload.preemptionDisabled,
          numPendingApplications: payload.numPendingApplications,
          numActiveApplications: payload.numActiveApplications,
          resources: payload.resources,
          type: "capacity",
        },
        // Relationships
        relationships: {
          users: {
            data: relationshipUserData
          }
        }
      };
      return {
        queue: this._super(store, primaryModelClass, fixedPayload, id, requestType),
        includedData: includedData
      };
    },

    handleQueue(store, primaryModelClass, payload, id, requestType) {
      var data = [];
      var includedData = [];
      var result = this.normalizeSingleResponse(store, primaryModelClass,
        payload, id, requestType);

      data.push(result.queue);
      includedData = includedData.concat(result.includedData);

      if (payload.queues) {
        for (var i = 0; i < payload.queues.queue.length; i++) {
          var queue = payload.queues.queue[i];
          queue.myParent = payload.queueName;
          var childResult = this.handleQueue(store, primaryModelClass, queue,
            queue.queueName,
            requestType);

          data = data.concat(childResult.data);
          includedData = includedData.concat(childResult.includedData);
        }
      }

      return {
        data: data,
        includedData: includedData
      };
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id, requestType) {
      var normalizedArrayResponse = {};
      var result = this.handleQueue(store, primaryModelClass,
        payload.scheduler.schedulerInfo, "root", requestType);

      normalizedArrayResponse.data = result.data;
      normalizedArrayResponse.included = result.includedData;

      return normalizedArrayResponse;
    }
});
