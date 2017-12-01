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
      if (payload.app) {
        payload = payload.app;
      }

      var timeoutInSecs = -1;
      var appExpiryTime = Converter.timeStampToDate(payload.finishedTime);
      if (payload.timeouts && payload.timeouts.timeout && payload.timeouts.timeout[0]) {
        timeoutInSecs = payload.timeouts.timeout[0].remainingTimeInSeconds;
        if (timeoutInSecs > -1) {
          appExpiryTime = Converter.isoDateToDate(payload.timeouts.timeout[0].expiryTime);
        }
      }

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          appName: payload.name,
          user: payload.user,
          queue: payload.queue,
          state: payload.state,
          startTime: Converter.timeStampToDate(payload.startedTime),
          elapsedTime: payload.elapsedTime,
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          finalStatus: payload.finalStatus,
          progress: payload.progress,
          applicationType: payload.applicationType,
          diagnostics: (payload.diagnostics && payload.diagnostics !== 'null')? payload.diagnostics : '',
          amContainerLogs: payload.amContainerLogs,
          amHostHttpAddress: payload.amHostHttpAddress,
          logAggregationStatus: payload.logAggregationStatus,
          unmanagedApplication: payload.unmanagedApplication,
          amNodeLabelExpression: payload.amNodeLabelExpression,
          priority: (payload.priority !== undefined)? payload.priority : 'N/A',
          allocatedMB: payload.allocatedMB,
          allocatedVCores: payload.allocatedVCores,
          runningContainers: payload.runningContainers,
          resourceRequests: payload.resourceRequests,
          memorySeconds: payload.memorySeconds,
          vcoreSeconds: payload.vcoreSeconds,
          preemptedResourceMB: payload.preemptedResourceMB,
          preemptedResourceVCores: payload.preemptedResourceVCores,
          numNonAMContainerPreempted: payload.numNonAMContainerPreempted,
          numAMContainerPreempted: payload.numAMContainerPreempted,
          clusterUsagePercentage: payload.clusterUsagePercentage,
          queueUsagePercentage: payload.queueUsagePercentage,
          currentAppAttemptId: payload.currentAppAttemptId,
          remainingTimeoutInSeconds: timeoutInSecs,
          applicationExpiryTime: appExpiryTime
        }
      };

      return fixedPayload;
    },

    normalizeSingleResponse(store, primaryModelClass, payload, id/*, requestType*/) {
      var p = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload, id);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      // payload has apps : { app: [ {},{},{} ]  }
      // need some error handling for ex apps or app may not be defined.
      if(payload.apps && payload.apps.app) {
        normalizedArrayResponse.data = payload.apps.app.map(singleApp => {
          return this.internalNormalizeSingleResponse(store, primaryModelClass,
            singleApp, singleApp.id);
          }, this);
      } else {
        normalizedArrayResponse.data = [];
      }

      return normalizedArrayResponse;
    }
});
