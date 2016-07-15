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
    internalNormalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      
      if (payload.appAttempt) {
        payload = payload.appAttempt;  
      }
      
      var fixedPayload = {
        id: payload.appAttemptId,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          startTime: Converter.timeStampToDate(payload.startTime),
          startedTime: Converter.timeStampToDate(payload.startedTime),
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          containerId: payload.containerId,
          amContainerId: payload.amContainerId,
          nodeHttpAddress: payload.nodeHttpAddress,
          nodeId: payload.nodeId,
          hosts: payload.host,
          state: payload.appAttemptState,
          logsLink: payload.logsLink,
          appAttemptId: payload.appAttemptId
        }
      };

      return fixedPayload;
    },

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var p = this.internalNormalizeSingleResponse(store, 
        primaryModelClass, payload, id, requestType);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      if (payload.appAttempts && payload.appAttempts.appAttempt) {
        // payload has apps : { app: [ {},{},{} ]  }
        // need some error handling for ex apps or app may not be defined.
        normalizedArrayResponse.data = payload.appAttempts.appAttempt.map(singleApp => {
          return this.internalNormalizeSingleResponse(store, primaryModelClass,
            singleApp, singleApp.id, requestType);
        }, this);
      } else {
        normalizedArrayResponse.data = [];
      }
      return normalizedArrayResponse;
    }
});