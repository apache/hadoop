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
      var events = payload.events,
          appFinishedEvent = events.findBy('id', 'YARN_APPLICATION_FINISHED'),
          finishedTs = appFinishedEvent? appFinishedEvent.timestamp : Date.now(),
          appState = (appFinishedEvent && appFinishedEvent.info)? appFinishedEvent.info.YARN_APPLICATION_STATE : "N/A",
          metrics = payload.metrics,
          cpuMetric = metrics.findBy('id', 'CPU'),
          memoryMetric = metrics.findBy('id', 'MEMORY'),
          cpu = cpuMetric? cpuMetric.values[Object.keys(cpuMetric.values)[0]] : -1,
          memory = memoryMetric? memoryMetric.values[Object.keys(memoryMetric.values)[0]] : -1;

      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName,
        attributes: {
          appId: payload.id,
          type: payload.info.YARN_APPLICATION_TYPE,
          uid: payload.info.UID,
          metrics: metrics,
          startedTs: payload.createdtime,
          finishedTs: finishedTs,
          state: payload.info.YARN_APPLICATION_STATE || appState,
          cpuVCores: cpu,
          memoryUsed: memory
        }
      };

      return this._super(store, primaryModelClass, fixedPayload, id, requestType);
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id, requestType) {
      var normalizedArrayResponse = {};

      normalizedArrayResponse.data = payload.map(singleApp => {
        return this.normalizeSingleResponse(store, primaryModelClass,
          singleApp, singleApp.id, requestType);
      });

      return normalizedArrayResponse;
    }
});
