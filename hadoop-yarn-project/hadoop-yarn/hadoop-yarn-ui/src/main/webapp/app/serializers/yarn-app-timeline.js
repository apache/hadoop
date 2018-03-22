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

    var events = payload.events,
        appFinishedEvent = events.findBy('id', 'YARN_APPLICATION_FINISHED'),
        startedTime = payload.createdtime,
        finishedTime = appFinishedEvent? appFinishedEvent.timestamp : Date.now(),
        elapsedTime = finishedTime - startedTime,
        diagnostics = payload.info.YARN_APPLICATION_DIAGNOSTICS_INFO,
        priority = payload.info.YARN_APPLICATION_PRIORITY;

    var fixedPayload = {
      id: id,
      type: primaryModelClass.modelName,
      attributes: {
        appName: payload.info.YARN_APPLICATION_NAME,
        user: payload.info.YARN_APPLICATION_USER,
        queue: "N/A",
        state: payload.info.YARN_APPLICATION_STATE,
        startTime: Converter.timeStampToDate(startedTime),
        elapsedTime: elapsedTime,
        finishedTime: Converter.timeStampToDate(finishedTime),
        finalStatus: payload.info.YARN_APPLICATION_FINAL_STATUS,
        progress: 100,
        applicationType: payload.info.YARN_APPLICATION_TYPE,
        diagnostics: (diagnostics && diagnostics !== 'null')? diagnostics : '',
        amHostHttpAddress: '',
        logAggregationStatus: '',
        unmanagedApplication: payload.info.YARN_APPLICATION_UNMANAGED_APPLICATION || 'N/A',
        amNodeLabelExpression: payload.configs.YARN_AM_NODE_LABEL_EXPRESSION,
        priority: (priority !== undefined)? priority : 'N/A',
        allocatedMB: 0,
        allocatedVCores: 0,
        runningContainers: 0,
        memorySeconds: payload.info.YARN_APPLICATION_MEM_METRIC,
        vcoreSeconds: payload.info.YARN_APPLICATION_CPU_METRIC,
        preemptedResourceMB: 0,
        preemptedResourceVCores: 0,
        numNonAMContainerPreempted: 0,
        numAMContainerPreempted: 0,
        clusterUsagePercentage: 0,
        queueUsagePercentage: 0,
        currentAppAttemptId: payload.info.YARN_APPLICATION_LATEST_APP_ATTEMPT
      }
    };

    return fixedPayload;
  },

  normalizeSingleResponse(store, primaryModelClass, payload, id/*, requestType*/) {
    var p = this.internalNormalizeSingleResponse(store, primaryModelClass, payload, id);
    return {data: p};
  }
});
