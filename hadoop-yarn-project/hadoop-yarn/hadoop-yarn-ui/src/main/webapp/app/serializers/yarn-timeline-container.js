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
import Ember from 'ember';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
  internalNormalizeSingleResponse(store, primaryModelClass, payload) {

    var fixedPayload = {
      id: payload.id,
      type: primaryModelClass.modelName,
      attributes: {
        allocatedMB: payload.info.YARN_CONTAINER_ALLOCATED_MEMORY,
        allocatedVCores: payload.info.YARN_CONTAINER_ALLOCATED_VCORE,
        assignedNodeId: payload.info.YARN_CONTAINER_ALLOCATED_HOST,
        priority: payload.info.YARN_CONTAINER_ALLOCATED_PRIORITY,
        startedTime:  Converter.timeStampToDate(payload.createdtime),
        finishedTime: Converter.timeStampToDate(payload.info.YARN_CONTAINER_FINISHED_TIME),
        nodeHttpAddress: payload.info.YARN_CONTAINER_ALLOCATED_HOST_HTTP_ADDRESS,
        exposedPorts: payload.info.YARN_CONTAINER_ALLOCATED_EXPOSED_PORTS,
        containerExitStatus: payload.info.YARN_CONTAINER_EXIT_STATUS + '',
        containerState: payload.info.YARN_CONTAINER_STATE,
        nodeId: payload.info.YARN_CONTAINER_ALLOCATED_HOST + ':' + payload.info.YARN_CONTAINER_ALLOCATED_PORT,
        diagnosticsInfo: payload.info.YARN_CONTAINER_DIAGNOSTICS_INFO
      }
    };
    return fixedPayload;
  },

  normalizeSingleResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalized = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload);
    return {
      data: normalized
    };
  },

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedArrayResponse = {
      data: []
    };
    if (payload && Ember.isArray(payload) && !Ember.isEmpty(payload)) {
      normalizedArrayResponse.data = payload.map(singleContainer => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass,
          singleContainer);
      });
    }
    return normalizedArrayResponse;
  }
});
