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
  internalNormalizeSingleResponse(store, primaryModelClass, payload, containerId, nodeId) {
    var fixedPayload = {
      id: "yarn_log_" + payload.fileName + "_" + Date.now(),
      type: primaryModelClass.modelName,
      attributes: {
        fileName: payload.fileName,
        fileSize: payload.fileSize,
        lastModifiedTime: payload.lastModifiedTime,
        redirectedUrl: payload.redirectedUrl,
        containerId: containerId,
        nodeId: nodeId
      }
    };
    return fixedPayload;
  },

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedArrayResponse = {
      data: []
    };
    // If JSON payload is an object with a containerLogsInfo property
    if (payload && payload.containerLogsInfo && payload.containerLogsInfo.containerLogInfo) {
      normalizedArrayResponse.data = payload.containerLogsInfo.containerLogInfo.map((signle_payload) => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass, signle_payload,
          payload.containerLogsInfo.containerId, payload.containerLogsInfo.nodeId);
      });
    }
    // If JSON payload is an array
    if (payload && payload[0] && payload[0].containerLogInfo) {
      normalizedArrayResponse.data = payload[0].containerLogInfo.map((signle_payload) => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass, signle_payload,
          payload[0].containerId, payload[0].nodeId);
      });
    }
    return normalizedArrayResponse;
  }
});
