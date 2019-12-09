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
      var recordId = id;
      if (!recordId) {
        recordId = payload.id;
      }
      var fixedPayload = {
        id: recordId,
        type: primaryModelClass.modelName,
        attributes: {
          flowName: payload.info.SYSTEM_INFO_FLOW_NAME,
          runid: payload.info.SYSTEM_INFO_FLOW_RUN_ID,
          shownid: payload.id,
          type: payload.type,
          createTime: this.checkDateValidity(payload.createdtime),
          endTime: this.checkDateValidity(payload.info.SYSTEM_INFO_FLOW_RUN_END_TIME),
          user: payload.info.SYSTEM_INFO_USER,
          metrics: payload.metrics,
        }
      };

      return fixedPayload;
    },

    checkDateValidity(timestamp) {
      if (timestamp && timestamp > 0) {
        return Converter.timeStampToDate(timestamp);
      }
      return 'N/A';
    },

    normalizeSingleResponse(store, primaryModelClass, payload, id/*, requestType*/) {
      var p = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload, id);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      normalizedArrayResponse.data = payload.map(singleApp => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass,
          singleApp, singleApp.id);
      }, this);
      return normalizedArrayResponse;
    }
});
