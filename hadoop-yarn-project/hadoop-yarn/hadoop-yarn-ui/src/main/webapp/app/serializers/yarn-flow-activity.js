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

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-timeline-flow
        attributes: {
          cluster: payload.info.SYSTEM_INFO_CLUSTER,
          flowName: payload.info.SYSTEM_INFO_FLOW_NAME,
          lastExecDate: Converter.timeStampToDateOnly(payload.info.SYSTEM_INFO_DATE),
          user: payload.info.SYSTEM_INFO_USER,
          flowruns: payload.flowruns,
          uid: payload.info.UID
        }
      };

      return this._super(store, primaryModelClass, fixedPayload, id,
        requestType);
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      normalizedArrayResponse.data = payload.map(singleEntity => {
        return this.normalizeSingleResponse(store, primaryModelClass,
          singleEntity, singleEntity.id, requestType);
      }, this);
      return normalizedArrayResponse;
    }
});
