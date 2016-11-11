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

export default DS.JSONAPISerializer.extend({
  internalNormalizeSingleResponse(store, primaryModelClass, payload) {
    if (payload.container) {
      payload = payload.container;
    }
    var fixedPayload = {
      id: payload.id,
      type: primaryModelClass.modelName,
      attributes: {
        containerId: payload.id,
        state: payload.state,
        user: payload.user,
        diagnostics: payload.diagnostics,
        exitCode: payload.exitCode,
        totalMemoryNeeded: payload.totalMemoryNeededMB,
        totalVCoresNeeded: payload.totalVCoresNeeded,
        containerLogFiles: payload.containerLogFiles
      }
    };

    return fixedPayload;
  },

  normalizeSingleResponse(store, primaryModelClass, payload, id,
    requestType) {
    // payload is of the form {"container":{}}
    var p = this.internalNormalizeSingleResponse(store,
        primaryModelClass, payload);
    return { data: p };
  },

  normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
    // expected return response is of the form { data: [ {}, {} ] }
    var normalizedArrayResponse = {};
    if (payload.containers && payload.containers.container) {
      // payload is of the form { "containers" : { "container": [ {},{},{} ]  } }
      normalizedArrayResponse.data =
          payload.containers.container.map(singleContainer => {
            return this.internalNormalizeSingleResponse(store, primaryModelClass,
                singleContainer);
          }, this);
    } else {
      // No container reported inside containers.
      // Response of the form { "containers": null }
      normalizedArrayResponse.data = [];
    }
    return normalizedArrayResponse;
  }
});
