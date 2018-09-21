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
  internalNormalizeSingleResponse(store, primaryModelClass, payload) {
    var info = payload.info;

    var fixedPayload = {
      id: 'yarn_component_instance_' + payload.id,
      type: primaryModelClass.modelName,
      attributes: {
        containerId: payload.id,
        component: info.COMPONENT_NAME,
        instanceName: info.COMPONENT_INSTANCE_NAME,
        state: info.STATE,
        createdTimestamp: payload.createdtime,
        startedTimestamp: info.LAUNCH_TIME,
        host: info.HOSTNAME,
        node: info.BARE_HOST,
        ipAddr: info.IP,
        exitStatusCode: info.EXIT_STATUS_CODE
      }
    };

    return fixedPayload;
  },

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedResponse = {data: []};
    var instanceUid = {};

    if (payload && Array.isArray(payload)) {
      this.sortPayloadByCreatedTimeAscending(payload);

      payload.forEach(function(container) {
        let componentName = container.info.COMPONENT_NAME;
        if (!instanceUid[componentName]) {
          instanceUid[componentName] = 0;
        }
        container.instanceId = ++instanceUid[componentName];
        var pl = this.internalNormalizeSingleResponse(store, primaryModelClass, container);
        normalizedResponse.data.push(pl);
      }.bind(this));
    }

    return normalizedResponse;
  },

  sortPayloadByCreatedTimeAscending(payload) {
    payload.sort(function(inst1, inst2) {
      return inst1.createdtime - inst2.createdtime;
    });
  }
});
