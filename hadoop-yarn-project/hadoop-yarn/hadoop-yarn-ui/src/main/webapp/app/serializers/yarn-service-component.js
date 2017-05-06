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
    var info = payload.info;
    var configs = payload.configs;
    var metrics = payload.metrics;
    var newConfigs = Ember.A();
    var newMetrics = Ember.Object.create();

    if (configs) {
      for (let conf in configs) {
        let confObj = Ember.Object.create({
          name: conf,
          value: configs[conf] || 'N/A'
        });
        newConfigs.push(confObj);
      }
    }

    if (metrics) {
      metrics.forEach(function(metric) {
        let val = metric.values[Object.keys(metric.values)[0]];
        newMetrics.set(metric.id, ((val !== undefined)? val : 'N/A'));
      });
    }

    var fixedPayload = {
      id: 'yarn_service_component_' + payload.id,
      type: primaryModelClass.modelName,
      attributes: {
        name: payload.id,
        vcores: info.RESOURCE_CPU,
        memory: info.RESOURCE_MEMORY,
        priority: 'N/A',
        instances: 'N/A',
        createdTimestamp: payload.createdtime,
        configs: newConfigs,
        metrics: newMetrics
      }
    };

    return fixedPayload;
  },

  normalizeArrayResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedResponse = {data: []};

    if (payload && Array.isArray(payload)) {
      payload.forEach(function(component) {
        var pl = this.internalNormalizeSingleResponse(store, primaryModelClass, component);
        normalizedResponse.data.push(pl);
      }.bind(this));
    }

    return normalizedResponse;
  }
});
