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
    var quicklinks = info.QUICK_LINKS;
    var metrics = payload.metrics;
    var newConfigs = Ember.A();
    var newQuicklinks = Ember.A();
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

    if (quicklinks) {
      for (let link in quicklinks) {
        let linkObj = Ember.Object.create({
          name: link,
          value: quicklinks[link] || 'N/A'
        });
        newQuicklinks.push(linkObj);
      }
    }

    if (metrics) {
      metrics.forEach(function(metric) {
        let val = metric.values[Object.keys(metric.values)[0]];
        newMetrics.set(metric.id, ((val !== undefined)? val : 'N/A'));
      });
    }

    var fixedPayload = {
      id: 'yarn_service_info_' + payload.id,
      type: primaryModelClass.modelName,
      attributes: {
        name: info.NAME,
        appId: payload.id,
        state: info.STATE,
        createdTimestamp: payload.createdtime,
        launchTimestamp: info.LAUNCH_TIME,
        quicklinks: newQuicklinks,
        configs: newConfigs,
        metrics: newMetrics
      }
    };

    return fixedPayload;
  },

  normalizeSingleResponse(store, primaryModelClass, payload/*, id, requestType*/) {
    var normalizedResponse = {data: []};

    if (payload && payload[0]) {
      var pl = this.internalNormalizeSingleResponse(store, primaryModelClass, payload[0]);
      normalizedResponse.data = pl;
    }

    return normalizedResponse;
  }
});
