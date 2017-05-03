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

import Ember from 'ember';

export default Ember.Component.extend({
  serviceDef: null,
  currentConfig: null,
  serviceConfigJson: '',

  actions: {
    showNewConfigurationModal() {
      var newConfig = this.get('serviceDef').createNewServiceConfig();
      this.set('currentConfig', newConfig);
      this.$('#addConfigurationModal').modal('show');
      if (this.get('isNonEmptyComponents') && this.get('currentConfig.componentName') === '') {
        this.set('currentConfig.componentName', this.get('componentNames.firstObject'));
      }
    },

    removeConfiguration(config) {
      this.get('serviceDef.serviceConfigs').removeObject(config);
    },

    configTypeChanged(type) {
      this.set('currentConfig.type', type);
      if (type === 'quicklink') {
        this.set('currentConfig.scope', 'service');
        this.set('currentConfig.componentName', '');
      }
    },

    addNewConfiguration() {
      this.get('serviceDef.serviceConfigs').addObject(this.get('currentConfig'));
      this.$('#addConfigurationModal').modal('hide');
    },

    showServiceConfigUploadModal() {
      this.set('serviceConfigJson', '');
      this.$("#service_config_upload_modal").modal('show');
    },

    uploadServiceConfig(json) {
      this.get('serviceDef').convertJsonServiceConfigs(json);
      this.$("#service_config_upload_modal").modal('hide');
    },

    configScopeChanged(scope) {
      this.set('currentConfig.scope', scope);
    },

    scopeComponentChanged(name) {
      this.set('currentConfig.componentName', name);
    }
  },

  isNonEmptyComponents: Ember.computed('serviceDef.serviceComponents.length', function() {
    return this.get('serviceDef.serviceComponents.length') > 0;
  }),

  isNotQuicklink: Ember.computed('currentConfig.type', function() {
    return this.get('currentConfig.type') !== "quicklink";
  }),

  componentNames: Ember.computed('serviceDef.serviceComponents.[]', function() {
    var names = [];
    this.get('serviceDef.serviceComponents').forEach(function(cmp) {
      names.push(cmp.get('name'));
    });
    return names;
  }),

  isValidCurrentConfig: Ember.computed.and('currentConfig', 'currentConfig.name', 'currentConfig.value')
});
