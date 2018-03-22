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
  currentFileConfig: null,
  fileConfigJson: '',
  fileConfigProps: '',
  propertyViewer: null,
  parseError: '',

  actions: {
    showNewConfigFileModal() {
      var newFile = this.get('serviceDef').createNewFileConfig();
      this.set('currentFileConfig', newFile);
      this.set('fileConfigProps', '');
      this.set('parseError', '');
      this.$('#addFileConfigModal').modal('show');
      if (this.get('isNonEmptyComponents') && this.get('currentFileConfig.componentName') === '') {
        this.set('currentFileConfig.componentName', this.get('componentNames.firstObject'));
      }
    },

    removeFileConfiguration(file) {
      this.get('serviceDef.fileConfigs').removeObject(file);
    },

    addNewFileConfig() {
      this.set('parseError', '');
      var props = this.get('fileConfigProps');
      if (props) {
        try {
          var parsed = JSON.parse(props);
          this.set('currentFileConfig.props', parsed);
        } catch (err) {
          this.set('parseError', `Invalid JSON: ${err.message}`);
          throw err;
        }
      }
      this.get('serviceDef.fileConfigs').addObject(this.get('currentFileConfig'));
      this.$('#addFileConfigModal').modal('hide');
    },

    showFileConfigUploadModal() {
      this.set('fileConfigJson', '');
      this.$("#service_file_config_upload_modal").modal('show');
    },

    uploadFileConfig(json) {
      this.get('serviceDef').convertJsonFileConfigs(json);
      this.$("#service_file_config_upload_modal").modal('hide');
    },

    configScopeChanged(scope) {
      this.set('currentFileConfig.scope', scope);
    },

    scopeComponentChanged(name) {
      this.set('currentFileConfig.componentName', name);
    },

    configTypeChanged(type) {
      this.set('currentFileConfig.type', type);
      if (type === "TEMPLATE") {
        this.set('currentFileConfig.props', null);
        this.set('fileConfigProps', '');
      }
    },

    showFileConfigPropertyViewer(props) {
      this.set('propertyViewer', props);
      this.$("#file_config_properties_viewer").modal('show');
    }
  },

  isNonEmptyComponents: Ember.computed('serviceDef.serviceComponents.length', function() {
    return this.get('serviceDef.serviceComponents.length') > 0;
  }),

  componentNames: Ember.computed('serviceDef.serviceComponents.[]', function() {
    var names = [];
    this.get('serviceDef.serviceComponents').forEach(function(cmp) {
      names.push(cmp.get('name'));
    });
    return names;
  }),

  isValidCurrentFileConfig: Ember.computed('currentFileConfig', 'currentFileConfig.srcFile', 'currentFileConfig.destFile', 'fileConfigProps', function() {
    return this.get('currentFileConfig') && this.get('currentFileConfig.destFile') && (this.get('currentFileConfig.srcFile') || this.get('fileConfigProps'));
  }),

  isConfigTypeHadoopXml: Ember.computed('currentFileConfig.type', function() {
    return this.get('currentFileConfig.type') === 'HADOOP_XML';
  })
});
