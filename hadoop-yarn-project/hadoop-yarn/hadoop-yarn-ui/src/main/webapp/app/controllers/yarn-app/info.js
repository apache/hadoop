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

export default Ember.Controller.extend({
  queryParams: ["service"],
  service: undefined,
  isLoading: false,
  actionResponse: null,

  actions: {
    showStopServiceConfirm() {
      this.set('actionResponse', null);
      Ember.$("#stopServiceConfirmDialog").modal('show');
    },

    stopService() {
      var self = this;
      Ember.$("#stopServiceConfirmDialog").modal('hide');
      var adapter = this.store.adapterFor('yarn-servicedef');
      self.set('isLoading', true);
      adapter.stopService(this.get('service'),  this.get('model.app.user')).then(function() {
        self.set('actionResponse', {msg: 'Service stopped successfully. Auto refreshing in 5 seconds.', type: 'success'});
        Ember.run.later(self, function() {
          this.set('actionResponse', null);
          this.send("refresh");
        }, 5000);
      }, function(errr) {
        let messg = errr.diagnostics || 'Error: Stop service failed!';
        self.set('actionResponse', {msg: messg, type: 'error'});
      }).finally(function() {
        self.set('isLoading', false);
      });
    },

    showDeleteServiceConfirm() {
      this.set('actionResponse', null);
      Ember.$("#deleteServiceConfirmDialog").modal('show');
    },

    deleteService() {
      var self = this;
      Ember.$("#deleteServiceConfirmDialog").modal('hide');
      var adapter = this.store.adapterFor('yarn-servicedef');
      self.set('isLoading', true);
      adapter.deleteService(this.get('service'),  this.get('model.app.user')).then(function() {
        self.set('actionResponse', {msg: 'Service deleted successfully. Redirecting to services in 5 seconds.', type: 'success'});
        Ember.run.later(self, function() {
          this.set('actionResponse', null);
          this.transitionToRoute("yarn-services");
        }, 5000);
      }, function(errr) {
        let messg = errr.diagnostics || 'Error: Delete service failed!';
        self.set('actionResponse', {msg: messg, type: 'error'});
      }).finally(function() {
        self.set('isLoading', false);
      });
    },

    resetActionResponse() {
      this.set('actionResponse', null);
    }
  },

  isRunningService: Ember.computed('model.serviceName', 'model.app.state', function() {
    return this.get('service') !== undefined && this.get('model.app.state') === 'RUNNING';
  }),

  amHostHttpAddressFormatted: Ember.computed('model.app.amHostHttpAddress', function() {
    var amHostAddress = this.get('model.app.amHostHttpAddress');
    if (amHostAddress && amHostAddress.indexOf('://') < 0) {
      amHostAddress = 'http://' + amHostAddress;
    }
    return amHostAddress;
  })
});
