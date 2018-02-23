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
  breadcrumbs: [{
    text: "Home",
    routeName: 'application'
  }, {
    text: "Services",
    routeName: 'yarn-services',
  }, {
    text: "New Service",
    routeName: 'yarn-deploy-service',
  }],

  savedStandardTemplates: [],
  savedJsonTemplates: [],
  serviceResponse: null,
  isLoading: false,

  actions: {
    deployServiceDef(serviceDef, userName) {
      var defjson = serviceDef.getServiceJSON();
      this.deployServiceApp(defjson, userName);
    },

    deployServiceJson(json, userName) {
      this.deployServiceApp(json, userName);
    }
  },

  gotoServices() {
    Ember.run.later(this, function() {
      this.set('serviceResponse', null);
      this.transitionToRoute('yarn-services');
    }, 1000);
  },

  deployServiceApp(requestJson, userName) {
    var self = this;
    var adapter = this.store.adapterFor('yarn-servicedef');
    this.set('isLoading', true);
    adapter.deployService(requestJson, userName).then(function() {
      self.set('serviceResponse', {message: 'Service has been accepted successfully. Redirecting to services in a second.', type: 'success'});
      self.gotoServices();
    }, function(errmsg) {
      self.set('serviceResponse', {message: errmsg, type: 'error'});
    }).finally(function() {
      self.set('isLoading', false);
    });
  }
});
