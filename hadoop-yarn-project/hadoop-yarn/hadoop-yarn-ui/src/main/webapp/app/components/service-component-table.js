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
  currentComponent: null,
  duplicateNameError: false,

  actions: {
    showAddComponentModal() {
      var newComp = this.get('serviceDef').createNewServiceComponent();
      this.set('currentComponent', newComp);
      this.set('duplicateNameError', false);
      this.$('#addComponentModal').modal('show');
    },

    addNewComponent() {
      this.set('duplicateNameError', false);
      if (this.isCurrentNameDuplicate()) {
        this.set('duplicateNameError', true);
        return;
      }
      this.get('serviceDef.serviceComponents').addObject(this.get('currentComponent'));
      this.$('#addComponentModal').modal('hide');
    },

    removeComponent(component) {
      this.get('serviceDef.serviceComponents').removeObject(component);
    }
  },

  isCurrentNameDuplicate() {
    var currName = this.get('currentComponent.name');
    var item = this.get('serviceDef.serviceComponents').findBy('name', currName);
    return !Ember.isNone(item);
  },

  isValidCurrentComponent: Ember.computed.and('currentComponent', 'currentComponent.name', 'currentComponent.cpus', 'currentComponent.memory', 'currentComponent.numOfContainers', 'currentComponent.launchCommand')
});
