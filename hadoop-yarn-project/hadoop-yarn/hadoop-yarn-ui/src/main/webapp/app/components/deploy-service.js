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
  viewType: 'standard',
  savedStandardTemplates: null,
  savedJsonTemplates: null,
  savedTemplateName: '',
  serviceDef: null,
  customServiceDef: '',
  serviceResp: null,
  isLoading: false,

  actions: {
    showSaveTemplateModal() {
      this.$('#saveListModal').modal('show');
    },

    deployService() {
      this.set('serviceResp', null);
      if (this.get('isStandardViewType')) {
        this.sendAction("deployServiceDef", this.get('serviceDef'));
      } else {
        try {
          var parsed = JSON.parse(this.get('customServiceDef'));
          this.sendAction("deployServiceJson", parsed);
        } catch (err) {
          this.set('serviceResp', {type: 'error', message: 'Invalid JSON: ' + err.message});
          throw err;
        }
      }
    },

    updateViewType(type) {
      this.set('viewType', type);
    },

    addToSavedList() {
      this.unselectAllSavedList();
      if (this.get('isStandardViewType')) {
        this.get('savedStandardTemplates').addObject({
          name: this.get('savedTemplateName'),
          defId: this.get('serviceDef.id'),
          active: true
        });
        this.set('serviceDef.isCached', true);
      } else {
        this.get('savedJsonTemplates').addObject({
          name: this.get('savedTemplateName'),
          custom: this.get('customServiceDef'),
          active: true
        });
      }
      this.$('#saveListModal').modal('hide');
      this.set('savedTemplateName', '');
    },

    updateServiceDef(def) {
      this.selectActiveListItem(def);
      if (this.get('isStandardViewType')) {
        this.set('serviceDef', this.getStore().peekRecord('yarn-servicedef', def.defId));
      } else {
        this.set('customServiceDef', def.custom);
      }
    },

    clearConfigs() {
      this.unselectAllSavedList();
      this.set('serviceResp', null);
      if (this.get('isStandardViewType')) {
        var oldDef = this.get('serviceDef');
        var def = oldDef.createNewServiceDef();
        this.set('serviceDef', def);
        if (!oldDef.get('isCached')) {
          oldDef.deleteRecord();
        }
      } else {
        this.set('customServiceDef', '');
      }
    },

    removeFromSavedList(list) {
      if (list.active) {
        this.send('clearConfigs');
      }
      if (this.get('isStandardViewType')) {
        this.get('savedStandardTemplates').removeObject(list);
      } else {
        this.get('savedJsonTemplates').removeObject(list);
      }
    },

    clearServiceResponse() {
      this.set('serviceResp', null);
    }
  },

  didInsertElement() {
    var self = this;
    self.$().find('.modal').on('shown.bs.modal', function() {
      self.$().find('.modal.in').find('input.form-control:first').focus();
    });
  },

  selectActiveListItem(item) {
    this.unselectAllSavedList();
    Ember.set(item, 'active', true);
  },

  unselectAllSavedList() {
    this.get('getSavedList').forEach(function(item) {
      Ember.set(item, 'active', false);
    });
  },

  getSavedList: Ember.computed('viewType', function() {
    if (this.get('isStandardViewType')) {
      return this.get('savedStandardTemplates');
    } else {
      return this.get('savedJsonTemplates');
    }
  }),

  getStore: function() {
    return this.get('serviceDef.store');
  },

  isStandardViewType: Ember.computed.equal('viewType', 'standard'),

  isCustomViewType: Ember.computed.equal('viewType', 'custom'),

  isValidTemplateName: Ember.computed.notEmpty('savedTemplateName'),

  isValidServiceDef: Ember.computed('serviceDef.name', 'serviceDef.queue', 'serviceDef.serviceComponents.[]', function () {
    return this.get('serviceDef').isValidServiceDef();
  }),

  isValidCustomServiceDef: Ember.computed.notEmpty('customServiceDef'),

  enableSaveOrDeployBtn: Ember.computed('isValidServiceDef', 'isValidCustomServiceDef', 'viewType', 'isLoading', function() {
    if (this.get('isLoading')) {
      return false;
    }
    if (this.get('isStandardViewType')) {
      return this.get('isValidServiceDef');
    } else {
      return this.get('isValidCustomServiceDef');
    }
  })
});
