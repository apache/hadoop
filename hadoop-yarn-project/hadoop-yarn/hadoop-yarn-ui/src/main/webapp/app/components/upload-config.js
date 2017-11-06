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
  dialogId: "config_upload_modal",
  title: "Upload Configuration",
  configJson: '',
  parseErrorMsg: '',

  actions: {
    uploadConfig() {
      var json = this.get('configJson');
      try {
        JSON.parse(json);
        this.upateParseResults("");
      } catch (ex) {
        this.upateParseResults("Invalid JSON: " + ex.message);
        throw ex;
      }
      if (!this.get('parseErrorMsg')) {
        this.sendAction("uploadConfig", json);
      }
    }
  },

  didInsertElement() {
    this.$('#' + this.get('dialogId')).on('shown.bs.modal', function() {
      this.upateParseResults("");
    }.bind(this));
  },

  isValidConfigJson: Ember.computed.notEmpty('configJson'),

  upateParseResults(message) {
    this.set('parseErrorMsg', message);
  }
});
