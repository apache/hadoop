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
  classNames: ['pull-right'],

  targetId: '',
  initialClosedState: false,

  didInsertElement() {
    if (!this.get('targetId')) {
      this.$('.toggle_switch').hide();
    }
    if (this.get('targetId') && this.get('initialClosedState')) {
      this.$('.toggle_switch').show();
      this.toggleToggleSwitchArrow();
      Ember.$('#' + this.get('targetId')).removeClass('panel-collapsed').show();
    }
  },

  toggleToggleSwitchArrow() {
    let $toggleArrow = this.$('.toggle_switch').find('span');
    if ($toggleArrow.hasClass('glyphicon-chevron-up')) {
      $toggleArrow.removeClass('glyphicon-chevron-up').addClass('glyphicon-chevron-down');
    } else {
      $toggleArrow.removeClass('glyphicon-chevron-down').addClass('glyphicon-chevron-up');
    }
  },

  toggleCollapsiblePanel() {
    let $collapsiblePanel = Ember.$('#' + this.get('targetId'));
    if ($collapsiblePanel.hasClass('panel-collapsed')) {
      $collapsiblePanel.removeClass('panel-collapsed');
      $collapsiblePanel.slideDown();
    } else {
      $collapsiblePanel.addClass('panel-collapsed');
      $collapsiblePanel.slideUp();
    }
  },

  actions: {
    togglePanelCollapse() {
      this.toggleToggleSwitchArrow();
      this.toggleCollapsiblePanel();
    }
  }
});
