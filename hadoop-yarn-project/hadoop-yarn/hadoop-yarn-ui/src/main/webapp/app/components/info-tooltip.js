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
import InfoSeeder from 'yarn-ui/utils/info-seeder';

export default Ember.Component.extend({
  classNames: ['tooltip', 'info-tooltip'],
  elementId: 'info_tooltip_wrapper',

  didInsertElement() {
    var $tooltip = Ember.$('#info_tooltip_wrapper');
    Ember.$('body').on('mouseenter', '.info-icon', function() {
      var $elem = Ember.$(this);
      var info = InfoSeeder[$elem.data('info')];
      var offset = $elem.offset();
      $tooltip.show();
      $tooltip.find("#tooltip_content").text(info);
      $tooltip.offset({top: offset.top + 20, left: offset.left - 10});
    }).on('mouseleave', '.info-icon', function() {
      $tooltip.find("#tooltip_content").text('');
      $tooltip.hide();
    });
  },

  WillDestroyElement() {
    Ember.$('body').off('hover');
  }
});
