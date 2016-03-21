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

/**
 * Base controller for application.
 */
export default Ember.Controller.extend({
  /**
   * Output main top UI menu which is common across all pages.
   * Menu item will be made active based on current path.
   */
  outputMainMenu: function(){
    var path = this.get('currentPath');
    var html = '<li';
    if (path == 'yarnQueue') {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarnQueue/root">Queues<span class="sr-only">' +
        '(current)</span></a></li><li';
    if (path.lastIndexOf('yarnApp', 0) == 0) {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarnApps">Applications<span class="sr-only">' +
        '(current)</span></a></li><li';
    if (path == 'clusterOverview') {
      html = html + ' class="active"';
    }
    html = html + '><a href="clusterOverview">Cluster Overview<span class=' +
        '"sr-only">(current)</span></a></li><li';
    if (path.lastIndexOf('yarnNode', 0) == 0) {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarnNodes">Nodes<span class="sr-only">' +
        '(current)</span></a></li>';
    return Ember.String.htmlSafe(html);
  }.property('currentPath')
});

