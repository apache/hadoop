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
    if (path === 'yarn-queue') {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarn-queue/root">Queues<span class="sr-only">' +
        '(current)</span></a></li><li';
    if (path.lastIndexOf('yarn-app', 0) === 0) {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarn-apps">Applications<span class="sr-only">' +
        '(current)</span></a></li><li';
    if (path === 'cluster-overview') {
      html = html + ' class="active"';
    }
    html = html + '><a href="cluster-overview">Cluster Overview<span class=' +
        '"sr-only">(current)</span></a></li><li';
    if (path.lastIndexOf('yarn-node', 0) === 0) {
      html = html + ' class="active"';
    }
    html = html + '><a href="yarn-nodes">Nodes<span class="sr-only">' +
        '(current)</span></a></li>';
    return Ember.String.htmlSafe(html);
  }.property('currentPath'),

  isQueuesTabActive: function() {
    var path = this.get('currentPath');
    if (path === 'yarn-queues') {
      return true;
    }
    return false;
  }.property('currentPath'),

  clusterInfo: function() {
    if (this.model && this.model.clusterInfo) {
      return this.model.clusterInfo.get('firstObject');
    }
    return null;
  }.property('model.clusterInfo'),

  userInfo: function() {
    if (this.model && this.model.userInfo) {
      return this.model.userInfo.get('firstObject');
    }
    return null;
  }.property('model.userInfo'),
});
