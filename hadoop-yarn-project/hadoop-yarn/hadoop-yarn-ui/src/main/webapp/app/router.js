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
import config from './config/environment';

var Router = Ember.Router.extend({
  location: config.locationType
});

Router.map(function() {
  this.route('yarnApps');
  this.route('yarnNodes');
  this.route('yarnNode', { path: '/yarnNode/:node_id/:node_addr' });
  this.route('yarnNodeApps', { path: '/yarnNodeApps/:node_id/:node_addr' });
  this.route('yarnNodeApp',
      { path: '/yarnNodeApp/:node_id/:node_addr/:app_id' });
  this.route('yarnNodeContainers',
      { path: '/yarnNodeContainers/:node_id/:node_addr' });
  this.route('yarnNodeContainer',
      { path: '/yarnNodeContainer/:node_id/:node_addr/:container_id' });
  this.route('yarnContainerLog', { path:
      '/yarnContainerLog/:node_id/:node_addr/:container_id/:filename' });
  this.route('yarnQueue', { path: '/yarnQueue/:queue_name' });
  this.route('clusterOverview');
  this.route('yarnApp', { path: '/yarnApp/:app_id' });
  this.route('yarnAppAttempt', { path: '/yarnAppAttempt/:app_attempt_id'});
  this.route('error');
  this.route('notfound', { path: '*:' });
});

export default Router;
