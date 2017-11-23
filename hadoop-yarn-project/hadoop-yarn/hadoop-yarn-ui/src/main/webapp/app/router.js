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
  this.route('yarn-apps', function () {
    this.route('apps');
  });
  this.route('yarn-services');
  this.route('yarn-nodes', function(){
    this.route('table');
    this.route('heatmap');
  });
  this.route('yarn-queue', {path: '/yarn-queue/:queue_name'}, function() {
    this.route('info');
    this.route('apps');
  });
  this.route('yarn-nodes-heatmap');
  this.route('yarn-node', { path: '/yarn-node/:node_id/:node_addr' }, function() {
    this.route("info");
    this.route("yarn-nm-gpu");
  });
  this.route('yarn-node-apps', { path: '/yarn-node-apps/:node_id/:node_addr' });
  this.route('yarn-node-app',
      { path: '/yarn-node-app/:node_id/:node_addr/:app_id' });
  this.route('yarn-node-containers',
      { path: '/yarn-node-containers/:node_id/:node_addr' });
  this.route('yarn-node-container',
      { path: '/yarn-node-container/:node_id/:node_addr/:container_id' });
  this.route('yarn-container-log', { path:
      '/yarn-container-log/:node_id/:node_addr/:container_id/:filename' });

  this.route('cluster-overview');
  this.route('yarn-app', function() {
    this.route('info', {path: '/:app_id/info'});
    this.route('attempts', {path: '/:app_id/attempts'});
    this.route('charts', {path: '/:app_id/charts'});
  });
  this.route('yarn-app-attempt', { path: '/yarn-app-attempt/:app_attempt_id'});
  this.route('error');
  this.route('notfound', { path: '*:' });
  this.route('yarn-queues', { path: '/yarn-queues/:queue_name' });

  this.route('yarn-flow-activity');
  this.route('yarn-flow', { path: '/yarn-flow/:flow_uid'}, function() {
    this.route('info');
    this.route('runs');
  });
  this.route('yarn-flowrun', { path: '/yarn-flowrun/:flowrun_uid'}, function() {
    this.route('info');
    this.route('metrics');
  });
  this.route('yarn-flowrun-metric', { path: '/yarn-flowrun-metric/:flowrun_uid/:metric_id'});
  this.route('timeline-error', {path: 'timeline-error/:error_id'});
});

export default Router;
