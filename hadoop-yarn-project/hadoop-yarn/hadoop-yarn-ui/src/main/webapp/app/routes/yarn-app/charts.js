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
import AbstractRoute from '../abstract';

export default AbstractRoute.extend({
  model(param, transition) {
    transition.send('updateBreadcrumbs', param.app_id, param.service, [{text: "Charts"}]);
    return Ember.RSVP.hash({
      appId: param.app_id,
      serviceName: param.service,

      app: this.store.find('yarn-app', param.app_id),

      rmContainers: this.store.find('yarn-app', param.app_id).then(function() {
        return this.store.query('yarn-app-attempt', {appId: param.app_id}).then(function (attempts) {
          if (attempts && attempts.get('firstObject')) {
            var appAttemptId = attempts.get('firstObject').get('appAttemptId');
            return this.store.query('yarn-container', {
              app_attempt_id: appAttemptId,
              is_rm: true
            });
          }
        }.bind(this));
      }.bind(this)),

      nodes: this.store.findAll('yarn-rm-node')
    });
  },

  unloadAll() {
    this.store.unloadAll('yarn-app');
    this.store.unloadAll('yarn-app-attempt');
    this.store.unloadAll('yarn-container');
    this.store.unloadAll('yarn-rm-node');
  }
});
