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
import AbstractRoute from './abstract';
import AppAttemptMixin from 'yarn-ui/mixins/app-attempt';

export default AbstractRoute.extend(AppAttemptMixin, {
  model(param) {
    return Ember.RSVP.hash({
      attempt: this.fetchAttemptInfoFromRMorATS(param.app_attempt_id, this.store),
      rmContainers: this.store.query('yarn-container', {
        app_attempt_id: param.app_attempt_id
      }).catch(function() {
        return Ember.A();
      }),
      tsContainers: this.store.query('yarn-timeline-container', {
        app_attempt_id: param.app_attempt_id
      }).catch(function() {
        return Ember.A();
      })
    });
  },

  unloadAll() {
    this.store.unloadAll('yarn-app-attempt');
    this.store.unloadAll('yarn-timeline-appattempt');
    this.store.unloadAll('yarn-container');
    this.store.unloadAll('yarn-timeline-container');
  }
});
