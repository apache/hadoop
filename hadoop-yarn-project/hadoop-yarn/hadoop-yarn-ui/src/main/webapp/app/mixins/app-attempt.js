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

export default Ember.Mixin.create({
  fetchAppInfoFromRMorATS(appId, store) {
    return new Ember.RSVP.Promise(function(resolve, reject) {
      store.find('yarn-app', appId).then(function(rmApp) {
        resolve(rmApp);
      }, function() {
        store.find('yarn-app-timeline', appId).then(function(atsApp) {
          resolve(atsApp);
        }, function() {
          console.error('Error:', 'Application not found in RM or ATS for appId: ' + appId);
          reject(null);
        });
      });
    });
  },

  fetchAttemptInfoFromRMorATS(attemptId, store) {
    return new Ember.RSVP.Promise(function(resolve, reject) {
      store.findRecord('yarn-app-attempt', attemptId, {reload: true}).then(function(rmAttempt) {
        resolve(rmAttempt);
      }, function() {
        store.findRecord('yarn-timeline-appattempt', attemptId, {reload: true}).then(function(atsAttempt) {
          resolve(atsAttempt);
        }, function() {
          console.error('Error:', 'Application attempt not found in RM or ATS for attemptId: ' + attemptId);
          reject(null);
        });
      });
    });
  },

  fetchAttemptListFromRMorATS(appId, store) {
    return new Ember.RSVP.Promise(function(resolve, reject) {
      store.query('yarn-app-attempt', {appId: appId}).then(function(rmAttempts) {
        resolve(rmAttempts);
      }, function() {
        store.query('yarn-timeline-appattempt', {appId: appId}).then(function(atsAttempts) {
          resolve(atsAttempts);
        }, function() {
          console.error('Error:', 'Application attempts not found in RM or ATS for appId: ' + appId);
          reject(null);
        });
      });
    });
  }
});
