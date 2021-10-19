/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
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
import AppAttemptMixin from 'yarn-ui/mixins/app-attempt';

export default AbstractRoute.extend(AppAttemptMixin, {
  model(param, transition) {
    const { app_id } = this.paramsFor('yarn-app');
    const { service } = param;
    transition.send('updateBreadcrumbs', app_id, service, [{text: 'Threaddump'}]);
    return Ember.RSVP.hash({
      appId: app_id,
      serviceName: service,
      attempts: this.fetchAttemptListFromRM(app_id, this.store)
        .catch(function(error) {
          Ember.Logger.log("App attempt list fetch failed!");
          Ember.Logger.log(error);
          return [];
        }),
      app: this.fetchAppInfoFromRM(app_id, this.store),
      userInfo: this.store.findAll('cluster-user-info', {reload: true})
        .catch(function(error) {
            Ember.Logger.log("userInfo querying failed");
            Ember.Logger.log(error);
            return null;
        })
    });
  },

  activate() {
    const controller = this.controllerFor("yarn-app.threaddump");
    const { attempt, containerid } = this.paramsFor('yarn-app.threaddump');
    controller.resetAfterRefresh();
    controller.initializeSelect();
    if (attempt) {
      controller.send("showContainersForAttemptId", attempt, containerid);
    } else {
      controller.set("selectedAttemptId", "");
    }
  },

  unloadAll() {
    this.store.unloadAll('yarn-app-attempt');
    this.store.unloadAll('yarn-container');
    this.store.unloadAll('yarn-node-container-log');
    this.store.unloadAll('cluster-user-info');
    if (this.controller) {
      this.controller.resetAfterRefresh();
    }
  }
});
