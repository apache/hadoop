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

export default AbstractRoute.extend({
  model(param) {
    var queueName = param.queue_name? param.queue_name.split('!')[0] : 'root';
    if (param.queue_name && param.queue_name.split('!').length > 1) {
      this.controllerFor('yarn-queues').set('showLoading', false);
    } else {
      this.controllerFor('yarn-queues').set('showLoading', true);
    }
    return Ember.RSVP.hash({
      selected : queueName,
      queues: this.store.query("yarn-queue.yarn-queue", {}).then((model) => {
        let type = model.get('firstObject').get('type');
        return this.store.query("yarn-queue." + type + "-queue", {});
      }),
      selectedQueue : undefined
    });
  },

  afterModel(model) {
    var type = model.queues.get('firstObject').constructor.modelName;
    model.selectedQueue = this.store.peekRecord(type, model.selected);
  },

  unloadAll() {
    this.store.unloadAll('yarn-queue.capacity-queue');
    this.store.unloadAll('yarn-queue.fair-queue');
    this.store.unloadAll('yarn-queue.fifo-queue');
    this.store.unloadAll('yarn-app');
  },

  refreshModel() {
    var param = this.paramsFor('yarn-queues');
    var queueName = param.queue_name? param.queue_name.split('!')[0] : 'root';
    if (queueName !== param.queue_name) {
      this.transitionTo('yarn-queues', queueName);
    } else {
      this.refresh();
    }
  },

  actions: {
    refresh: function () {
      this.unloadAll();
      this.refreshModel();
    },

    loading() {
      if (!this.controllerFor('yarn-queues').get('showLoading')) {
        return false;
      }
      return true;
    }
  }
});
