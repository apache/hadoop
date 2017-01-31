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
import Constants from 'yarn-ui/constants';

import AbstractRoute from './abstract';

export default AbstractRoute.extend({
  model(param) {
    var id = param.node_addr + Constants.PARAM_SEPARATOR + param.container_id +
        Constants.PARAM_SEPARATOR + param.filename;
    return Ember.RSVP.hash({
      containerLog: this.store.findRecord('yarn-container-log', id),
      containerInfo: { id: param.container_id },
      nodeInfo: { id: param.node_id, addr: param.node_addr }
    }).then(function(hash) {
      // Just return as its success.
      return hash;
    }, function(reason) {
      if (reason.errors && reason.errors[0]) {
        // This means HTTP error response was sent by adapter.
        return reason;
      } else {
        // Assume empty response received from server.
        return { nodeInfo: { id: param.node_id, addr: param.node_addr },
            containerInfo: { id: param.container_id },
            containerLog: { logs: "", containerID: param.container_id,
                logFileName: param.filename}};
      }
    });
  },

  afterModel(model) {
    // Handle errors and redirect if promise is rejected.
    if (model.errors && model.errors[0]) {
      if (parseInt(model.errors[0].status) === 404) {
        this.replaceWith('/notfound');
      } else {
        this.replaceWith('/error');
      }
    }
  },

  unloadAll() {
    this.store.unloadAll('yarn-container-log');
  }
});
