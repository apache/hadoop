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
    // Fetches data from both NM and RM. RM is queried to get node usage info.
    return Ember.RSVP.hash({
      nodeInfo: { id: param.node_id, addr: param.node_addr },
      node: this.store.findRecord('yarn-node', param.node_addr),
      rmNode: this.store.findRecord('yarn-rm-node', param.node_id)
    });
  },

  unloadAll() {
    this.store.unloadAll('yarn-node');
    this.store.unloadAll('yarn-rm-node');
  }
});
