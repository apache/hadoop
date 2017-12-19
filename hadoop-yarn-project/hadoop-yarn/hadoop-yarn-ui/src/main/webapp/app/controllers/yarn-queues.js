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
import {PARTITION_LABEL} from '../constants';

export default Ember.Controller.extend({
  needReload: true,
  selectedQueue: undefined,
  showLoading: true,
  filteredPartition: PARTITION_LABEL,

  breadcrumbs: [
    {
      text: "Home",
      routeName: "application"
    },
    {
      text: "Queues",
      routeName: "yarn-queues",
      model: "root"
    }
  ],

  actions: {
    setFilter(partition) {
      this.set("filteredPartition", partition);
      const model = this.get('model');
      const {selectedQueue} = model;
      // If the selected queue does not have the filtered partition
      // reset it to root
      if (!selectedQueue.get('partitions').contains(partition)) {
        const root = model.queues.get('firstObject');
        document.location.href = "#/yarn-queues/" + root.get("id") + "!";
        this.set("model.selectedQueue", root);
        this.set("model.selected", root.get('id'));
      }
    }
  }
});
