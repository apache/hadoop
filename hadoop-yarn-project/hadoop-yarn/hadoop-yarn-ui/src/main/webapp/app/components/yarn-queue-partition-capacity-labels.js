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

import Ember from "ember";
import { PARTITION_LABEL } from "../constants";

export default Ember.Component.extend({
  didUpdateAttrs: function({ oldAttrs, newAttrs }) {
    this._super(...arguments);
    this.set("data", this.initData());
  },

  init() {
    this._super(...arguments);
    this.set("data", this.initData());
  },

  initData() {
    const queue = this.get("queue");
    const partitionMap = this.get("partitionMap");
    const filteredParition = this.get("filteredPartition") || PARTITION_LABEL;
    const userLimit = queue.get("userLimit");
    const userLimitFactor = queue.get("userLimitFactor");
    const isLeafQueue = queue.get("isLeafQueue");

    return {
      ...partitionMap[filteredParition],
      userLimit,
      userLimitFactor,
      isLeafQueue
    };
  }
});
