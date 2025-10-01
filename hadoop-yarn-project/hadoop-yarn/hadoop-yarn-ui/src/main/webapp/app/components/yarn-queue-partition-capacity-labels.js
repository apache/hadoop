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
    const resourceUsagesByPartitionMap = this.get("resourceUsagesByPartitionMap");
    const filteredPartition = this.get("filteredPartition") || PARTITION_LABEL;
    const userLimit = queue.get("userLimit");
    const userLimitFactor = queue.get("userLimitFactor");
    const isLeafQueue = queue.get("isLeafQueue");
    const isWeightMode = queue.get("isWeightMode");
    const isFlexibleDynamicQueue = queue.get("isFlexibleDynamicQueue");
    const weight = queue.get("weight");
    const orderingPolicyInfo = queue.get("orderingPolicyInfo");
    const normalizedWeight = queue.get("normalizedWeight");
    const creationMethod = queue.get("creationMethod");
    const numActiveApplications = queue.get("numActiveApplications");
    const numPendingApplications = queue.get("numPendingApplications");
    const numContainers = queue.get("numContainers");
    const maxApplications = queue.get("maxApplications");
    const maxApplicationsPerUser = queue.get("maxApplicationsPerUser");
    const nodeLabels = queue.get("nodeLabels");
    const defaultNodeLabelExpression = queue.get("defaultNodeLabelExpression");
    const preemptionDisabled = queue.get("preemptionDisabled");
    const intraQueuePreemptionDisabled = queue.get("intraQueuePreemptionDisabled");
    const defaultPriority = queue.get("defaultPriority");

    return {
      ...partitionMap[filteredPartition],
      ...resourceUsagesByPartitionMap[filteredPartition],
      userLimit,
      userLimitFactor,
      isLeafQueue,
      isWeightMode,
      weight,
      normalizedWeight,
      orderingPolicyInfo,
      creationMethod,
      isFlexibleDynamicQueue,
      numActiveApplications,
      numPendingApplications,
      numContainers,
      maxApplications,
      maxApplicationsPerUser,
      userLimit,
      userLimitFactor,
      nodeLabels,
      defaultNodeLabelExpression,
      preemptionDisabled,
      intraQueuePreemptionDisabled,
      defaultPriority
    };
  }
});
