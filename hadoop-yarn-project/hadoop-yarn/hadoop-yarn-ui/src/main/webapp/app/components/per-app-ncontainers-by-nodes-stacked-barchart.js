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

import StackedBarchart from 'yarn-ui/components/stacked-barchart';

export default StackedBarchart.extend({
  getDataForRender: function(containers, nodes) {
    var arr = [];
    var nodeToContainers = {};
    nodes.forEach(function(n) {
      nodeToContainers[n.id] = 0;
    });

    containers.forEach(function(c) {
      var nodeId = c.get("assignedNodeId");
      var n = nodeToContainers[nodeId];
      if (undefined != n) {
        nodeToContainers[nodeId] += 1;
      }
    });

    for (var nodeId in nodeToContainers) {
      var n = nodeToContainers[nodeId];

      var subArr = [];
      subArr.push({
        value: n,
        bindText: "This app has " + n + " containers running on node=" + nodeId
      });

      arr.push(subArr);
    }

    console.log(arr);

    return arr;
  },

  didInsertElement: function() {
    this.initChart(true);

    this.colors = ["Orange", "Grey", "Gainsboro"];

    var containers = this.get("rmContainers");
    var nodes = this.get("nodes");

    var data = this.getDataForRender(containers, nodes);

    this.show(
      data, this.get("title"), ["Running containers from this app"]);
  },
})
