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
import Converter from 'yarn-ui/utils/converter';

export default StackedBarchart.extend({
  getDataForRender: function(containers, nodes) {
    var arr = [];
    var nodeToResources = {};
    nodes.forEach(function(n) {
      nodeToResources[n.id] =
      {
        used: Number(n.get("usedMemoryMB")),
        avail: Number(n.get("availMemoryMB"))
      };
    });

    containers.forEach(function(c) {
      res = nodeToResources[c.get("assignedNodeId")];
      if (res) {
        if (!res.usedByTheApp) {
          res.usedByTheApp = 0;
        }
        res.usedByTheApp += Number(c.get("allocatedMB"));
      }
    });

    for (var nodeId in nodeToResources) {
      var res = nodeToResources[nodeId];

      var subArr = [];
      var value = res.usedByTheApp ? res.usedByTheApp : 0;
      subArr.push({
        value: value,
        bindText: "This app uses " + Converter.memoryToSimpliedUnit(value) + ". On node=" + nodeId,
      });

      value = res.used - value;
      value = Math.max(value, 0);
      subArr.push({
        value: value,
        bindText: "Other applications uses " + Converter.memoryToSimpliedUnit(value) + ". On node=" + nodeId,
      });

      subArr.push({
        value: res.avail,
        bindText: "Free resource " + Converter.memoryToSimpliedUnit(res.avail) + " . On node=" + nodeId
      });

      arr.push(subArr);
    }

    console.log(arr);

    return arr;
  },

  didInsertElement: function() {
    this.initChart(true);

    this.colors = ["lightsalmon", "Grey", "mediumaquamarine"];

    var containers = this.get("rmContainers");
    var nodes = this.get("nodes");

    var data = this.getDataForRender(containers, nodes);

    this.show(
      data, this.get("title"), ["Used by this app", "Used by other apps",
        "Available"]);
  },
});
