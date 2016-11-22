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
import DonutChart from 'yarn-ui/components/donut-chart';
import BaseUsageDonutChart from 'yarn-ui/components/base-usage-donut-chart';
import ColorUtils from 'yarn-ui/utils/color-utils';
import HrefAddressUtils from 'yarn-ui/utils/href-address-utils';

export default BaseUsageDonutChart.extend({
  colors: d3.scale.category20().range(),

  draw: function() {
    var usageByQueues = [];
    var avail = 100;

    this.get("data").forEach(function (queue) {
      var v = queue.get("absUsedCapacity");

      if (queue.get("isLeafQueue")) {
        if (v > 1e-2) {
          usageByQueues.push({
            label: queue.get("id"),
            link: HrefAddressUtils.getQueueLink(queue.get("id")),
            value: v.toFixed(2)
          });

          avail = avail - v;
        }
      }
    });

    usageByQueues.sort(function(a, b) {
      return b.value - a.value;
    });

    usageByQueues = this.mergeLongTails(usageByQueues, 8);

    usageByQueues.push({
      label: "Available",
      value: avail.toFixed(4)
    });

    this.colors = ColorUtils.getColors(usageByQueues.length, ["others", "good"], true);

    this.renderDonutChart(usageByQueues, this.get("title"), this.get("showLabels"),
      this.get("middleLabel"), "100%", "%");
  },

  didInsertElement: function() {
    this.initChart();
    this.draw();
  },
})