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

import BaseUsageDonutChart from 'yarn-ui/components/base-usage-donut-chart';
import ColorUtils from 'yarn-ui/utils/color-utils';
import HrefAddressUtils from 'yarn-ui/utils/href-address-utils';

export default BaseUsageDonutChart.extend({
  colors: d3.scale.category20().range(),

  draw: function() {
    var usageByApps = [];
    var avail = 100;

    this.get("data").forEach(function (app) {
      var v = app.get("clusterUsagePercentage");
      if (v > 1e-2) {
        usageByApps.push({
          label: app.get("id"),
          link: HrefAddressUtils.getApplicationLink(app.get("id")),
          value: v.toFixed(2)
        });

        avail = avail - v;
      }
    }.bind(this));

    usageByApps.sort(function(a,b) {
      return b.value - a.value;
    });

    usageByApps = this.mergeLongTails(usageByApps, 8);

    usageByApps.push({
      label: "Available",
      value: avail.toFixed(4)
    });

    this.colors = ColorUtils.getColors(usageByApps.length, ["others", "good"], true);

    this.renderDonutChart(usageByApps, this.get("title"), this.get("showLabels"),
      this.get("middleLabel"), "100%", "%");
  },
});