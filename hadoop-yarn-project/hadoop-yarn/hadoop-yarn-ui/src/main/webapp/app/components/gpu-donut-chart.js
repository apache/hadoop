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

import DonutChart from 'yarn-ui/components/donut-chart';
import ColorUtils from 'yarn-ui/utils/color-utils';

export default DonutChart.extend({
  draw: function() {
    // Construct data
    var data = [];
    if (this.get("gpu-render-type") === "gpu-memory") {
      data.push({
        label: "Used",
        value: parseFloat(this.get("gpuInfo").gpuMemoryUsage.usedMemoryMiB),
      });
      data.push({
        label: "Available",
        value: parseFloat(this.get("gpuInfo").gpuMemoryUsage.availMemoryMiB)
      });
    } else if (this.get("gpu-render-type") === "gpu-utilization") {
      var utilization = parseFloat(this.get("gpuInfo").gpuUtilizations.overallGpuUtilization);
      data.push({
        label: "Utilized",
        value: utilization,
      });
      data.push({
        label: "Available",
        value: 100 - utilization
      });
    }

    var colorTargets = this.get("colorTargets");
    if (colorTargets) {
      var colorTargetReverse = Boolean(this.get("colorTargetReverse"));
      var targets = colorTargets.split(" ");
      this.colors = ColorUtils.getColors(data.length, targets, colorTargetReverse);
    }

    this.renderDonutChart(data, this.get("title"), this.get("showLabels"),
      this.get("middleLabel"), this.get("middleValue"), this.get("suffix"));
  },

  didInsertElement: function() {
    // ParentId includes minorNumber
    var newParentId = this.get("parentId") + this.get("gpuInfo").minorNumber;
    this.set("parentId", newParentId);

    this.initChart();
    this.draw();
  },
});