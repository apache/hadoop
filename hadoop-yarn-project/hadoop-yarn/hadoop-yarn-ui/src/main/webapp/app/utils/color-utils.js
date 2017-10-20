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

export default {
  preDefinedColors : ["#1f77b4", "#aec7e8", "#ffbb78",
    "#98df8a", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b",
    "#c49c94", "#e377c2", "#f7b6d2", "#c7c7c7", "#bcbd22",
    "#dbdb8d", "#17becf", "#9edae5"],

  colorMap: {
    "warn": "#ff7f0e",
    "good": "#2ca02c",
    "error": "#d62728",
    "others": "#7f7f7f",
  },

  getColors: function(nColors, colorsTarget, reverse = false) {
    var colors = [];
    for (var i = 0; i < nColors; i++) {
      colors.push(undefined);
    }

    var startIdx = 0;

    if (reverse) {
      startIdx = Math.max(nColors - colorsTarget.length, 0);
    }

    for (i = 0; i < colorsTarget.length; i++) {
      if (i + startIdx < nColors) {
        colors[i + startIdx] = this.getColorByTarget(colorsTarget[i]);
      }
    }

    var idx = 0;
    for (i = 0; i < nColors; i++) {
      if (!colors[i]) {
        colors[i] = this.preDefinedColors[i % this.preDefinedColors.length];
        idx ++;
      }
    }

    return colors;
  },

  getColorByTarget: function(target) {
    return this.colorMap[target];
  }
};
