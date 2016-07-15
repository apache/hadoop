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
import Converter from 'yarn-ui/utils/converter';

export default Ember.Component.extend({
  tooltip : undefined,
  colors: d3.scale.category10().range(),

  init: function () {
    this._super();
    this.set("chart", {
      svg: undefined,
      g: undefined,
      h: 0,
      w: 0,
      tooltip: undefined
    });
  },

  initChart: function(removeLast = false) {
    // Init tooltip if it is not initialized
    // this.tooltip = d3.select("#chart-tooltip");
    if (!this.tooltip) {
      this.tooltip = d3.select("body")
        .append("div")
        .attr("class", "tooltip")
        .attr("id", "chart-tooltip")
        .style("opacity", 0);
    }

    var parentId = this.get("parentId");

    if (removeLast) {
      // Init svg
      var svg = d3.select("#" + parentId + "-svg");
      if (svg) {
        svg.remove();
      }
    }

    var parent = d3.select("#" + parentId);
    var bbox = parent.node().getBoundingClientRect();
    this.chart.w = bbox.width - 30;

    var ratio = 0.75; // 4:3 by default
    if (this.get("ratio")) {
      ratio = this.get("ratio");
    }
    this.chart.h = bbox.width * ratio;

    if (this.get("maxHeight")) {
      this.chart.h = Math.min(this.get("maxHeight"), this.chart.h);
    }

    this.chart.svg = parent.append("svg")
      .attr("width", this.chart.w)
      .attr("height", this.chart.h)
      .attr("id", parentId + "-svg");

    this.chart.g = this.chart.svg.append("g");
  },

  renderTitleAndBG: function(g, title, layout, background=true) {
    var bg = g.append("g");
    bg.append("text")
      .text(title)
      .attr("x", (layout.x1 + layout.x2) / 2)
      .attr("y", layout.y1 + layout.margin + 20)
      .attr("class", "chart-title");

    if (background) {
      bg.append("rect")
        .attr("x", layout.x1)
        .attr("y", layout.y1)
        .attr("width", layout.x2 - layout.x1)
        .attr("height", layout.y2 - layout.y1)
        .attr("class", "chart-frame");
    }
  },

  bindTooltip: function(d) {
    d.on("mouseover", function(d) {
        this.tooltip
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mousemove", function(d) {
        // Handle pie chart case
        var data = d;
        if (d.data) {
          data = d.data;
        }

        this.tooltip.style("opacity", .9);
        var value = data.value;
        if (this.get("type") == "memory") {
          value = Converter.memoryToSimpliedUnit(value);
        }
        this.tooltip.html(data.label + " = " + value)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mouseout", function(d) {
        this.tooltip.style("opacity", 0);
      }.bind(this));
  },

  adjustMaxHeight: function(h) {
    this.chart.svg.attr("height", h);
  },

  getLayout: function() {
    var x1 = 0;
    var y1 = 0;
    var x2 = this.chart.w;
    var y2 = this.chart.h;

    var layout = {
      x1: x1,
      y1: y1,
      x2: x2 - 10,
      y2: y2 - 10,
      margin: 10
    };
    return layout;
  },
});
