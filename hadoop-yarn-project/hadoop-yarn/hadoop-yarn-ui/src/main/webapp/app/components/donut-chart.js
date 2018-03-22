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
import BaseChartComponent from 'yarn-ui/components/base-chart-component';
import ColorUtils from 'yarn-ui/utils/color-utils';
import Converter from 'yarn-ui/utils/converter';
import {Entities} from 'yarn-ui/constants';

export default BaseChartComponent.extend({
  /*
   * data = [{label="xx", value=},{...}]
   */
  renderDonutChart: function(data, title, showLabels = false, 
    middleLabel = "Total", middleValue = undefined, suffix = "") {
    var g = this.chart.g;
    var layout = this.getLayout();
    this.renderTitleAndBG(g, title, layout);

    var total = 0;
    var allZero = true;
    for (var i = 0; i < data.length; i++) {
      total += data[i].value;
      if (data[i].value > 1e-6) {
        allZero = false;
      }
    }

    if (!middleValue) {
      if (this.get(Entities.Type) === Entities.Memory) {
        middleValue = Converter.memoryToSimpliedUnit(total);
      } else if (this.get(Entities.Type) === Entities.Resource) {
        middleValue = Converter.resourceToSimplifiedUnit(total, this.get(Entities.Unit));
      } else {
        middleValue = total;
      }
    }

    //Width and height
    var h = layout.y2 - layout.y1;

    // 50 is for title
    var outerRadius = (h - 50 - 2 * layout.margin) / 2;

    // Ratio of inner radius to outer radius
    var radiusRatio = 0.75;
    var innerRadius = outerRadius * radiusRatio;

    var arc = d3.svg.arc()
      .innerRadius(innerRadius)
      .outerRadius(outerRadius);

    var cx;
    var cy = layout.y1 + 50 + layout.margin + outerRadius;
    if (showLabels) {
      cx = layout.x1 + layout.margin + outerRadius;
    } else {
      cx = (layout.x1 + layout.x2) / 2;
    }

    var pie = d3.layout.pie();
    pie.sort(null);
    pie.value(function(d) {
      var v = d.value;
      // make sure it > 0
      v = Math.max(v, 1e-6);
      return v;
    });

    //Set up groups
    var arcs = g
      .selectAll("g.arc")
      .data(pie(data))
      .enter()
      .append("g")
      .attr("class", "arc")
      .attr("transform", "translate(" + cx + "," + cy + ")");

    function tweenPie(finish) {
      var start = {
        startAngle: 0,
        endAngle: 0
      };
      var i = d3.interpolate(start, finish);
      return function(d) {
        return arc(i(d));
      };
    }

    //Draw arc paths
    var path = arcs.append("path")
      .attr("fill", function(d, i) {
        if (d.value > 1e-6) {
          return this.colors[i];
        } else {
          return "white";
        }
      }.bind(this))
      .attr("d", arc)
      .attr("stroke", function(d, i) {
        if (allZero) {
          return this.colors[i];
        }
      }.bind(this));
    this.bindTooltip(path);
    path.on("click", function (d) {
      var data = d.data;
      if (data.link) {
        this.tooltip.remove();
        document.location.href = data.link;
      }
    }.bind(this));

    // Show labels
    if (showLabels) {
      var lx = layout.x1 + layout.margin + outerRadius * 2 + 30;
      var squareW = 15;
      var margin = 10;

      var select = g.selectAll(".rect")
        .data(data)
        .enter();
      select.append("rect")
        .attr("fill", function(d, i) {
          return this.colors[i];
        }.bind(this))
        .attr("x", lx)
        .attr("y", function(d, i) {
          return layout.y1 + 75 + (squareW + margin) * i + layout.margin;
        })
        .attr("width", squareW)
        .attr("height", squareW);
      select.append("text")
        .attr("x", lx + squareW + margin)
        .attr("y", function(d, i) {
          return layout.y1 + 80 + (squareW + margin) * i + layout.margin + squareW / 2;
        })
        .text(function(d) {
          var value = d.value;
          if (this.get("type") === "memory") {
            value = Converter.memoryToSimpliedUnit(value);
          } else if (this.get("type") === "resource") {
            value = Converter.resourceToSimplifiedUnit(value, this.get(Entities.Unit));
          }

          return d.label + ' = ' + value + suffix;
        }.bind(this));
    }

    if (middleLabel) {
      var highLightColor = this.colors[0];
      g.append("text").text(middleLabel).attr("x", cx).attr("y", cy - 10).
        attr("class", "donut-highlight-text").attr("fill", highLightColor);
      g.append("text").text(middleValue).attr("x", cx).attr("y", cy + 15).
        attr("class", "donut-highlight-sub").attr("fill", highLightColor);
    }

    path.transition()
      .duration(500)
      .attrTween('d', tweenPie);
  },

  _dataChange: Ember.observer("data", function() {
    this.chart.g.selectAll("*").remove();
    if(this.get("data")) {
      this.draw();
    }
  }),

  draw: function() {
    var colorTargets = this.get("colorTargets");
    if (colorTargets) {
      var colorTargetReverse = Boolean(this.get("colorTargetReverse"));
      var targets = colorTargets.split(" ");
      this.colors = ColorUtils.getColors(this.get("data").length, targets, colorTargetReverse);
    }

    this.renderDonutChart(this.get("data"), this.get("title"), this.get("showLabels"), 
                          this.get("middleLabel"), this.get("middleValue"), this.get("suffix"));
  },

  didInsertElement: function() {
    // When parentIdPrefix is specified, use parentidPrefix + name as new parent
    // id
    if (this.get("parentIdPrefix")) {
      var newParentId = this.get("parentIdPrefix") + this.get("id");
      this.set("parentId", newParentId);
      console.log(newParentId);
    }

    this.initChart();
    this.draw();
  },
});