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

export default BaseChartComponent.extend({
  // data: 
  //    [{label=label1, value=value1}, ...]
  //    ...
  renderBarChart: function(data, title, textWidth = 50) {
    var g = this.chart.g;
    var layout = this.getLayout();
    this.renderTitleAndBG(g, title, layout);

    var maxValue = -1;
    for (var i = 0; i < data.length; i++) {
      if (data[i] instanceof Array) {
        if (data[i][0].value > maxValue) {
          maxValue = data[i][0].value;
        }
      } else {
        if (data[i].value > maxValue) {
          maxValue = data[i].value;
        }
      }
    }

    var singleBarHeight = 30;

    // 50 is for text
    var maxBarWidth = layout.x2 - layout.x1 - 2 * layout.margin - textWidth - 50;

    // 30 is for title
    var maxBarsHeight = layout.y2 - layout.y1 - 2 * layout.margin - 30;
    var gap = (maxBarsHeight - data.length * singleBarHeight) / (data.length -
      1);

    var xScaler = d3.scale.linear()
      .domain([0, maxValue])
      .range([0, maxBarWidth]);

    // show bar text
    for (var i = 0; i < data.length; i++) {
      g.append("text")
        .text(
          function() {
            return data[i].label;
          })
        .attr("y", function() {
          return layout.y1 + singleBarHeight / 2 + layout.margin + (gap +
            singleBarHeight) * i + 30;
        })
        .attr("x", layout.x1 + layout.margin);
    }

    // show bar
    var bar = g.selectAll("bars")
      .data(data)
      .enter()
      .append("rect")
      .attr("y", function(d, i) {
        return layout.y1 + 30 + layout.margin + (gap + singleBarHeight) * i;
      })
      .attr("x", layout.x1 + layout.margin + textWidth)
      .attr("height", singleBarHeight)
      .attr("fill", function(d, i) {
        return this.colors[i];
      }.bind(this))
      .attr("width", 0);

    this.bindTooltip(bar);

    bar.transition()
      .duration(500)
      .attr("width", function(d) {
        var w;
        w = xScaler(d.value);
        // At least each item has 3 px
        w = Math.max(w, 3);
        return w;
      });

    // show bar value
    for (var i = 0; i < data.length; i++) {
      g.append("text")
        .text(
          function() {
            return data[i].value;
          })
        .attr("y", function() {
          return layout.y1 + singleBarHeight / 2 + layout.margin + (gap +
            singleBarHeight) * i + 30;
        })
        .attr("x", layout.x1 + layout.margin + textWidth + 15 + xScaler(data[i].value));
    }
  },

  draw: function() {
    this.renderBarChart(this.get("data"), this.get("title"), this.get("textWidth"));
  },

  _dataChange: Ember.observer("data", function() {
    this.chart.g.selectAll("*").remove();
    this.draw();
  }),

  didInsertElement: function() {
    this.initChart();
    this.draw();
  },
})