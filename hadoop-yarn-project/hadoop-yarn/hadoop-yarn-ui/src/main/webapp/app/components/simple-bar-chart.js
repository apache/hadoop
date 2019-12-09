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

export default Ember.Component.extend({
  data: [],
  xAxisTickFormatter: null,
  yAxisTickFormatter: null,
  xAxisText: '',
  yAxisText: '',
  enableTooltip: true,
  onBarChartClickCallback: Ember.K,
  hideTootlipOnBarChartClick: true,

  initChart() {
    this.height = 400;
    this.barWidth = 30;
    this.width = Math.max(500, 40 * this.data.length);
  },

  drawChart() {
    var margin = {top: 20, right: 20, bottom: 100, left: 100},
        axisLabelPadding = 10,
        width = this.width - margin.left - margin.right - axisLabelPadding,
        height = this.height - margin.top - margin.bottom - axisLabelPadding,
        xAxisText = this.xAxisText? this.xAxisText : '',
        yAxisText = this.yAxisText? this.yAxisText : '',
        data = this.data,
        self = this;

    var xScale = d3.scale.ordinal().rangeRoundBands([0, width], 0.1);
    var yScale = d3.scale.linear().range([height, 0]);

    var xAxis = d3.svg.axis()
        .scale(xScale)
        .orient("bottom")
        .tickFormat(function(tick) {
          if (self.isFunction(self.xAxisTickFormatter)) {
            return self.xAxisTickFormatter(tick);
          } else {
            return tick;
          }
        });

    var yAxis = d3.svg.axis()
        .scale(yScale)
        .orient("left")
        .tickFormat(function(tick) {
          if (self.isFunction(self.yAxisTickFormatter)) {
            return self.yAxisTickFormatter(tick);
          } else {
            return tick;
          }
        });

    var svg = d3.select(this.element)
      .append("svg")
        .attr("class", "simple-bar-chart")
        .attr("width", width + margin.left + margin.right + axisLabelPadding)
        .attr("height", height + margin.top + margin.bottom + axisLabelPadding)
      .append("g")
        .attr("transform", "translate("+(margin.left+axisLabelPadding)+","+(margin.top)+")");

    xScale.domain(data.map(function(d) { return d.label; }));
    yScale.domain([0, d3.max(data, function(d) { return d.value; })]);

    var gx = svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    gx.selectAll("text")
        .style("text-anchor", "end")
        .attr("dx", "-.8em")
        .attr("dy", "-.3em")
        .attr("transform", "rotate(-60)");

    gx.append("text")
        .attr("transform", "translate("+(width/2)+","+(margin.bottom)+")")
        .style("text-anchor", "middle")
        .text(xAxisText);

    var gy = svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);

    gy.append("text")
        .attr("transform", "translate("+(-margin.left)+","+(height/2)+")rotate(-90)")
        .style("text-anchor", "middle")
        .text(yAxisText);

    var barWidth = this.barWidth;
    var minBarWidth = Math.min(barWidth, xScale.rangeBand());
    var bars = svg.selectAll("bar")
        .data(data)
      .enter().append("rect")
        .attr("x", function(d) {
          var padding = 0;
          var rangeBand = xScale.rangeBand();
          if ((rangeBand - barWidth) > 0) {
            padding = (rangeBand - barWidth) / 2;
          }
          return xScale(d.label) + padding;
        })
        .attr("width", minBarWidth)
        .attr("y", function() {
          return yScale(0);
        })
        .attr("height", function() {
          return height - yScale(0);
        })
        .on('click', function(d) {
          if (self.enableTooltip && self.hideTootlipOnBarChartClick) {
            self.hideTootlip();
          }
          if (self.isFunction(self.onBarChartClickCallback)) {
            self.onBarChartClickCallback(d);
          }
        });

    bars.transition()
        .duration(1000)
        .delay(100)
        .attr("y", function(d) {
          return yScale(d.value);
        })
        .attr("height", function(d) {
          return height - yScale(d.value);
        });

    if (this.enableTooltip) {
      this.bindTooltip(bars);
    }
  },

  bindTooltip(bars) {
    var self = this;
    var tooltip = this.tooltip;
    if (tooltip) {
      bars.on("mouseenter", function(d) {
        tooltip.html(d.tooltip);
        self.showTooltip();
      }).on("mousemove", function() {
        tooltip.style("left", (d3.event.pageX + 5) + "px")
            .style("top", (d3.event.pageY - 25) + "px");
      }).on("mouseout", function() {
        self.hideTootlip();
      });
    }
  },

  initTooltip() {
    this.tooltip = d3.select("body")
        .append("div")
        .attr("class", "tooltip simple-barchart-tooltip")
        .style("opacity", 1);

    this.hideTootlip();
  },

  hideTootlip() {
    if (this.tooltip) {
     this.tooltip.style("display", "none");
    }
  },

  showTooltip() {
    if (this.tooltip) {
     this.tooltip.style("display", "block");
    }
  },

  isFunction(func) {
    return Ember.typeOf(func) === "function";
  },

  didInsertElement() {
    this.initChart();
    if (this.enableTooltip) {
      this.initTooltip();
    }
    this.drawChart();
  },

  willDestroyElement() {
    if (this.tooltip) {
      this.tooltip.remove();
    }
  }
});
