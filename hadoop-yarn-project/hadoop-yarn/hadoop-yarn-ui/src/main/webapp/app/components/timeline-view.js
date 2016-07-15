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
  canvas: {
    svg: undefined,
    h: 0,
    w: 0,
    tooltip: undefined
  },

  clusterMetrics: undefined,
  modelArr: [],
  colors: d3.scale.category10().range(),
  _selected: undefined,

  selected: function() {
    return this._selected;
  }.property(),

  tableComponentName: function() {
    return "app-attempt-table";
  }.property(),

  setSelected: function(d) {
    if (this._selected == d) {
      return;
    }

    // restore color
    if (this._selected) {
      var dom = d3.select("#timeline-bar-" + this._selected.get("id"));
      dom.attr("fill", this.colors[0]);
    }

    this._selected = d;
    this.set("selected", d);
    dom = d3.select("#timeline-bar-" + d.get("id"));
    dom.attr("fill", this.colors[1]);
  },

  getPerItemHeight: function() {
    var arrSize = this.modelArr.length;

    if (arrSize < 20) {
      return 30;
    } else if (arrSize < 100) {
      return 10;
    } else {
      return 2;
    }
  },

  getPerItemGap: function() {
    var arrSize = this.modelArr.length;

    if (arrSize < 20) {
      return 5;
    } else if (arrSize < 100) {
      return 1;
    } else {
      return 1;
    }
  },

  getCanvasHeight: function() {
    return (this.getPerItemHeight() + this.getPerItemGap()) * this.modelArr.length + 200;
  },

  draw: function(start, end) {
    // get w/h of the svg
    var bbox = d3.select("#" + this.get("parent-id"))
      .node()
      .getBoundingClientRect();
    this.canvas.w = bbox.width;
    this.canvas.h = this.getCanvasHeight();

    this.canvas.svg = d3.select("#" + this.get("parent-id"))
      .append("svg")
      .attr("width", this.canvas.w)
      .attr("height", this.canvas.h)
      .attr("id", this.get("my-id"));
    this.renderTimeline(start, end);
  },

  renderTimeline: function(start, end) {
    var border = 30;
    var singleBarHeight = this.getPerItemHeight();
    var gap = this.getPerItemGap();
    var textWidth = 200;
    /*
     start-time                              end-time
      |--------------------------------------|
         ==============
                ==============
                        ==============
                              ===============
     */
    var xScaler = d3.scale.linear()
      .domain([start, end])
      .range([0, this.canvas.w - 2 * border - textWidth]);

    /*
     * Render frame of timeline view
     */
    this.canvas.svg.append("line")
      .attr("x1", border + textWidth)
      .attr("y1", border - 5)
      .attr("x2", this.canvas.w - border)
      .attr("y2", border - 5)
      .attr("class", "chart");

    this.canvas.svg.append("line")
      .attr("x1", border + textWidth)
      .attr("y1", border - 10)
      .attr("x2", border + textWidth)
      .attr("y2", border - 5)
      .attr("class", "chart");

    this.canvas.svg.append("line")
      .attr("x1", this.canvas.w - border)
      .attr("y1", border - 10)
      .attr("x2", this.canvas.w - border)
      .attr("y2", border - 5)
      .attr("class", "chart");

    this.canvas.svg.append("text")
        .text(Converter.timeStampToDate(start))
        .attr("y", border - 15)
        .attr("x", border + textWidth)
        .attr("class", "bar-chart-text")
        .attr("text-anchor", "left");

    this.canvas.svg.append("text")
        .text(Converter.timeStampToDate(end))
        .attr("y", border - 15)
        .attr("x", this.canvas.w - border)
        .attr("class", "bar-chart-text")
        .attr("text-anchor", "end");

    // show bar
    var bar = this.canvas.svg.selectAll("bars")
      .data(this.modelArr)
      .enter()
      .append("rect")
      .attr("y", function(d, i) {
        return border + (gap + singleBarHeight) * i;
      })
      .attr("x", function(d, i) {
        return border + textWidth + xScaler(d.get("startTs"));
      })
      .attr("height", singleBarHeight)
      .attr("fill", function(d, i) {
        return this.colors[0];
      }.bind(this))
      .attr("width", function(d, i) {
        var finishedTs = xScaler(d.get("finishedTs"));
        finishedTs = finishedTs > 0 ? finishedTs : xScaler(end);
        return finishedTs - xScaler(d.get("startTs"));
      })
      .attr("id", function(d, i) {
        return "timeline-bar-" + d.get("id");
      });
    bar.on("click", function(d) {
      this.setSelected(d);
    }.bind(this));

    this.bindTooltip(bar);

    if (this.modelArr.length <= 20) {
      // show bar texts
      for (var i = 0; i < this.modelArr.length; i++) {
        this.canvas.svg.append("text")
          .text(this.modelArr[i].get(this.get("label")))
          .attr("y", border + (gap + singleBarHeight) * i + singleBarHeight / 2)
          .attr("x", border)
          .attr("class", "bar-chart-text");
      }
    }
  },

  bindTooltip: function(d) {
    d.on("mouseover", function(d) {
        this.tooltip
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mousemove", function(d) {
        this.tooltip.style("opacity", .9);
        this.tooltip.html(d.get("tooltipLabel"))
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mouseout", function(d) {
        this.tooltip.style("opacity", 0);
      }.bind(this));
  },

  initTooltip: function() {
    this.tooltip = d3.select("body")
      .append("div")
      .attr("class", "tooltip")
      .attr("id", "chart-tooltip")
      .style("opacity", 0);
  },

  didInsertElement: function() {
    // init tooltip
    this.initTooltip();
    this.modelArr = [];

    // init model
    if (this.get("rmModel")) {
      this.get("rmModel").forEach(function(o) {
        if(!this.modelArr.contains(o)) {
          this.modelArr.push(o);
        }
      }.bind(this));
    }

    if (this.get("tsModel")) {
      this.get("tsModel").forEach(function(o) {
        if(!this.modelArr.contains(o)) {
          this.modelArr.push(o);
        }
      }.bind(this));
    }

    if(this.modelArr.length == 0) {
      return;
    }

    this.modelArr.sort(function(a, b) {
      var tsA = a.get("startTs");
      var tsB = b.get("startTs");

      return tsA - tsB;
    });
    if (this.modelArr.length > 0) {
      var begin = this.modelArr[0].get("startTs");
    }
    var end = 0;
    for (var i = 0; i < this.modelArr.length; i++) {
      var ts = this.modelArr[i].get("finishedTs");
      if (ts > end) {
        end = ts;
      }
    }
    if (end < begin) {
      end = Date.now();
    }

    this.draw(begin, end);

    if (this.modelArr.length > 0) {
      this.setSelected(this.modelArr[0]);
    }
  },
});