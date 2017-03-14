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

import BaseChartComponent from 'yarn-ui/components/base-chart-component';

export default BaseChartComponent.extend({
  CELL_WIDTH: 250,
  SAMPLE_CELL_WIDTH: 100,
  SAMPLE_HEIGHT: 30,
  CELL_HEIGHT: 30,
  CELL_MARGIN: 2,
  RACK_MARGIN: 20,
  filter: "",
  selectedCategory: 0,

  bindTP: function(element, cell) {
    element.on("mouseover", function() {
      this.tooltip
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
      cell.style("opacity", 1.0);
    }.bind(this))
      .on("mousemove", function() {
        // Handle pie chart case
        var text = cell.attr("tooltiptext");

        this.tooltip.style("opacity", 0.9);
        this.tooltip.html(text)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mouseout", function() {
        this.tooltip.style("opacity", 0);
        cell.style("opacity", 0.8);
      }.bind(this));
  },

  bindSelectCategory: function(element, i) {
    element.on("click", function() {
      if (this.selectedCategory == i) {
        // Remove selection for second click
        this.selectedCategory = 0;
      } else {
        this.selectedCategory = i;
      }
      this.didInsertElement();
    }.bind(this));
  },

  isNodeSelected: function(node) {
    if (this.filter) {
      var rack = node.get("rack");
      var host = node.get("nodeHostName");
      if (!rack.includes(this.filter) && !host.includes(this.filter)) {
        return false;
      }
    }

    if (this.selectedCategory === 0) {
      return true;
    }

    var usage = node.get("usedMemoryMB") /
      (node.get("usedMemoryMB") + node.get("availMemoryMB"))
    var lowerLimit = (this.selectedCategory - 1) * 0.2;
    var upperLimit = this.selectedCategory * 0.2;
    if (lowerLimit <= usage && usage <= upperLimit) {
      return true;
    }
    return false;
  },

  // data:
  //    [{label=label1, value=value1}, ...]
  //    ...
  renderCells: function (model, title) {
    var data = [];
    model.forEach(function (o) {
      data.push(o);
    });

    this.chart.g.remove();
    this.chart.g = this.chart.svg.append("g");
    var g = this.chart.g;
    var layout = this.getLayout();
    layout.margin = 50;

    let racks = new Set();
    for (var i = 0; i < data.length; i++) {
      racks.add(data[i].get("rack"));
    }

    let racksArray = [];
    racks.forEach(v => racksArray.push(v));

    var xOffset = layout.margin;
    var yOffset = layout.margin * 3;

    var colorFunc = d3.interpolate(d3.rgb("#bdddf5"), d3.rgb("#0f3957"));

    var sampleXOffset = (layout.x2 - layout.x1) / 2 - 2.5 * this.SAMPLE_CELL_WIDTH -
      2 * this.CELL_MARGIN;
    var sampleYOffset = layout.margin * 2;

    for (i = 1; i <= 5; i++) {
      var ratio = i * 0.2 - 0.1;

      var rect = g.append("rect")
        .attr("x", sampleXOffset)
        .attr("y", sampleYOffset)
        .attr("fill", this.selectedCategory === i ? "#2ca02c" : colorFunc(ratio))
        .attr("width", this.SAMPLE_CELL_WIDTH)
        .attr("height", this.SAMPLE_HEIGHT)
        .attr("class", "hyperlink");
      this.bindSelectCategory(rect, i);
      var text = g.append("text")
        .text("" + (ratio * 100).toFixed(1) + "% Used")
        .attr("y", sampleYOffset + this.SAMPLE_HEIGHT / 2 + 5)
        .attr("x", sampleXOffset + this.SAMPLE_CELL_WIDTH / 2)
        .attr("class", "heatmap-cell hyperlink");
      this.bindSelectCategory(text, i);
      sampleXOffset += this.CELL_MARGIN + this.SAMPLE_CELL_WIDTH;
    }

    if (this.selectedCategory != 0) {
      var text = g.append("text")
        .text("Clear")
        .attr("y", sampleYOffset + this.SAMPLE_HEIGHT / 2 + 5)
        .attr("x", sampleXOffset + 20)
        .attr("class", "heatmap-clear hyperlink");
      this.bindSelectCategory(text, 0);
    }

    var chartXOffset = -1;

    for (i = 0; i < racksArray.length; i++) {
      var text = g.append("text")
        .text(racksArray[i])
        .attr("y", yOffset + this.CELL_HEIGHT / 2 + 5)
        .attr("x", layout.margin)
        .attr("class", "heatmap-rack");

      if (-1 === chartXOffset) {
        chartXOffset = layout.margin + text.node().getComputedTextLength() + 30;
      }

      xOffset = chartXOffset;

      for (var j = 0; j < data.length; j++) {
        var rack = data[j].get("rack");
        var host = data[j].get("nodeHostName");

        if (rack === racksArray[i]) {
          this.addNode(g, xOffset, yOffset, colorFunc, data[j]);
          xOffset += this.CELL_MARGIN + this.CELL_WIDTH;
          if (xOffset + this.CELL_MARGIN + this.CELL_WIDTH >= layout.x2 -
            layout.margin) {
            xOffset = chartXOffset;
            yOffset = yOffset + this.CELL_MARGIN + this.CELL_HEIGHT;
          }

        }
      }

      while (xOffset > chartXOffset && xOffset + this.CELL_MARGIN +
        this.CELL_WIDTH < layout.x2 - layout.margin) {
        this.addPlaceholderNode(g, xOffset, yOffset);
        xOffset += this.CELL_MARGIN + this.CELL_WIDTH;
      }

      if (xOffset !== chartXOffset) {
        xOffset = chartXOffset;
        yOffset += this.CELL_MARGIN + this.CELL_HEIGHT;
      }
      yOffset += this.RACK_MARGIN;
    }

    layout.y2 = yOffset + layout.margin;
    this.adjustMaxHeight(layout.y2);
    this.renderTitleAndBG(g, title, layout, false);
  },

  addNode: function (g, xOffset, yOffset, colorFunc, data) {
    var rect = g.append("rect")
      .attr("y", yOffset)
      .attr("x", xOffset)
      .attr("height", this.CELL_HEIGHT)
      .attr("fill", colorFunc(data.get("usedMemoryMB") /
        (data.get("usedMemoryMB") + data.get("availMemoryMB"))))
      .attr("width", this.CELL_WIDTH)
      .attr("tooltiptext", data.get("toolTipText"));

    if (this.isNodeSelected(data)) {
      rect.style("opacity", 0.8);
      this.bindTP(rect, rect);
    } else {
      rect.style("opacity", 0.8);
      rect.attr("fill", "DimGray");
    }
    var node_id = data.get("id"),
        node_addr = data.get("nodeHTTPAddress"),
        href = `#/yarn-node/${node_id}/${node_addr}`;
    var a = g.append("a")
      .attr("href", href);
    var text = a.append("text")
      .text(data.get("nodeHostName"))
      .attr("y", yOffset + this.CELL_HEIGHT / 2 + 5)
      .attr("x", xOffset + this.CELL_WIDTH / 2)
      .attr("class", this.isNodeSelected(data) ? "heatmap-cell" : "heatmap-cell-notselected")
    if (this.isNodeSelected(data)) {
      this.bindTP(a, rect);
    }
  },

  addPlaceholderNode: function(g, xOffset, yOffset) {
    g.append("rect")
      .attr("y", yOffset)
      .attr("x", xOffset)
      .attr("height", this.CELL_HEIGHT)
      .attr("fill", "grey")
      .attr("width", this.CELL_WIDTH)
      .style("opacity", 0.20);
  },

  draw: function() {
    this.initChart(true);
    this.renderCells(this.get("model"), this.get("title"), this.get("textWidth"));
  },

  didInsertElement: function () {
    this.draw();
  },

  actions: {
    applyFilter: function(event) {
      this.filter = event.srcElement.value;
      this.selectedCategory = 0;
      this.didInsertElement();
    }
  }
});
