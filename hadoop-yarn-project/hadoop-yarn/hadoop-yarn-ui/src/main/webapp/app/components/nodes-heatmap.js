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
  memoryLabel: "Memory",
  cpuLabel: "VCores",
  containersLabel: "Containers",
  totalContainers: 0,

  bindTP: function(element, cell) {
    var currentToolTip = this.tooltip;
    element.on("mouseover", function() {
      currentToolTip
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
      cell.style("opacity", 1.0);
    }.bind(this))
      .on("mousemove", function() {
        // Handle pie chart case
        var text = cell.attr("tooltiptext");
        currentToolTip
            .style("background", "black")
            .style("opacity", 0.7);
        currentToolTip
            .html(text)
            .style('font-size', '12px')
            .style('color', 'white')
            .style('font-weight', '400');
        currentToolTip
            .style("left", (d3.event.pageX) + "px")
            .style("top", (d3.event.pageY - 28) + "px");
  }.bind(this))
      .on("mouseout", function() {
        currentToolTip.style("opacity", 0);
        cell.style("opacity", 0.8);
      }.bind(this));
  },

  bindSelectCategory: function(element, i) {
    element.on("click", function() {
      if (this.selectedCategory === i) {
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

    var usage = this.calcUsage(node);
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
    var selectedOption = d3.select("select").property("value");
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

    var gradientStartColor = "#2ca02c";
    var gradientEndColor = "#ffb014";

    var colorFunc = d3.interpolateRgb(d3.rgb(gradientStartColor), d3.rgb(gradientEndColor));

    var sampleXOffset = (layout.x2 - layout.x1) / 2 - 2.5 * this.SAMPLE_CELL_WIDTH -
      2 * this.CELL_MARGIN;
    var sampleYOffset = layout.margin * 2;
    var text;

    for (i = 1; i <= 5; i++) {
      var ratio = i * 0.2 - 0.1;

      var rect = g.append("rect")
        .attr("x", sampleXOffset)
        .attr("y", sampleYOffset)
        .attr("fill", this.selectedCategory === i ? "#2c7bb6" : colorFunc(ratio))
        .attr("width", this.SAMPLE_CELL_WIDTH)
        .attr("height", this.SAMPLE_HEIGHT)
        .attr("class", "hyperlink");
      this.bindSelectCategory(rect, i);
      text = g.append("text")
        .text("" + (ratio * 100).toFixed(1) + "% Used")
        .attr("y", sampleYOffset + this.SAMPLE_HEIGHT / 2 + 5)
        .attr("x", sampleXOffset + this.SAMPLE_CELL_WIDTH / 2)
        .attr("class", "heatmap-cell hyperlink");
      this.bindSelectCategory(text, i);
      sampleXOffset += this.CELL_MARGIN + this.SAMPLE_CELL_WIDTH;
    }

    if (this.selectedCategory !== 0) {
      text = g.append("text")
        .text("Clear")
        .attr("y", sampleYOffset + this.SAMPLE_HEIGHT / 2 + 5)
        .attr("x", sampleXOffset + 20)
        .attr("class", "heatmap-clear hyperlink");
      this.bindSelectCategory(text, 0);
    }

    var chartXOffset = -1;

    this.totalContainers = 0;
    for (i = 0; i < racksArray.length; i++) {
      text = g.append("text")
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

        if (rack === racksArray[i]) {
          this.totalContainers += data[j].get("numContainers");
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
    this.renderTitleAndBG(g, title + selectedOption + ")" , layout, false);
  },

  addNode: function (g, xOffset, yOffset, colorFunc, data) {
    var rect = g.append("rect")
      .attr("y", yOffset)
      .attr("x", xOffset)
      .attr("height", this.CELL_HEIGHT)
      .attr("fill", colorFunc(this.calcUsage(data)))
      .attr("width", this.CELL_WIDTH)
      .attr("tooltiptext", data.get("toolTipText") + this.getToolTipText(data));

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
    a.append("text")
      .text(data.get("nodeHostName"))
      .attr("y", yOffset + this.CELL_HEIGHT / 2 + 5)
      .attr("x", xOffset + this.CELL_WIDTH / 2)
      .attr("class", this.isNodeSelected(data) ? "heatmap-cell" : "heatmap-cell-notselected");
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
    var parentId = this.get("parentId");
    var self = this;
    var optionsData = [this.memoryLabel, this.cpuLabel, this.containersLabel];
    d3.select("#heatmap-select")
      .on('change', function() {
        self.renderCells(self.get("model"), self.get("title"), self.get("textWidth"));
      })
      .selectAll('option')
      .data(optionsData).enter()
      .append('option')
      .text(function (d) { return d; });

    this.draw();
  },

  actions: {
    applyFilter: function(event) {
      this.filter = event.srcElement.value;
      this.selectedCategory = 0;
      this.didInsertElement();
    }
  },

  calcUsage: function(data) {
    var selectedOption = d3.select('select').property("value");
    if (selectedOption === this.memoryLabel) {
      return data.get("usedMemoryMB") /
        (data.get("usedMemoryMB") + data.get("availMemoryMB"));
    }
    else if (selectedOption === this.cpuLabel) {
      return data.get("usedVirtualCores") /
        (data.get("usedVirtualCores") + data.get("availableVirtualCores"));
    }
    else if (selectedOption === this.containersLabel) {
      var totalContainers = this.totalContainers;
      if (totalContainers === 0) { return 0; }
      return data.get("numContainers") / totalContainers;
    }
  },

  getToolTipText: function(data) {
    var selectedOption = d3.select('select').property("value");
    if (selectedOption === this.memoryLabel) {
      return "<p>Used Memory: " + Math.round(data.get("usedMemoryMB")) + " MB</p>" +
        "<p>Available Memory: " + Math.round(data.get("availMemoryMB")) + " MB</p>";
    }
    else if (selectedOption === this.cpuLabel) {
      return "<p>Used VCores: " + Math.round(data.get("usedVirtualCores")) + " VCores</p>" +
        "<p>Available VCores: " + Math.round(data.get("availableVirtualCores")) + " VCores</p>";
    }
    else if (selectedOption === this.containersLabel) {
        return "<p>Containers: " + Math.round(data.get("numContainers")) + " Containers</p>" +
          "<p>Total Containers: " + this.totalContainers + " Containers</p>";
    }
  }
});
