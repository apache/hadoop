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
  MAX_BAR_HEIGHT: 120,
  MAX_BAR_WIDTH: 30,
  GAP: 5,
  filter: "",
  WIDTH_OF_SAMPLE: 200,

  bindTP: function(element) {
    element.on("mouseover", function() {
      this.tooltip
        .style("left", (d3.event.pageX) + "px")
        .style("top", (d3.event.pageY - 28) + "px");
      element.style("opacity", 1.0);
    }.bind(this))
      .on("mousemove", function() {
        // Handle pie chart case
        var text = element.attr("tooltiptext");

        this.tooltip.style("opacity", 0.9);
        this.tooltip.html(text)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mouseout", function() {
        this.tooltip.style("opacity", 0);
        element.style("opacity", 0.8);
      }.bind(this));

    element.on("click", function() {
      if (element.attr("link")) {
        this.tooltip.remove();
        document.location.href = element.attr("link");
      }
    }.bind(this));
  },

  printSamples: function(n, layout, g, colorTitles) {
    var yOffset = layout.margin * 3;

    for (var i = 0; i < n; i++) {
      var xOffset = layout.x2 - this.WIDTH_OF_SAMPLE - layout.margin;
      g.append("rect").
        attr("fill", this.colors[i]).
        attr("x", xOffset).
        attr("y", yOffset).
        attr("width", 20).
        attr("height", 20);

      g.append("text").
        attr("x", xOffset + 30).
        attr("y", yOffset + 10).
        text(colorTitles[i]);

      yOffset = yOffset + 30;
    }
  },

  // data:
  //    [[{value=xx, bindText=xx}, {value=yy, bindText=yy}],  [  ...    ]]
  //     __________________________________________________   ___________
  //                          bar-1                              bar-2
  show: function (data, title, colorTitles) {
    var width = this.MAX_BAR_WIDTH;
    var height = this.MAX_BAR_HEIGHT;

    this.chart.g.remove();
    this.chart.g = this.chart.svg.append("g");
    var g = this.chart.g;
    var layout = this.getLayout();
    layout.margin = 50;

    var nBarPerRow = Math.floor((layout.x2 - layout.x1 - 3 * layout.margin -
      this.WIDTH_OF_SAMPLE) /
      (width + this.GAP));

    var xOffset;
    var yOffset = layout.margin * 2;

    var maxValue = 0;
    var maxN = 0;

    var i = 0;
    var j = 0;

    for (i = 0; i < data.length; i++) {
      var total = 0;
      for (j = 0; j < data[i].length; j++) {
        total += data[i][j].value;
      }

      if (total > maxValue) {
        maxValue = total;
      }
      if (data[i].length > maxN) {
        maxN = data[i].length;
      }
    }

    // print samples
    this.printSamples(maxN, layout, g, colorTitles);

    // print data
    data.sort(function(a, b) {
      return b[0].value - a[0].value;
    });

    for (i = 0; i < data.length; i++) {
      if (i % nBarPerRow === 0) {
        xOffset = layout.margin;
        yOffset += layout.margin + height;
      }

      var leftTopY = yOffset;
      for (j = 0; j < data[i].length; j++) {
        var dy = data[i][j].value * height / maxValue;
        if (dy > 0) {
          leftTopY = leftTopY - dy;

          var node = g.append("rect").
            attr("fill", this.colors[j]).
            attr("x", xOffset).
            attr("y", leftTopY).
            attr("width", width).
            attr("height", dy).
            attr("tooltiptext",
              (data[i][j].bindText) ? data[i][j].bindText : data[i][j].value).
            attr("link", data[i][j].link)
            .style("opacity", 0.8);

          this.bindTP(node);
        }
      }

      if (data[i].length === 1) {
        g.append("text")
          .text(data[i][0].value)
          .attr("y", leftTopY - 10)
          .attr("x", xOffset + width / 2)
          .attr("class", "heatmap-cell")
          .style("fill", "black");
      }

      xOffset += width + this.GAP;
    }

    layout.y2 = yOffset + layout.margin;
    this.adjustMaxHeight(layout.y2);
    this.renderTitleAndBG(g, title, layout, false);
  },

  draw: function() {
    this.initChart(true);
    //Mock.initMockNodesData(this);

    // mock data
    var arr = [];
    for (var i = 0; i < 5; i++) {
      var subArr = [];
      for (var j = 0; j < Math.random() * 4 + 1; j++) {
        subArr.push({
          value : Math.abs(Math.random())
        });
      }
      arr.push(subArr);
    }

    this.show(
      arr, this.get("title"));
  },

  didInsertElement: function () {
    this.draw();
  },

  actions: {
    applyFilter: function(event) {
      this.filter = event.srcElement.value;
      this.didInsertElement();
    }
  }
});