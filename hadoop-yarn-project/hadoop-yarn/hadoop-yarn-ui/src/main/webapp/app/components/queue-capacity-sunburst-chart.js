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
import HrefAddressUtils from 'yarn-ui/utils/href-address-utils';

export default BaseChartComponent.extend({
  queues: undefined,
  nodes: undefined,

  type: "capacity",
  changingType: true,

  initData: function() {
    this.queues = {};
    this.nodes = {};

    this.get("data").forEach(function(o) {
      this.queues[o.id] = o;
    }.bind(this));

    this.initQueue("root", this.nodes);
  },

  initQueue: function(queueName, node) {
    if ((!queueName) || (!this.queues[queueName])) { return; }

    var queue = this.queues[queueName];
    node.name = queueName;
    node.data = {
      label: queueName,
      max: queue.get("maxCapacity"),
      capacity: queueName === "root" ? queue.get("capacity") : queue.get("absCapacity"),
      used: queueName === "root" ? queue.get("usedCapacity") : queue.get("absUsedCapacity"),
      link: HrefAddressUtils.getQueueLink(queueName)
    };
    node.parent = queue.get("parent");
    node.queueData = queue;

    var children = queue.get("children");
    if (children.length > 0) {
      node.children = [];
      children.forEach(function(child) {
        var childNode = {};
        node.children.push(childNode);
        this.initQueue(child, childNode);
      }.bind(this));
    }
  },

  defineGradient: function(name, start, stop) {
    var gradient = this.chart.svg.append("defs")
      .append("linearGradient")
        .attr("id", name)
        .attr("x1", "0%")
        .attr("y1", "50%")
        .attr("x2", "100%")
        .attr("y2", "50%");
    gradient.append("stop")
      .attr("offset", "0%")
      .attr("stop-color", start);
    gradient.append("stop")
      .attr("offset", "100%")
      .attr("stop-color", stop);
  },

  round: function(number, decimals=4) {
    var e = Math.pow(10, decimals);
    return Math.round(number * e) / e;
  },

  changeChartType: function(path, partition, arc) {
    d3.selectAll(".radio-center")
      .style("opacity", 0);
    d3.selectAll("#radio-" + this.type)
      .style("opacity", 1);
    var value = this.type === "capacity" ?
      function(d) { return d.data.capacity; } :
      function(d) { return d.data.used; };
    this.changingType = true;
    path.data(partition.value(value).nodes)
      .transition()
        .duration(1000)
        .attrTween("d", function(finish) {
          var start = {
            x: finish.xOld,
            dx: finish.dxOld
          };
          return function(t) {
            var end = d3.interpolate(start, finish)(t);
            finish.xOld = end.x;
            finish.dxOld = end.dx;
            return arc(end);
          };
        })
        .each("end", function() {
          this.changingType = false;
        }.bind(this));
  },

  renderSunburstChart: function(data, showLabels=false) {
    var g = this.chart.g;
    var layout = this.getLayout();

    var radius = (layout.y2 - layout.y1 - 50 - 2 * layout.margin) / 2;

    var cx;
    var cy = layout.y1 + 50 + layout.margin + radius;
    if (showLabels) {
      cx = layout.x1 + layout.margin + radius;
    } else {
      cx = (layout.x1 + layout.x2) / 2;
    }

    var partition = d3.layout.partition()
      .sort(null)
      .size([2 * Math.PI, radius])
      .value(function(d) { return d.data.capacity; });

    var arc = d3.svg.arc()
      .startAngle(function(d) { return d.x; })
      .endAngle(function(d) { return d.x + d.dx; })
      .innerRadius(function(d) { return d.y; })
      .outerRadius(function(d) { return d.y + d.dy; });

    var underCapColorStart = "#26bbf0", underCapColorEnd = "#1c95c0";
    var overCapColorStart = "#ffbc0b", overCapColorEnd = "#dca41b";
    var underCapColorFunc = d3.interpolate(
      d3.rgb(underCapColorStart), d3.rgb(underCapColorEnd));
    var overCapColorFunc = d3.interpolate(
      d3.rgb(overCapColorStart), d3.rgb(overCapColorEnd));
    this.defineGradient("under", underCapColorStart, underCapColorEnd);
    this.defineGradient("over", overCapColorStart, overCapColorEnd);

    var path = g.datum(data).selectAll("path")
      .data(partition.nodes)
      .enter()
    .append("path")
      .attr("d", arc)
      .style("stroke", "#fff")
      .style("fill", function(d) {
        if (d.data.used > d.data.capacity) {
          return overCapColorFunc(d.data.used / d.data.max);
        }
        return underCapColorFunc(d.data.used / d.data.capacity);
      })
      .each(function(d) {
        d.xOld = d.x;
        d.dxOld = d.dx;
      })
      .attr("transform", "translate(" + cx + "," + cy + ")");

    path.on("mousemove", function(d) {
      if (this.changingType) { return; }

      g.selectAll("path").transition();
      var path = [];
      var current = d;
      while (current) {
        path.push(current);
        current = current.parent;
      }
      g.selectAll("path")
        .style("opacity", 0.3);
      g.selectAll("path")
        .filter(function(d) {
          return path.indexOf(d) !== -1;
        })
        .style("opacity", 1);

      var data = d.data;
      this.tooltip.style("opacity", 0.7);
      this.tooltip.html(
        "<p><b>" + data.label + "</b></p>" +
        "<p>Used: " + this.round(data.used) + "%</p>" +
        "<p>Configured: " + this.round(data.capacity) + "%</p>" +
        "<p>Max: " + this.round(data.max) + "%</p>")
        .style("left", d3.event.pageX + "px")
        .style("top", (d3.event.pageY - 28) + "px");
    }.bind(this));

    path.on("mouseout", function() {
      if (this.changingType) { return; }

      g.selectAll("path")
        .transition()
        .duration(500)
        .style("opacity", 1);
      this.tooltip.style("opacity", 0);
    }.bind(this));

    path.on("click", function (d) {
      var data = d.data;
      if (data.link) {
        this.tooltip.remove();
        document.location.href = data.link;
      }
    }.bind(this));

    if (showLabels) {
      var lx = layout.x1 + layout.margin + radius * 2 + 30;
      var squareW = 15;
      var margin = 10;

      var labels = [
        {label: "Under capacity", gradient: "url(#under)"},
        {label: "Over capacity", gradient: "url(#over)"}
      ];
      var select = g.selectAll("rect")
        .data(labels)
        .enter();
      select.append("rect")
        .attr("fill", function(d) { return d.gradient; })
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
        .text(function(d) { return d.label; });

      g.append("text")
        .attr("x", lx - 2)
        .attr("y", layout.y1 + 75 + (squareW + margin) * 4 + layout.margin)
        .style("font-weight", "bold")
        .text("Chart type");
      var types = [
        {label: "Configured capacity", type: "capacity"},
        {label: "Used capacity", type: "used"}
      ];
      select = g.selectAll("circle")
        .data(types)
        .enter();
      select.append("circle")
        .attr("cx", lx + squareW / 2)
        .attr("cy", function(d, i) {
          return layout.y1 + 75 + (squareW + margin) * (5 + i) + layout.margin;
        })
        .attr("r", squareW / 2)
        .attr("fill", "white")
        .attr("stroke", "black")
        .on("click", function(d) {
          this.type = d.type;
          this.changeChartType(path, partition, arc, d.type);
        }.bind(this));
      select.append("circle")
        .attr("cx", lx + squareW / 2)
        .attr("cy", function(d, i) {
          return layout.y1 + 75 + (squareW + margin) * (5 + i) + layout.margin;
        })
        .attr("r", squareW / 2 - 2)
        .attr("fill", "green")
        .attr("id", function(d) { return "radio-" + d.type; })
        .attr("class", "radio-center")
        .attr("opacity", function(d) {
          return d.type === this.type ? 1 : 0;
        }.bind(this))
        .on("click", function(d) {
          this.type = d.type;
          this.changeChartType(path, partition, arc);
        }.bind(this));
      select.append("text")
        .attr("x", lx + squareW + margin)
        .attr("y", function(d, i) {
          return layout.y1 + 81 + (squareW + margin) * (5 + i) + layout.margin;
        })
        .text(function(d) { return d.label; });
    }

    path.transition()
      .duration(500)
      .attrTween("d", function(finish) {
        var start = {x: 0, dx: 0};
        return function(d) {
          return arc(d3.interpolate(start, finish)(d));
        };
      })
      .each("end", function() {
        this.changingType = false;
      }.bind(this));
  },

  didInsertElement: function() {
    this.initChart();
    this.initData();
    this.renderSunburstChart(this.nodes, this.get("showLabels"));
  },
});
