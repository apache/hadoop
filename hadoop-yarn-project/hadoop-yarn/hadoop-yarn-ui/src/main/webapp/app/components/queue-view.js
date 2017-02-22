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
import ChartUtilsMixin from 'yarn-ui/mixins/charts-utils';

export default Ember.Component.extend(ChartUtilsMixin, {
  queues: {
    data: undefined,
    foldedQueues: {},
    selectedQueueCircle: undefined,
    maxDepth: -1,
  },

  queueColors: d3.scale.category20().range(),

  renderQueue: function (now, depth, sequence) {
    if (depth > this.queues.maxDepth) {
      this.queues.maxDepth = depth;
    }

    var cx = 20 + depth * 30;
    var cy = 20 + sequence * 30;
    var name = now.get("name");

    var g = this.queues.dataGroup.append("g")
      .attr("id", "queue-" + name + "-g");

    var folded = this.queues.foldedQueues[name];
    var isParentQueue = false;

    // render its children
    var children = [];
    var childrenNames = now.get("children");
    if (childrenNames) {
      childrenNames.forEach(function (name) {
        isParentQueue = true;
        var child = this.queues.data[name];
        if (child) {
          children.push(child);
        }
      }.bind(this));
    }
    if (folded) {
      children = [];
    }
    var linefunction = d3.svg.line()
      .interpolate("basis")
      .x(function (d) {
        return d.x;
      })
      .y(function (d) {
        return d.y;
      });

    for (var i = 0; i < children.length; i++) {
      sequence = sequence + 1;
      // Get center of children queue
      var cc = this.renderQueue(children[i],
        depth + 1, sequence);
      g.append("path")
        .attr("class", "queue")
        .attr("d", linefunction([{
          x: cx,
          y: cy
        }, {
          x: cc.x - 20,
          y: cc.y
        }, cc]));
    }

    var circle = g.append("circle")
      .attr("cx", cx)
      .attr("cy", cy)
      .attr("class", "queue");

    circle.on('mouseover', function () {
    }.bind(this));
    circle.on('mouseout', function () {
      if (circle !== this.queues.selectedQueueCircle) {
        circle.style("fill", this.queueColors[0]);
      }
    }.bind(this));
    circle.on('click', function () {
      circle.style("fill", this.queueColors[2]);
      var pre = this.queues.selectedQueueCircle;
      this.queues.selectedQueueCircle = circle;
      if (pre) {
        pre.on('mouseout')();
      }
      this.renderCharts(name);
    }.bind(this));
    circle.on('dblclick', function () {
      if (!isParentQueue) {
        return;
      }

      if (this.queues.foldedQueues[name]) {
        delete this.queues.foldedQueues[name];
      } else {
        this.queues.foldedQueues[name] = now;
      }
      this.renderQueues();
    }.bind(this));

    var text = name;
    if (folded) {
      text = name + " (+)";
    }

    // print queue's name
    g.append("text")
      .attr("x", cx + 30)
      .attr("y", cy + 5)
      .text(text)
      .attr("class", "queue");

    return {
      x: cx,
      y: cy
    };
  },

  renderQueues: function () {
    if (this.queues.dataGroup) {
      this.queues.dataGroup.remove();
    }
    // render queues
    this.queues.dataGroup = this.canvas.svg.append("g")
      .attr("id", "queues-g");

    if (this.queues.data) {
      this.renderQueue(this.queues.data['root'], 0, 0);

    }
  },

  draw: function () {
    this.queues.data = {};
    this.get("model")
      .forEach(function (o) {
        this.queues.data[o.id] = o;
      }.bind(this));

    // get w/h of the svg
    var bbox = d3.select("#main-container")
      .node()
      .getBoundingClientRect();
    this.canvas.w = bbox.width;
    this.canvas.h = Math.max(Object.keys(this.queues.data)
        .length * 35, 1500);

    this.canvas.svg = d3.select("#main-container")
      .append("svg")
      .attr("width", this.canvas.w)
      .attr("height", this.canvas.h)
      .attr("id", "main-svg");

    this.renderBackground();

    this.renderQueues();
    this.renderCharts("root");
  },

  didInsertElement: function () {
    this.draw();
  },

  /*
   * data = [{label="xx", value=},{...}]
   */
  renderTable: function (data) {
    d3.select("#main-svg")
      .append('table')
      .selectAll('tr')
      .data(data)
      .enter()
      .append('tr')
      .selectAll('td')
      .data(function (d) {
        return d;
      })
      .enter()
      .append('td')
      .text(function (d) {
        return d;
      });
  },

  renderQueueCapacities: function (queue, layout) {
    // Render bar chart
    this.renderCells(this.charts.g, [{
      label: "Cap",
      value: queue.get("capacity")
    }, {
      label: "MaxCap",
      value: queue.get("maxCapacity")
    }, {
      label: "UsedCap",
      value: queue.get("usedCapacity")
    }], "Queue Capacities", layout, 60);
  },

  renderChildrenCapacities: function (queue, layout) {
    var data = [];
    var children = queue.get("children");
    if (children) {
      for (var i = 0; i < children.length; i++) {
        var child = this.queues.data[children[i]];
        data.push({
          label: child.get("name"),
          value: child.get("capacity")
        });
      }
    }

    this.renderDonutChart(this.charts.g, data, "Children Capacities", layout, true);
  },

  renderChildrenUsedCapacities: function (queue, layout) {
    var data = [];
    var children = queue.get("children");
    if (children) {
      for (var i = 0; i < children.length; i++) {
        var child = this.queues.data[children[i]];
        data.push({
          label: child.get("name"),
          value: child.get("usedCapacity")
        });
      }
    }

    this.renderDonutChart(this.charts.g, data, "Children Used Capacities", layout, true);
  },

  renderLeafQueueUsedCapacities: function (layout) {
    var leafQueueUsedCaps = [];
    for (var queueName in this.queues.data) {
      var q = this.queues.data[queueName];
      if ((!q.get("children")) || q.get("children")
          .length === 0) {
        // it's a leafqueue
        leafQueueUsedCaps.push({
          label: q.get("name"),
          value: q.get("usedCapacity")
        });
      }
    }

    this.renderDonutChart(this.charts.g, leafQueueUsedCaps, "LeafQueues Used Capacities",
      layout, true);
  },

  renderCharts: function (queueName) {
    this.charts.leftBannerLen = this.queues.maxDepth * 30 + 100;
    this.initCharts();

    var queue = this.queues.data[queueName];
    var idx = 0;

    if (queue.get("name") === "root") {
      this.renderLeafQueueUsedCapacities(this.getLayout(idx++));
    }
    if (queue.get("name") !== "root") {
      this.renderQueueCapacities(queue, this.getLayout(idx++));
    }
    if (queue.get("children") && queue.get("children")
        .length > 0) {
      this.renderChildrenCapacities(queue, this.getLayout(idx++));
      this.renderChildrenUsedCapacities(queue, this.getLayout(idx++));
    }
  },
});
