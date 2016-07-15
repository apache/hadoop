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

const INBETWEEN_HEIGHT = 130;

export default Ember.Component.extend({
  // Map: <queue-name, queue>
  map : undefined,

  // Normalized data for d3
  treeData: undefined,

  // folded queues, folded[<queue-name>] == true means <queue-name> is folded
  foldedQueues: { },

  // maxDepth
  maxDepth: 0,

  // num of leaf queue, folded queue is treated as leaf queue
  numOfLeafQueue: 0,

  // mainSvg
  mainSvg: undefined,

  // Init data
  initData: function() {
    this.map = { };
    this.treeData = { };
    this.maxDepth = 0;
    this.numOfLeafQueue = 0;

    this.get("model")
      .forEach(function(o) {
        this.map[o.id] = o;
      }.bind(this));

    var selected = this.get("selected");

    this.initQueue("root", 1, this.treeData);
  },

  // get Children array of given queue
  getChildrenNamesArray: function(q) {
    var namesArr = [];

    // Folded queue's children is empty
    if (this.foldedQueues[q.get("name")]) {
      return namesArr;
    }

    var names = q.get("children");
    if (names) {
      names.forEach(function(name) {
        namesArr.push(name);
      });
    }

    return namesArr;
  },

  // Init queues
  initQueue: function(queueName, depth, node) {
    if ((!queueName) || (!this.map[queueName])) {
      // Queue is not existed
      return;
    }

    if (depth > this.maxDepth) {
      this.maxDepth = this.maxDepth + 1;
    }

    var queue = this.map[queueName];

    var names = this.getChildrenNamesArray(queue);

    node.name = queueName;
    node.parent = queue.get("parent");
    node.queueData = queue;

    if (names.length > 0) {
      node.children = [];

      names.forEach(function(name) {
        var childQueueData = {};
        node.children.push(childQueueData);
        this.initQueue(name, depth + 1, childQueueData);
      }.bind(this));
    } else {
      this.numOfLeafQueue = this.numOfLeafQueue + 1;
    }
  },

  update: function(source, root, tree, diagonal) {
    var duration = 300;
    var i = 0;

    // Compute the new tree layout.
    var nodes = tree.nodes(root).reverse();
    var links = tree.links(nodes);

    // Normalize for fixed-depth.
    nodes.forEach(function(d) { d.y = d.depth * 200; });

    // Update the nodes…
    var node = this.mainSvg.selectAll("g.node")
      .data(nodes, function(d) { return d.id || (d.id = ++i); });

    // Enter any new nodes at the parent's previous position.
    var nodeEnter = node.enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { return "translate(" + source.y0 + "," + source.x0 + ")"; })
      .on("mouseover", function(d,i){
        if (d.queueData.get("name") != this.get("selected")) {
            document.location.href = "#/yarn-queues/" + d.queueData.get("name");
        }

        Ember.run.later(this, function () {
          var treeWidth = this.maxDepth * 200;
          var treeHeight = this.numOfLeafQueue * INBETWEEN_HEIGHT;
          var tree = d3.layout.tree().size([treeHeight, treeWidth]);
          var diagonal = d3.svg.diagonal()
            .projection(function(d) { return [d.y, d.x]; });

          this.update(this.treeData, this.treeData, tree, diagonal);
        }, 100);

      }.bind(this))
    .on("click", function (d) {
      document.location.href = "#/yarn-queue/" + d.queueData.get("name");
    });

    nodeEnter.append("circle")
      .attr("r", 1e-6)
      .style("fill", function(d) {
        var usedCap = d.queueData.get("usedCapacity");
        if (usedCap <= 60.0) {
          return "LimeGreen";
        } else if (usedCap <= 100.0) {
          return "DarkOrange";
        } else {
          return "LightCoral";
        }
      });

    // append percentage
    nodeEnter.append("text")
      .attr("x", function(d) { return 0; })
      .attr("dy", ".35em")
      .attr("fill", "white")
      .attr("text-anchor", function(d) { return "middle"; })
      .text(function(d) {
        var usedCap = d.queueData.get("usedCapacity");
        if (usedCap >= 100.0) {
          return usedCap.toFixed(0) + "%";
        } else {
          return usedCap.toFixed(1) + "%";
        }
      })
      .style("fill-opacity", 1e-6);

    // append queue name
    nodeEnter.append("text")
      .attr("x", "0px")
      .attr("dy", "45px")
      .attr("text-anchor", "middle")
      .text(function(d) { return d.name; })
      .style("fill-opacity", 1e-6);

    // Transition nodes to their new position.
    var nodeUpdate = node.transition()
      .duration(duration)
      .attr("transform", function(d) { return "translate(" + d.y + "," + d.x + ")"; });

    nodeUpdate.select("circle")
      .attr("r", 30)
      .attr("href", 
        function(d) {
          return "#/yarn-queues/" + d.queueData.get("name");
        })
      .style("stroke-width", function(d) {
        if (d.queueData.get("name") == this.get("selected")) {
          return 7;
        } else {
          return 2;
        }
      }.bind(this))
      .style("stroke", function(d) {
        if (d.queueData.get("name") == this.get("selected")) {
          return "gray";
        } else {
          return "gray";
        }
      }.bind(this));

    nodeUpdate.selectAll("text")
      .style("fill-opacity", 1);

    // Transition exiting nodes to the parent's new position.
    var nodeExit = node.exit().transition()
      .duration(duration)
      .attr("transform", function(d) { return "translate(" + source.y + "," + source.x + ")"; })
      .remove();

    nodeExit.select("circle")
      .attr("r", 1e-6);

    nodeExit.select("text")
      .style("fill-opacity", 1e-6);

    // Update the links…
    var link = this.mainSvg.selectAll("path.link")
      .data(links, function(d) { return d.target.id; });

    // Enter any new links at the parent's previous position.
    link.enter().insert("path", "g")
      .attr("class", "link")
      .attr("d", function(d) {
      var o = {x: source.x0, y: source.y0};
      return diagonal({source: o, target: o});
      });

    // Transition links to their new position.
    link.transition()
      .duration(duration)
      .attr("d", diagonal);

    // Transition exiting nodes to the parent's new position.
    link.exit().transition()
      .duration(duration)
      .attr("d", function(d) {
      var o = {x: source.x, y: source.y};
      return diagonal({source: o, target: o});
      })
      .remove();

    // Stash the old positions for transition.
    nodes.forEach(function(d) {
      d.x0 = d.x;
      d.y0 = d.y;
    });
  },

  reDraw: function() {
    this.initData();

    var margin = {top: 20, right: 120, bottom: 20, left: 120};
    var treeWidth = this.maxDepth * 200;
    var treeHeight = this.numOfLeafQueue * INBETWEEN_HEIGHT;
    var width = treeWidth + margin.left + margin.right;
    var height = treeHeight + margin.top + margin.bottom;
    var layout = { };

    if (this.mainSvg) {
      this.mainSvg.remove();
    }

    this.mainSvg = d3.select("#" + this.get("parentId")).append("svg")
      .attr("width", width)
      .attr("height", height)
      .attr("class", "tree-selector")
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var tree = d3.layout.tree().size([treeHeight, treeWidth]);

    var diagonal = d3.svg.diagonal()
      .projection(function(d) { return [d.y, d.x]; });

    var root = this.treeData;
    root.x0 = height / 2;
    root.y0 = 0;

    d3.select(self.frameElement).style("height", height);

    this.update(root, root, tree, diagonal);
  },

  didInsertElement: function() {
   this.reDraw();
  }
});