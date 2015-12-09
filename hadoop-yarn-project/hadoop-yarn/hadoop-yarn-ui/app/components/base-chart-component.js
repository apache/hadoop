import Ember from 'ember';

export default Ember.Component.extend({
  chart: undefined,
  tooltip : undefined,
  colors: d3.scale.category10().range(),

  initChart: function() {
    this.chart = {
      svg: undefined,
      g: undefined,
      h: 0,
      w: 0,
      tooltip: undefined
    };

    // Init tooltip if it is not initialized
    this.tooltip = d3.select("#chart-tooltip");
    if (!this.tooltip) {
      this.tooltip = d3.select("body")
        .append("div")
        .attr("class", "tooltip")
        .attr("id", "chart-tooltip")
        .style("opacity", 0);
    }

    // Init svg
    var svg = this.chart.svg;
    if (svg) {
      svg.remove();
    }

    var parentId = this.get("parentId");
    var parent = d3.select("#" + parentId);
    var bbox = parent.node().getBoundingClientRect();
    this.chart.w = bbox.width - 30;

    var ratio = 0.75; // 4:3 by default
    if (this.get("ratio")) {
      ratio = this.get("ratio");
    }
    this.chart.h = bbox.width * ratio;

    if (this.get("maxHeight")) {
      this.chart.h = Math.min(this.get("maxHeight"), this.chart.h);
    }

    this.chart.svg = parent.append("svg")
      .attr("width", this.chart.w)
      .attr("height", this.chart.h);

    this.chart.g = this.chart.svg.append("g");
  },

  renderTitleAndBG: function(g, title, layout) {
    var bg = g.append("g");
    bg.append("text")
      .text(title)
      .attr("x", (layout.x1 + layout.x2) / 2)
      .attr("y", layout.y1 + layout.margin + 20)
      .attr("class", "chart-title");

    bg.append("rect")
      .attr("x", layout.x1)
      .attr("y", layout.y1)
      .attr("width", layout.x2 - layout.x1)
      .attr("height", layout.y2 - layout.y1)
      .attr("class", "chart-frame");
  },

  bindTooltip: function(d) {
    d.on("mouseover", function(d) {
        this.tooltip
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mousemove", function(d) {
        // Handle pie chart case
        var data = d;
        if (d.data) {
          data = d.data;
        }

        this.tooltip.style("opacity", .9);
        this.tooltip.html(data.label + " = " + data.value)
          .style("left", (d3.event.pageX) + "px")
          .style("top", (d3.event.pageY - 28) + "px");
      }.bind(this))
      .on("mouseout", function(d) {
        this.tooltip.style("opacity", 0);
      }.bind(this));
  },

  getLayout: function() {
    var x1 = 0;
    var y1 = 0;
    var x2 = this.chart.w;
    var y2 = this.chart.h;

    var layout = {
      x1: x1,
      y1: y1,
      x2: x2 - 10,
      y2: y2 - 10,
      margin: 10
    };
    return layout;
  },
});
