import Ember from 'ember';
import BaseChartComponent from 'yarn-ui/components/base-chart-component';

export default BaseChartComponent.extend({
  /*
   * data = [{label="xx", value=},{...}]
   */
  renderDonutChart: function(data, title, showLabels = false, 
    middleLabel = "Total", middleValue = undefined) {
    var g = this.chart.g;
    var layout = this.getLayout();
    this.renderTitleAndBG(g, title, layout);

    var total = 0;
    var allZero = true;
    for (var i = 0; i < data.length; i++) {
      total += data[i].value;
      if (data[i].value > 1e-6) {
        allZero = false;
      }
    }

    if (!middleValue) {
      middleValue = total;
    }

    //Width and height
    var h = layout.y2 - layout.y1;

    // 50 is for title
    var outerRadius = (h - 50 - 2 * layout.margin) / 2;
    var innerRadius = outerRadius * 0.618;
    var arc = d3.svg.arc()
      .innerRadius(innerRadius)
      .outerRadius(outerRadius);

    var cx;
    var cy = layout.y1 + 50 + layout.margin + outerRadius;
    if (showLabels) {
      cx = layout.x1 + layout.margin + outerRadius;
    } else {
      cx = (layout.x1 + layout.x2) / 2;
    }

    var pie = d3.layout.pie();
    pie.sort(null);
    pie.value(function(d) {
      var v = d.value;
      // make sure it > 0
      v = Math.max(v, 1e-6);
      return v;
    });

    //Set up groups
    var arcs = g
      .selectAll("g.arc")
      .data(pie(data))
      .enter()
      .append("g")
      .attr("class", "arc")
      .attr("transform", "translate(" + cx + "," + cy + ")");

    function tweenPie(finish) {
      var start = {
        startAngle: 0,
        endAngle: 0
      };
      var i = d3.interpolate(start, finish);
      return function(d) {
        return arc(i(d));
      };
    }

    //Draw arc paths
    var path = arcs.append("path")
      .attr("fill", function(d, i) {
        if (d.value > 1e-6) {
          return this.colors[i];
        } else {
          return "white";
        }
      }.bind(this))
      .attr("d", arc)
      .attr("stroke", function(d, i) {
        if (allZero) {
          return this.colors[i];
        }
      }.bind(this))
      .attr("stroke-dasharray", function(d, i) {
        if (d.value <= 1e-6) {
          return "10,10";
        }
      }.bind(this));
    this.bindTooltip(path);

    // Show labels
    if (showLabels) {
      var lx = layout.x1 + layout.margin + outerRadius * 2 + 30;
      var squareW = 15;
      var margin = 10;

      var select = g.selectAll(".rect")
        .data(data)
        .enter();
      select.append("rect")
        .attr("fill", function(d, i) {
          return this.colors[i];
        }.bind(this))
        .attr("x", lx)
        .attr("y", function(d, i) {
          return layout.y1 + 50 + (squareW + margin) * i + layout.margin;
        })
        .attr("width", squareW)
        .attr("height", squareW);
      select.append("text")
        .attr("x", lx + squareW + margin)
        .attr("y", function(d, i) {
          return layout.y1 + 50 + (squareW + margin) * i + layout.margin + squareW / 2;
        })
        .text(function(d) {
          return d.label + ' = ' + d.value;
        });
    }

    if (middleLabel) {
      var highLightColor = this.colors[0];
      g.append("text").text(middleLabel).attr("x", cx).attr("y", cy - 10).
        attr("class", "donut-highlight-text").attr("fill", highLightColor);
      g.append("text").text(middleValue).attr("x", cx).attr("y", cy + 20).
        attr("class", "donut-highlight-text").attr("fill", highLightColor).
        style("font-size", "30px");
    }

    path.transition()
      .duration(500)
      .attrTween('d', tweenPie);
  },

  draw: function() {
    this.initChart();
    this.renderDonutChart(this.get("data"), this.get("title"), this.get("showLabels"), 
                          this.get("middleLabel"), this.get("middleValue"));
  },

  didInsertElement: function() {
    this.draw();
  },
})