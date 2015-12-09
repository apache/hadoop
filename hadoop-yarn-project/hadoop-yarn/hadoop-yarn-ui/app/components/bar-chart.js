import Ember from 'ember';
import BaseChartComponent from 'yarn-ui/components/base-chart-component';

export default BaseChartComponent.extend({
  // data: 
  //    [{label=label1, value=value1}, ...]
  //    ...
  renderBarChart: function(data, title, textWidth = 50) {
    var g = this.chart.g;
    var layout = this.getLayout();
    this.renderTitleAndBG(g, title, layout);

    var maxValue = -1;
    for (var i = 0; i < data.length; i++) {
      if (data[i] instanceof Array) {
        if (data[i][0].value > maxValue) {
          maxValue = data[i][0].value;
        }
      } else {
        if (data[i].value > maxValue) {
          maxValue = data[i].value;
        }
      }
    }

    var singleBarHeight = 30;

    // 50 is for text
    var maxBarWidth = layout.x2 - layout.x1 - 2 * layout.margin - textWidth - 50;

    // 30 is for title
    var maxBarsHeight = layout.y2 - layout.y1 - 2 * layout.margin - 30;
    var gap = (maxBarsHeight - data.length * singleBarHeight) / (data.length -
      1);

    var xScaler = d3.scale.linear()
      .domain([0, maxValue])
      .range([0, maxBarWidth]);

    // show bar text
    for (var i = 0; i < data.length; i++) {
      g.append("text")
        .text(
          function() {
            return data[i].label;
          })
        .attr("y", function() {
          return layout.y1 + singleBarHeight / 2 + layout.margin + (gap +
            singleBarHeight) * i + 30;
        })
        .attr("x", layout.x1 + layout.margin);
    }

    // show bar
    var bar = g.selectAll("bars")
      .data(data)
      .enter()
      .append("rect")
      .attr("y", function(d, i) {
        return layout.y1 + 30 + layout.margin + (gap + singleBarHeight) * i;
      })
      .attr("x", layout.x1 + layout.margin + textWidth)
      .attr("height", singleBarHeight)
      .attr("fill", function(d, i) {
        return this.colors[i];
      }.bind(this))
      .attr("width", 0);

    this.bindTooltip(bar);

    bar.transition()
      .duration(500)
      .attr("width", function(d) {
        var w;
        w = xScaler(d.value);
        // At least each item has 3 px
        w = Math.max(w, 3);
        return w;
      });

    // show bar value
    for (var i = 0; i < data.length; i++) {
      g.append("text")
        .text(
          function() {
            return data[i].value;
          })
        .attr("y", function() {
          return layout.y1 + singleBarHeight / 2 + layout.margin + (gap +
            singleBarHeight) * i + 30;
        })
        .attr("x", layout.x1 + layout.margin + textWidth + 15 + xScaler(data[i].value));
    }
  },

  draw: function() {
    this.initChart();
    this.renderBarChart(this.get("data"), this.get("title"), this.get("textWidth"));
  },

  didInsertElement: function() {
    this.draw();
  },
})