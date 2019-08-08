package org.apache.hadoop.ozone.insight;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.ozone.insight.Component.Type;

/**
 * Definition of a group of metrics which can be displayed.
 */
public class MetricGroupDisplay {

  /**
   * List fhe included metrics.
   */
  private List<MetricDisplay> metrics = new ArrayList<>();

  /**
   * Name of the component which includes the metrics (scm, om,...).
   */
  private Component component;

  /**
   * Human readable description.
   */
  private String description;

  public MetricGroupDisplay(Component component, String description) {
    this.component = component;
    this.description = description;
  }

  public MetricGroupDisplay(Type componentType, String metricName) {
    this(new Component(componentType), metricName);
  }

  public List<MetricDisplay> getMetrics() {
    return metrics;
  }

  public void addMetrics(MetricDisplay item) {
    this.metrics.add(item);
  }

  public String getDescription() {
    return description;
  }

  public Component getComponent() {
    return component;
  }
}
