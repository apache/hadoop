package org.apache.hadoop.ozone.insight;

import java.util.HashMap;
import java.util.Map;

/**
 * Definition of one displayable hadoop metrics.
 */
public class MetricDisplay {

  /**
   * Prometheus metrics name.
   */
  private String id;

  /**
   * Human readable definition of the metrhics.
   */
  private String description;

  /**
   * Prometheus metrics tag to filter out the right metrics.
   */
  private Map<String, String> filter;

  public MetricDisplay(String description, String id) {
    this(description, id, new HashMap<>());
  }

  public MetricDisplay(String description, String id,
      Map<String, String> filter) {
    this.id = id;
    this.description = description;
    this.filter = filter;
  }

  public String getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, String> getFilter() {
    return filter;
  }

  public boolean checkLine(String line) {
    return false;
  }
}
