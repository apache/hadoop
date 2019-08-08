package org.apache.hadoop.ozone.insight;

import java.util.List;

/**
 * Definition of a specific insight points.
 */
public interface InsightPoint {

  /**
   * Human readdable description.
   */
  String getDescription();

  /**
   * List of the related loggers.
   */
  List<LoggerSource> getRelatedLoggers(boolean verbose);

  /**
   * List of the related metrics.
   */
  List<MetricGroupDisplay> getMetrics();

  /**
   * List of the configuration classes.
   */
  List<Class> getConfigurationClasses();



}
