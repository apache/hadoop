package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;

/**
 * Insight definition to check internal events.
 */
public class EventQueueInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers
        .add(new LoggerSource(Type.SCM, EventQueue.class,
            defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public String getDescription() {
    return "Information about the internal async event delivery";
  }

}
