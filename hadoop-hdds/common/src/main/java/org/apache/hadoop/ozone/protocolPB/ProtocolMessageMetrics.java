package org.apache.hadoop.ozone.protocolPB;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.google.protobuf.ProtocolMessageEnum;

/**
 * Metrics to count all the subtypes of a specific message.
 */
public class ProtocolMessageMetrics implements MetricsSource {

  private String name;

  private String description;

  private Map<ProtocolMessageEnum, AtomicLong> counters =
      new ConcurrentHashMap<>();

  public static ProtocolMessageMetrics create(String name,
      String description, ProtocolMessageEnum[] types) {
    ProtocolMessageMetrics protocolMessageMetrics =
        new ProtocolMessageMetrics(name, description,
            types);
    return protocolMessageMetrics;
  }

  public ProtocolMessageMetrics(String name, String description,
      ProtocolMessageEnum[] values) {
    this.name = name;
    this.description = description;
    for (ProtocolMessageEnum value : values) {
      counters.put(value, new AtomicLong(0));
    }
  }

  public void increment(ProtocolMessageEnum key) {
    counters.get(key).incrementAndGet();
  }

  public void register() {
    DefaultMetricsSystem.instance()
        .register(name, description, this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(name);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(name);
    counters.forEach((key, value) -> {
      builder.addCounter(new MetricName(key.toString(), ""), value.longValue());
    });
    builder.endRecord();
  }

  /**
   * Simple metrics info implementation.
   */
  public static class MetricName implements MetricsInfo {
    private String name;
    private String description;

    public MetricName(String name, String description) {
      this.name = name;
      this.description = description;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String description() {
      return description;
    }
  }
}
