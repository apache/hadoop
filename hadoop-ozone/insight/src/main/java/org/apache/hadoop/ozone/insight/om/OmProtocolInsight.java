package org.apache.hadoop.ozone.insight.om;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB;

/**
 * Insight definition for the OM RPC server.
 */
public class OmProtocolInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(
        new LoggerSource(Type.OM,
            OzoneManagerProtocolServerSideTranslatorPB.class,
            defaultLevel(verbose)));
    return loggers;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> metrics = new ArrayList<>();

    Map<String, String> filter = new HashMap<>();
    filter.put("servername", "OzoneManagerService");

    addRpcMetrics(metrics, Type.OM, filter);

    addProtocolMessageMetrics(metrics, "om_client_protocol", Type.OM,
        OzoneManagerProtocolProtos.Type.values());

    return metrics;
  }

  @Override
  public String getDescription() {
    return "Ozone Manager RPC endpoint";
  }

}
