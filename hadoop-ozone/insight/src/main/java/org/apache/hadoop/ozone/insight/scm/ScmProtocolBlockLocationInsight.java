package org.apache.hadoop.ozone.insight.scm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdds.protocol.proto.ScmBlockLocationProtocolProtos;
import org.apache.hadoop.hdds.scm.server.SCMBlockProtocolServer;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.LoggerSource;
import org.apache.hadoop.ozone.insight.MetricGroupDisplay;
import org.apache.hadoop.ozone.protocolPB.ScmBlockLocationProtocolServerSideTranslatorPB;

/**
 * Insight metric to check the SCM block location protocol behaviour.
 */
public class ScmProtocolBlockLocationInsight extends BaseInsightPoint {

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> loggers = new ArrayList<>();
    loggers.add(
        new LoggerSource(Type.SCM,
            ScmBlockLocationProtocolServerSideTranslatorPB.class,
            defaultLevel(verbose)));
    new LoggerSource(Type.SCM,
        SCMBlockProtocolServer.class,
        defaultLevel(verbose));
    return loggers;
  }

  @Override
  public List<MetricGroupDisplay> getMetrics() {
    List<MetricGroupDisplay> metrics = new ArrayList<>();

    Map<String, String> filter = new HashMap<>();
    filter.put("servername", "StorageContainerLocationProtocolService");

    addRpcMetrics(metrics, Type.SCM, filter);

    addProtocolMessageMetrics(metrics, "scm_block_location_protocol",
        Type.SCM, ScmBlockLocationProtocolProtos.Type.values());

    return metrics;
  }

  @Override
  public String getDescription() {
    return "SCM Block location protocol endpoint";
  }

}
