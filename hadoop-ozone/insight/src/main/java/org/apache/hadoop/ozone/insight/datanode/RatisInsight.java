package org.apache.hadoop.ozone.insight.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.insight.BaseInsightPoint;
import org.apache.hadoop.ozone.insight.Component;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.insight.InsightPoint;
import org.apache.hadoop.ozone.insight.LoggerSource;

/**
 * Insight definition for datanode/pipline metrics.
 */
public class RatisInsight extends BaseInsightPoint implements InsightPoint {

  private OzoneConfiguration conf;

  public RatisInsight(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public List<LoggerSource> getRelatedLoggers(boolean verbose) {
    List<LoggerSource> result = new ArrayList<>();
    try {
      ScmClient scmClient = createScmClient(conf);
      Pipeline pipeline = scmClient.listPipelines()
          .stream()
          .filter(d -> d.getNodes().size() > 1)
          .findFirst()
          .get();
      for (DatanodeDetails datanode : pipeline.getNodes()) {
        Component dn =
            new Component(Type.DATANODE, datanode.getUuid().toString(),
                datanode.getHostName(), 9882);
        result
            .add(new LoggerSource(dn, "org.apache.ratis.server.impl",
                defaultLevel(verbose)));
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't enumerate required logs", e);
    }

    return result;
  }

  @Override
  public String getDescription() {
    return "More information about one ratis datanode ring.";
  }

}
