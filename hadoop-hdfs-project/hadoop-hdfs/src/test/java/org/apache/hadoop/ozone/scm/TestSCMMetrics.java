/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm;

import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerReport;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.ozone.scm.container.placement.metrics.SCMMetrics;
import org.apache.hadoop.ozone.scm.node.SCMNodeManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This class tests the metrics of Storage Container Manager.
 */
public class TestSCMMetrics {
  /**
   * Set the timeout for each test.
   */
  @Rule
  public Timeout testTimeout = new Timeout(90000);

  private static MiniOzoneClassicCluster cluster = null;

  @Test
  public void testContainerMetrics() throws Exception {
    int nodeCount = 2;
    int numReport = 2;
    long size = OzoneConsts.GB * 5;
    long used = OzoneConsts.GB * 2;
    long readBytes = OzoneConsts.GB * 1;
    long writeBytes = OzoneConsts.GB * 2;
    int keyCount = 1000;
    int readCount = 100;
    int writeCount = 50;
    OzoneConfiguration conf = new OzoneConfiguration();

    try {
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
          .numDataNodes(nodeCount).build();

      ContainerStat stat = new ContainerStat(size, used, keyCount, readBytes,
          writeBytes, readCount, writeCount);
      StorageContainerManager scmManager = cluster.getStorageContainerManager();

      ContainerReportsRequestProto request = createContainerReport(numReport,
          stat, null);
      String fstDatanodeID = request.getDatanodeID().getDatanodeUuid();
      scmManager.sendContainerReport(request);

      // verify container stat metrics
      MetricsRecordBuilder scmMetrics = getMetrics(SCMMetrics.SOURCE_NAME);
      assertEquals(size * numReport,
          getLongGauge("LastContainerReportSize", scmMetrics));
      assertEquals(used * numReport,
          getLongGauge("LastContainerReportUsed", scmMetrics));
      assertEquals(readBytes * numReport,
          getLongGauge("LastContainerReportReadBytes", scmMetrics));
      assertEquals(writeBytes * numReport,
          getLongGauge("LastContainerReportWriteBytes", scmMetrics));

      assertEquals(keyCount * numReport,
          getLongGauge("LastContainerReportKeyCount", scmMetrics));
      assertEquals(readCount * numReport,
          getLongGauge("LastContainerReportReadCount", scmMetrics));
      assertEquals(writeCount * numReport,
          getLongGauge("LastContainerReportWriteCount", scmMetrics));

      // add one new report
      request = createContainerReport(1, stat, null);
      String sndDatanodeID = request.getDatanodeID().getDatanodeUuid();
      scmManager.sendContainerReport(request);

      scmMetrics = getMetrics(SCMMetrics.SOURCE_NAME);
      assertEquals(size * (numReport + 1),
          getLongCounter("ContainerReportSize", scmMetrics));
      assertEquals(used * (numReport + 1),
          getLongCounter("ContainerReportUsed", scmMetrics));
      assertEquals(readBytes * (numReport + 1),
          getLongCounter("ContainerReportReadBytes", scmMetrics));
      assertEquals(writeBytes * (numReport + 1),
          getLongCounter("ContainerReportWriteBytes", scmMetrics));

      assertEquals(keyCount * (numReport + 1),
          getLongCounter("ContainerReportKeyCount", scmMetrics));
      assertEquals(readCount * (numReport + 1),
          getLongCounter("ContainerReportReadCount", scmMetrics));
      assertEquals(writeCount * (numReport + 1),
          getLongCounter("ContainerReportWriteCount", scmMetrics));

      // Re-send reports but with different value for validating
      // the aggregation.
      stat = new ContainerStat(100, 50, 3, 50, 60, 5, 6);
      scmManager.sendContainerReport(createContainerReport(1, stat,
          fstDatanodeID));

      stat = new ContainerStat(1, 1, 1, 1, 1, 1, 1);
      scmManager.sendContainerReport(createContainerReport(1, stat,
          sndDatanodeID));

      // the global container metrics value should be updated
      scmMetrics = getMetrics(SCMMetrics.SOURCE_NAME);
      assertEquals(101, getLongCounter("ContainerReportSize", scmMetrics));
      assertEquals(51, getLongCounter("ContainerReportUsed", scmMetrics));
      assertEquals(51, getLongCounter("ContainerReportReadBytes", scmMetrics));
      assertEquals(61, getLongCounter("ContainerReportWriteBytes", scmMetrics));

      assertEquals(4, getLongCounter("ContainerReportKeyCount", scmMetrics));
      assertEquals(6, getLongCounter("ContainerReportReadCount", scmMetrics));
      assertEquals(7, getLongCounter("ContainerReportWriteCount", scmMetrics));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testStaleNodeContainerReport() throws Exception {
    int nodeCount = 2;
    int numReport = 2;
    long size = OzoneConsts.GB * 5;
    long used = OzoneConsts.GB * 2;
    long readBytes = OzoneConsts.GB * 1;
    long writeBytes = OzoneConsts.GB * 2;
    int keyCount = 1000;
    int readCount = 100;
    int writeCount = 50;
    OzoneConfiguration conf = new OzoneConfiguration();

    try {
      cluster = new MiniOzoneClassicCluster.Builder(conf)
          .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
          .numDataNodes(nodeCount).build();

      ContainerStat stat = new ContainerStat(size, used, keyCount, readBytes,
          writeBytes, readCount, writeCount);
      StorageContainerManager scmManager = cluster.getStorageContainerManager();

      DataNode dataNode = cluster.getDataNodes().get(0);
      String datanodeUuid = dataNode.getDatanodeId().getDatanodeUuid();
      ContainerReportsRequestProto request = createContainerReport(numReport,
          stat, datanodeUuid);
      scmManager.sendContainerReport(request);

      MetricsRecordBuilder scmMetrics = getMetrics(SCMMetrics.SOURCE_NAME);
      assertEquals(size * numReport,
          getLongCounter("ContainerReportSize", scmMetrics));
      assertEquals(used * numReport,
          getLongCounter("ContainerReportUsed", scmMetrics));
      assertEquals(readBytes * numReport,
          getLongCounter("ContainerReportReadBytes", scmMetrics));
      assertEquals(writeBytes * numReport,
          getLongCounter("ContainerReportWriteBytes", scmMetrics));

      assertEquals(keyCount * numReport,
          getLongCounter("ContainerReportKeyCount", scmMetrics));
      assertEquals(readCount * numReport,
          getLongCounter("ContainerReportReadCount", scmMetrics));
      assertEquals(writeCount * numReport,
          getLongCounter("ContainerReportWriteCount", scmMetrics));

      // reset stale interval time to move node from healthy to stale
      SCMNodeManager nodeManager = (SCMNodeManager) cluster
          .getStorageContainerManager().getScmNodeManager();
      nodeManager.setStaleNodeIntervalMs(100);

      // verify the metrics when node becomes stale
      GenericTestUtils.waitFor(() -> {
        MetricsRecordBuilder metrics = getMetrics(SCMMetrics.SOURCE_NAME);
        return 0 == getLongCounter("ContainerReportSize", metrics)
            && 0 == getLongCounter("ContainerReportUsed", metrics)
            && 0 == getLongCounter("ContainerReportReadBytes", metrics)
            && 0 == getLongCounter("ContainerReportWriteBytes", metrics)
            && 0 == getLongCounter("ContainerReportKeyCount", metrics)
            && 0 == getLongCounter("ContainerReportReadCount", metrics)
            && 0 == getLongCounter("ContainerReportWriteCount", metrics);
      }, 1000, 60000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private ContainerReportsRequestProto createContainerReport(int numReport,
      ContainerStat stat, String datanodeUuid) {
    StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto.Builder
        reportsBuilder = StorageContainerDatanodeProtocolProtos
        .ContainerReportsRequestProto.newBuilder();

    for (int i = 0; i < numReport; i++) {
      ContainerReport report = new ContainerReport(
          UUID.randomUUID().toString(), DigestUtils.sha256Hex("Simulated"));
      report.setSize(stat.getSize().get());
      report.setBytesUsed(stat.getUsed().get());
      report.setReadCount(stat.getReadCount().get());
      report.setReadBytes(stat.getReadBytes().get());
      report.setKeyCount(stat.getKeyCount().get());
      report.setWriteCount(stat.getWriteCount().get());
      report.setWriteBytes(stat.getWriteBytes().get());
      reportsBuilder.addReports(report.getProtoBufMessage());
    }

    DatanodeID datanodeID;
    if (datanodeUuid == null) {
      datanodeID = SCMTestUtils.getDatanodeID();
    } else {
      datanodeID = new DatanodeID("null", "null", datanodeUuid, 0, 0, 0, 0);
    }

    reportsBuilder.setDatanodeID(datanodeID.getProtoBufMessage());
    reportsBuilder.setType(StorageContainerDatanodeProtocolProtos
        .ContainerReportsRequestProto.reportType.fullReport);
    return reportsBuilder.build();
  }
}
