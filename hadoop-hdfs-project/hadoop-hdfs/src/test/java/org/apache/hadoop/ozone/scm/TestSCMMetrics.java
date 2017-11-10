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

import static org.apache.hadoop.test.MetricsAsserts.getLongGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneClassicCluster;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerReport;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsRequestProto;
import org.apache.hadoop.ozone.scm.container.placement.metrics.ContainerStat;
import org.apache.hadoop.ozone.scm.container.placement.metrics.SCMMetrics;
import org.junit.Test;

/**
 * This class tests the metrics of Storage Container Manager.
 */
public class TestSCMMetrics {
  private static MiniOzoneCluster cluster = null;

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
      scmManager.sendContainerReport(createContainerReport(numReport, stat));

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
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private ContainerReportsRequestProto createContainerReport(int numReport,
      ContainerStat stat) {
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
    reportsBuilder.setDatanodeID(SCMTestUtils.getDatanodeID()
        .getProtoBufMessage());
    reportsBuilder.setType(StorageContainerDatanodeProtocolProtos
        .ContainerReportsRequestProto.reportType.fullReport);
    return reportsBuilder.build();
  }
}
