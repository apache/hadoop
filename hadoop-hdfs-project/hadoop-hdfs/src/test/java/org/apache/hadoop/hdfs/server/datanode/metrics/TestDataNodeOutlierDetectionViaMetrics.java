/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.metrics;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.metrics2.lib.MetricsTestHelper;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test that the {@link DataNodePeerMetrics} class is able to detect
 * outliers i.e. slow nodes via the metrics it maintains.
 * Set a timeout for every test case.
 */
@Timeout(300)
public class TestDataNodeOutlierDetectionViaMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeOutlierDetectionViaMetrics.class);

  // A few constants to keep the test run time short.
  private static final int WINDOW_INTERVAL_SECONDS = 3;
  private static final int ROLLING_AVERAGE_WINDOWS = 10;
  private static final int SLOW_NODE_LATENCY_MS = 20_000;
  private static final int FAST_NODE_MAX_LATENCY_MS = 5;
  private static final long MIN_OUTLIER_DETECTION_PEERS = 10;

  private Random random = new Random(System.currentTimeMillis());

  private Configuration conf;

  @BeforeEach
  public void setup() {
    GenericTestUtils.setLogLevel(DataNodePeerMetrics.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(OutlierDetector.LOG, Level.TRACE);
    conf = new HdfsConfiguration();
  }

  /**
   * Test that a very slow peer is detected as an outlier.
   */
  @Test
  public void testOutlierIsDetected() throws Exception {
    final String slowNodeName = "SlowNode";

    DataNodePeerMetrics peerMetrics = new DataNodePeerMetrics(
        "PeerMetrics-For-Test", conf);

    MetricsTestHelper.replaceRollingAveragesScheduler(
        peerMetrics.getSendPacketDownstreamRollingAverages(),
        ROLLING_AVERAGE_WINDOWS,
        WINDOW_INTERVAL_SECONDS, TimeUnit.SECONDS);

    injectFastNodesSamples(peerMetrics);
    injectSlowNodeSamples(peerMetrics, slowNodeName);

    // Trigger a snapshot.
    peerMetrics.dumpSendPacketDownstreamAvgInfoAsJson();

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return peerMetrics.getOutliers().size() > 0;
      }
    }, 500, 100_000);

    final Map<String, OutlierMetrics> outliers = peerMetrics.getOutliers();
    LOG.info("Got back outlier nodes: {}", outliers);
    assertThat(outliers.size()).isEqualTo(1);
    assertTrue(outliers.containsKey(slowNodeName));
  }

  /**
   * Test that when there are no outliers, we get back nothing.
   */
  @Test
  public void testWithNoOutliers() throws Exception {
    DataNodePeerMetrics peerMetrics = new DataNodePeerMetrics(
        "PeerMetrics-For-Test", conf);

    MetricsTestHelper.replaceRollingAveragesScheduler(
        peerMetrics.getSendPacketDownstreamRollingAverages(),
        ROLLING_AVERAGE_WINDOWS,
        WINDOW_INTERVAL_SECONDS, TimeUnit.SECONDS);

    injectFastNodesSamples(peerMetrics);

    // Trigger a snapshot.
    peerMetrics.dumpSendPacketDownstreamAvgInfoAsJson();

    // Ensure that we get back the outlier.
    assertTrue(peerMetrics.getOutliers().isEmpty());
  }

  /**
   * Inject fake stats for MIN_OUTLIER_DETECTION_PEERS fast nodes.
   *
   * @param peerMetrics
   */
  public void injectFastNodesSamples(DataNodePeerMetrics peerMetrics) {
    for (int nodeIndex = 0;
         nodeIndex < MIN_OUTLIER_DETECTION_PEERS; ++nodeIndex) {
      final String nodeName = "FastNode-" + nodeIndex;
      LOG.info("Generating stats for node {}", nodeName);
      for (int i = 0;
           i < 2 * peerMetrics.getMinOutlierDetectionSamples();
           ++i) {
        peerMetrics.addSendPacketDownstream(
            nodeName, random.nextInt(FAST_NODE_MAX_LATENCY_MS));
      }
    }
  }

  /**
   * Inject fake stats for one extremely slow node.
   */
  public void injectSlowNodeSamples(
      DataNodePeerMetrics peerMetrics, String slowNodeName)
      throws InterruptedException {

    // And the one slow node.
    for (int i = 0;
         i < 2 * peerMetrics.getMinOutlierDetectionSamples();
         ++i) {
      peerMetrics.addSendPacketDownstream(
          slowNodeName, SLOW_NODE_LATENCY_MS);
    }
  }

  /**
   * Verifies that slow disk detection is performed per StorageType group.
   * Scenario 1: 1 SSD + 10 HDDs — SSD group skipped (size below minimum),
   * 2 HDDs detected.
   * Scenario 2: 6 SSDs + 10 HDDs — 2 SSDs and 2 HDDs
   * each detected independently within their own group.
   */
  @Test
  public void testStorageTypeAwareSlowDiskDetection() throws Exception {
    Configuration testConf = new HdfsConfiguration();
    testConf.setLong(DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY, 5);
    testConf.setInt(DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY, 1);

    // Scenario 1: 1 SSD + 10 HDDs.
    {
      List<FsVolumeSpi> volumes = new ArrayList<>();
      volumes.add(createMockDiskVolume("/ssd0/", StorageType.SSD, 5000.0));
      for (int i = 0; i < 8; i++) {
        volumes.add(createMockDiskVolume("/hdd" + i + "/", StorageType.DISK, 0.5));
      }
      volumes.add(createMockDiskVolume("/hdd8/", StorageType.DISK, 5000.0));
      volumes.add(createMockDiskVolume("/hdd9/", StorageType.DISK, 6000.0));

      DataNodeDiskMetrics diskMetrics = buildMetrics(testConf, volumes);
      try {
        GenericTestUtils.waitFor(() -> diskMetrics.getDiskOutliersStats().size() >= 2, 100, 10_000);
        Map<String, ?> outliers = diskMetrics.getDiskOutliersStats();
        assertFalse(outliers.containsKey("/ssd0/"), "SSD group too small, must not be flagged");
        assertTrue(outliers.containsKey("/hdd8/"), "Slow HDD must be detected");
        assertTrue(outliers.containsKey("/hdd9/"), "Slow HDD must be detected");
        assertThat(diskMetrics.getSlowDisksToExclude()).hasSize(1);
      } finally {
        diskMetrics.shutdownAndWait();
      }
    }

    // Scenario 2: 6 SSDs + 10 HDDs.
    {
      List<FsVolumeSpi> volumes = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        volumes.add(createMockDiskVolume("/ssd" + i + "/", StorageType.SSD, i + 1.0));
      }
      volumes.add(createMockDiskVolume("/ssd4/", StorageType.SSD, 5000.0));
      volumes.add(createMockDiskVolume("/ssd5/", StorageType.SSD, 6000.0));
      for (int i = 0; i < 8; i++) {
        volumes.add(createMockDiskVolume("/hdd" + i + "/", StorageType.DISK, 0.5));
      }
      volumes.add(createMockDiskVolume("/hdd8/", StorageType.DISK, 5000.0));
      volumes.add(createMockDiskVolume("/hdd9/", StorageType.DISK, 6000.0));

      DataNodeDiskMetrics diskMetrics = buildMetrics(testConf, volumes);
      try {
        GenericTestUtils.waitFor(() -> diskMetrics.getDiskOutliersStats().size() >= 4, 100, 10_000);
        Map<String, ?> outliers = diskMetrics.getDiskOutliersStats();
        assertTrue(outliers.containsKey("/ssd4/"), "Slow SSD must be detected");
        assertTrue(outliers.containsKey("/ssd5/"), "Slow SSD must be detected");
        assertTrue(outliers.containsKey("/hdd8/"), "Slow HDD must be detected");
        assertTrue(outliers.containsKey("/hdd9/"), "Slow HDD must be detected");
        assertThat(outliers.keySet().stream().filter(k -> k.startsWith("/ssd")).count()).isEqualTo(
            2);
        assertThat(outliers.keySet().stream().filter(k -> k.startsWith("/hdd")).count()).isEqualTo(
            2);
        assertThat(diskMetrics.getSlowDisksToExclude()).hasSize(2);
      } finally {
        diskMetrics.shutdownAndWait();
      }
    }
  }

  private DataNodeDiskMetrics buildMetrics(Configuration conf, List<FsVolumeSpi> volumes) {
    DataNode mockDn = mock(DataNode.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    FsDatasetSpi mockDataset = mock(FsDatasetSpi.class);
    FsDatasetSpi.FsVolumeReferences mockRefs = mock(FsDatasetSpi.FsVolumeReferences.class);
    when(mockDn.getFSDataset()).thenReturn(mockDataset);
    when(mockDataset.getFsVolumeReferences()).thenReturn(mockRefs);
    doAnswer(inv -> volumes.iterator()).when(mockRefs).iterator();
    return new DataNodeDiskMetrics(mockDn, 100, conf);
  }

  /**
   * Creates a mock volume; only metadata latency drives outlier detection.
   */
  @SuppressWarnings("unchecked")
  private FsVolumeSpi createMockDiskVolume(String path, StorageType storageType,
      double metadataMs) {
    FsVolumeSpi mockVolume = mock(FsVolumeSpi.class);
    DataNodeVolumeMetrics mockMetrics = mock(DataNodeVolumeMetrics.class);
    try {
      when(mockVolume.getBaseURI()).thenReturn(new URI("file://" + path));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    when(mockVolume.getStorageType()).thenReturn(storageType);
    when(mockVolume.getMetrics()).thenReturn(mockMetrics);
    when(mockMetrics.getMetadataOperationMean()).thenReturn(metadataMs);
    when(mockMetrics.getReadIoMean()).thenReturn(0.0);
    when(mockMetrics.getWriteIoMean()).thenReturn(0.0);
    return mockVolume;
  }
}
