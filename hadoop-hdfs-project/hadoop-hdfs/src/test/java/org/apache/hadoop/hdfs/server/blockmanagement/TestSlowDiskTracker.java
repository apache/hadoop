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

package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hdfs.DFSConfigKeys
    .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys
    .DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowDiskTracker
    .DiskLatency;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.FakeTimer;

import java.util.function.Supplier;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link SlowDiskTracker}.
 */
public class TestSlowDiskTracker {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestSlowDiskTracker.class);

  /**
   * Set a timeout for every test case.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  private static Configuration conf;
  private SlowDiskTracker tracker;
  private FakeTimer timer;
  private long reportValidityMs;
  private static final long OUTLIERS_REPORT_INTERVAL = 1000;
  private static final ObjectReader READER = new ObjectMapper().readerFor(
          new TypeReference<ArrayList<DiskLatency>>() {});

  static {
    conf = new HdfsConfiguration();
    conf.setLong(DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setInt(DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, 100);
    conf.setTimeDuration(DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        OUTLIERS_REPORT_INTERVAL, TimeUnit.MILLISECONDS);
  }
  @Before
  public void setup() {
    timer = new FakeTimer();
    tracker = new SlowDiskTracker(conf, timer);
    reportValidityMs = tracker.getReportValidityMs();
  }

  @Test
  public void testDataNodeHeartbeatSlowDiskReport() throws Exception {
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    try {
      DataNode dn1 = cluster.getDataNodes().get(0);
      DataNode dn2 = cluster.getDataNodes().get(1);
      NameNode nn = cluster.getNameNode(0);

      DatanodeManager datanodeManager = nn.getNamesystem().getBlockManager()
          .getDatanodeManager();
      SlowDiskTracker slowDiskTracker = datanodeManager.getSlowDiskTracker();
      slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 100);

      dn1.getDiskMetrics().addSlowDiskForTesting("disk1", ImmutableMap.of(
          DiskOp.WRITE, 1.3));
      dn1.getDiskMetrics().addSlowDiskForTesting("disk2", ImmutableMap.of(
          DiskOp.READ, 1.6, DiskOp.WRITE, 1.1));
      dn2.getDiskMetrics().addSlowDiskForTesting("disk1", ImmutableMap.of(
          DiskOp.METADATA, 0.8));
      dn2.getDiskMetrics().addSlowDiskForTesting("disk2", ImmutableMap.of(
          DiskOp.WRITE, 1.3));

      String dn1ID = dn1.getDatanodeId().getIpcAddr(false);
      String dn2ID = dn2.getDatanodeId().getIpcAddr(false);

      // Advance the timer and wait for NN to receive reports from DataNodes.
      Thread.sleep(OUTLIERS_REPORT_INTERVAL);

      // Wait for NN to receive reports from all DNs
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return (slowDiskTracker.getSlowDisksReport().size() == 4);
        }
      }, 1000, 100000);

      Map<String, DiskLatency> slowDisksReport = getSlowDisksReportForTesting(
          slowDiskTracker);

      assertThat(slowDisksReport.size(), is(4));
      assertTrue(Math.abs(slowDisksReport.get(dn1ID + ":disk1")
          .getLatency(DiskOp.WRITE) - 1.3) < 0.0000001);
      assertTrue(Math.abs(slowDisksReport.get(dn1ID + ":disk2")
          .getLatency(DiskOp.READ) - 1.6) < 0.0000001);
      assertTrue(Math.abs(slowDisksReport.get(dn1ID + ":disk2")
          .getLatency(DiskOp.WRITE) - 1.1) < 0.0000001);
      assertTrue(Math.abs(slowDisksReport.get(dn2ID + ":disk1")
          .getLatency(DiskOp.METADATA) - 0.8) < 0.0000001);
      assertTrue(Math.abs(slowDisksReport.get(dn2ID + ":disk2")
          .getLatency(DiskOp.WRITE) - 1.3) < 0.0000001);

      // Test the slow disk report JSON string
      ArrayList<DiskLatency> jsonReport = getAndDeserializeJson(
          slowDiskTracker.getSlowDiskReportAsJsonString());

      assertThat(jsonReport.size(), is(4));
      assertTrue(isDiskInReports(jsonReport, dn1ID, "disk1", DiskOp.WRITE, 1.3));
      assertTrue(isDiskInReports(jsonReport, dn1ID, "disk2", DiskOp.READ, 1.6));
      assertTrue(isDiskInReports(jsonReport, dn1ID, "disk2", DiskOp.WRITE, 1.1));
      assertTrue(isDiskInReports(jsonReport, dn2ID, "disk1", DiskOp.METADATA,
          0.8));
      assertTrue(isDiskInReports(jsonReport, dn2ID, "disk2", DiskOp.WRITE, 1.3));
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Edge case, there are no reports to retrieve.
   */
  @Test
  public void testEmptyReports() {
    tracker.updateSlowDiskReportAsync(timer.monotonicNow());
    assertTrue(getSlowDisksReportForTesting(tracker).isEmpty());
  }

  @Test
  public void testReportsAreRetrieved() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.METADATA, 1.1, DiskOp.READ, 1.8));
    addSlowDiskForTesting("dn1", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.3));
    addSlowDiskForTesting("dn2", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.1));

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !tracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 5000);

    Map<String, DiskLatency> reports = getSlowDisksReportForTesting(tracker);

    assertThat(reports.size(), is(3));
    assertTrue(Math.abs(reports.get("dn1:disk1")
        .getLatency(DiskOp.METADATA) - 1.1) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn1:disk1")
        .getLatency(DiskOp.READ) - 1.8) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn1:disk2")
        .getLatency(DiskOp.READ) - 1.3) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn2:disk2")
        .getLatency(DiskOp.READ) - 1.1) < 0.0000001);
  }

  /**
   * Test that when all reports are expired, we get back nothing.
   */
  @Test
  public void testAllReportsAreExpired() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.METADATA, 1.1, DiskOp.READ, 1.8));
    addSlowDiskForTesting("dn1", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.3));
    addSlowDiskForTesting("dn2", "disk2",
        ImmutableMap.of(DiskOp.WRITE, 1.1));

    // No reports should expire after 1ms.
    timer.advance(1);
    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !tracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 5000);

    Map<String, DiskLatency> reports = getSlowDisksReportForTesting(tracker);

    assertThat(reports.size(), is(3));
    assertTrue(Math.abs(reports.get("dn1:disk1")
        .getLatency(DiskOp.METADATA) - 1.1) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn1:disk1")
        .getLatency(DiskOp.READ) - 1.8) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn1:disk2")
        .getLatency(DiskOp.READ) - 1.3) < 0.0000001);
    assertTrue(Math.abs(reports.get("dn2:disk2")
        .getLatency(DiskOp.WRITE) - 1.1) < 0.0000001);

    // All reports should expire after REPORT_VALIDITY_MS.
    timer.advance(reportValidityMs);
    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return tracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 3000);

    reports = getSlowDisksReportForTesting(tracker);

    assertThat(reports.size(), is(0));
  }

  /**
   * Test the case when a subset of reports has expired.
   * Ensure that we only get back non-expired reports.
   */
  @Test
  public void testSomeReportsAreExpired() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.METADATA, 1.1, DiskOp.READ, 1.8));
    addSlowDiskForTesting("dn1", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.3));
    timer.advance(reportValidityMs);
    addSlowDiskForTesting("dn2", "disk2",
        ImmutableMap.of(DiskOp.WRITE, 1.1));

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !tracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 5000);

    Map<String, DiskLatency> reports = getSlowDisksReportForTesting(tracker);

    assertThat(reports.size(), is(1));
    assertTrue(Math.abs(reports.get("dn2:disk2")
        .getLatency(DiskOp.WRITE) - 1.1) < 0.0000001);
  }

  /**
   * Test the case when an expired report is replaced by a valid one.
   */
  @Test
  public void testReplacement() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.METADATA, 1.1, DiskOp.READ, 1.8));
    timer.advance(reportValidityMs);
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.READ, 1.4));

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return !tracker.getSlowDisksReport().isEmpty();
      }
    }, 500, 5000);

    Map<String, DiskLatency> reports = getSlowDisksReportForTesting(tracker);

    assertThat(reports.size(), is(1));
    assertTrue(reports.get("dn1:disk1").getLatency(DiskOp.METADATA) == null);
    assertTrue(Math.abs(reports.get("dn1:disk1")
        .getLatency(DiskOp.READ) - 1.4) < 0.0000001);
  }

  @Test
  public void testGetJson() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.METADATA, 1.1, DiskOp.READ, 1.8));
    addSlowDiskForTesting("dn1", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.3));
    addSlowDiskForTesting("dn2", "disk2",
        ImmutableMap.of(DiskOp.WRITE, 1.1));
    addSlowDiskForTesting("dn3", "disk1",
        ImmutableMap.of(DiskOp.WRITE, 1.1));

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return tracker.getSlowDiskReportAsJsonString() != null;
      }
    }, 500, 5000);

    ArrayList<DiskLatency> jsonReport = getAndDeserializeJson(
        tracker.getSlowDiskReportAsJsonString());

    // And ensure its contents are what we expect.
    assertThat(jsonReport.size(), is(4));
    assertTrue(isDiskInReports(jsonReport, "dn1", "disk1", DiskOp.METADATA,
        1.1));
    assertTrue(isDiskInReports(jsonReport, "dn1", "disk1", DiskOp.READ, 1.8));
    assertTrue(isDiskInReports(jsonReport, "dn1", "disk2", DiskOp.READ, 1.3));
    assertTrue(isDiskInReports(jsonReport, "dn2", "disk2", DiskOp.WRITE, 1.1));
    assertTrue(isDiskInReports(jsonReport, "dn3", "disk1", DiskOp.WRITE, 1.1));
  }

  @Test
  public void testGetJsonSizeIsLimited() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.READ, 1.1));
    addSlowDiskForTesting("dn1", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.2));
    addSlowDiskForTesting("dn1", "disk3",
        ImmutableMap.of(DiskOp.READ, 1.3));
    addSlowDiskForTesting("dn2", "disk1",
        ImmutableMap.of(DiskOp.READ, 1.4));
    addSlowDiskForTesting("dn2", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.5));
    addSlowDiskForTesting("dn3", "disk1",
        ImmutableMap.of(DiskOp.WRITE, 1.6));
    addSlowDiskForTesting("dn3", "disk2",
        ImmutableMap.of(DiskOp.READ, 1.7));
    addSlowDiskForTesting("dn3", "disk3",
        ImmutableMap.of(DiskOp.READ, 1.2));

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());

    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return tracker.getSlowDiskReportAsJsonString() != null;
      }
    }, 500, 5000);

    ArrayList<DiskLatency> jsonReport = getAndDeserializeJson(
        tracker.getSlowDiskReportAsJsonString());

    // Ensure that only the top 5 highest latencies are in the report.
    assertThat(jsonReport.size(), is(5));
    assertTrue(isDiskInReports(jsonReport, "dn3", "disk2", DiskOp.READ, 1.7));
    assertTrue(isDiskInReports(jsonReport, "dn3", "disk1", DiskOp.WRITE, 1.6));
    assertTrue(isDiskInReports(jsonReport, "dn2", "disk2", DiskOp.READ, 1.5));
    assertTrue(isDiskInReports(jsonReport, "dn2", "disk1", DiskOp.READ, 1.4));
    assertTrue(isDiskInReports(jsonReport, "dn1", "disk3", DiskOp.READ, 1.3));

    // Remaining nodes should be in the list.
    assertFalse(isDiskInReports(jsonReport, "dn1", "disk1", DiskOp.READ, 1.1));
    assertFalse(isDiskInReports(jsonReport, "dn1", "disk2", DiskOp.READ, 1.2));
    assertFalse(isDiskInReports(jsonReport, "dn3", "disk3", DiskOp.READ, 1.2));
  }

  @Test
  public void testEmptyReport() throws Exception {
    addSlowDiskForTesting("dn1", "disk1",
        ImmutableMap.of(DiskOp.READ, 1.1));
    timer.advance(reportValidityMs);

    tracker.updateSlowDiskReportAsync(timer.monotonicNow());
    Thread.sleep(OUTLIERS_REPORT_INTERVAL*2);

    assertTrue(tracker.getSlowDiskReportAsJsonString() == null);
  }

  @Test
  public void testRemoveInvalidReport() throws Exception {
    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    try {
      NameNode nn = cluster.getNameNode(0);

      DatanodeManager datanodeManager =
          nn.getNamesystem().getBlockManager().getDatanodeManager();
      SlowDiskTracker slowDiskTracker = datanodeManager.getSlowDiskTracker();
      slowDiskTracker.setReportValidityMs(OUTLIERS_REPORT_INTERVAL * 3);
      assertTrue(slowDiskTracker.getSlowDisksReport().isEmpty());
      slowDiskTracker.addSlowDiskReport(
          "dn1",
          generateSlowDiskReport("disk1",
              Collections.singletonMap(DiskOp.WRITE, 1.3)));
      slowDiskTracker.addSlowDiskReport(
          "dn2",
          generateSlowDiskReport("disk2",
              Collections.singletonMap(DiskOp.WRITE, 1.1)));

      // wait for slow disk report
      GenericTestUtils.waitFor(() -> !slowDiskTracker.getSlowDisksReport()
          .isEmpty(), 500, 5000);
      Map<String, DiskLatency> slowDisksReport =
          getSlowDisksReportForTesting(slowDiskTracker);
      assertEquals(2, slowDisksReport.size());

      // wait for invalid report to be removed
      Thread.sleep(OUTLIERS_REPORT_INTERVAL * 3);
      GenericTestUtils.waitFor(() -> slowDiskTracker.getSlowDisksReport()
          .isEmpty(), 500, 5000);
      slowDisksReport = getSlowDisksReportForTesting(slowDiskTracker);
      assertEquals(0, slowDisksReport.size());

    } finally {
      cluster.shutdown();
    }
  }

  private boolean isDiskInReports(ArrayList<DiskLatency> reports,
      String dataNodeID, String disk, DiskOp diskOp, double latency) {
    String diskID = SlowDiskTracker.getSlowDiskIDForReport(dataNodeID, disk);
    for (DiskLatency diskLatency : reports) {
      if (diskLatency.getSlowDiskID().equals(diskID)) {
        if (diskLatency.getLatency(diskOp) == null) {
          return false;
        }
        if (Math.abs(diskLatency.getLatency(diskOp) - latency) < 0.0000001) {
          return true;
        }
      }
    }
    return false;
  }

  private ArrayList<DiskLatency> getAndDeserializeJson(
      final String json) throws IOException {
    return READER.readValue(json);
  }

  private void addSlowDiskForTesting(String dnID, String disk,
      Map<DiskOp, Double> latencies) {
    Map<String, Map<DiskOp, Double>> slowDisk = Maps.newHashMap();
    slowDisk.put(disk, latencies);
    SlowDiskReports slowDiskReport = SlowDiskReports.create(slowDisk);
    tracker.addSlowDiskReport(dnID, slowDiskReport);
  }

  private SlowDiskReports generateSlowDiskReport(String disk,
      Map<DiskOp, Double> latencies) {
    Map<String, Map<DiskOp, Double>> slowDisk = Maps.newHashMap();
    slowDisk.put(disk, latencies);
    SlowDiskReports slowDiskReport = SlowDiskReports.create(slowDisk);
    return slowDiskReport;
  }

  Map<String, DiskLatency> getSlowDisksReportForTesting(
      SlowDiskTracker slowDiskTracker) {
    Map<String, DiskLatency> slowDisksMap = Maps.newHashMap();
    for (DiskLatency diskLatency : slowDiskTracker.getSlowDisksReport()) {
      slowDisksMap.put(diskLatency.getSlowDiskID(), diskLatency);
    }
    return slowDisksMap;
  }
}
