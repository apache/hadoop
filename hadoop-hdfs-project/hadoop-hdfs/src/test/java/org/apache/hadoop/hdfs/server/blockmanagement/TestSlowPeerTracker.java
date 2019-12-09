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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.SlowPeerTracker
    .ReportForJson;
import org.apache.hadoop.util.FakeTimer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SlowPeerTracker}.
 */
public class TestSlowPeerTracker {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestSlowPeerTracker.class);

  /**
   * Set a timeout for every test case.
   */
  @Rule
  public Timeout testTimeout = new Timeout(300_000);

  private Configuration conf;
  private SlowPeerTracker tracker;
  private FakeTimer timer;
  private long reportValidityMs;
  private static final ObjectReader READER =
      new ObjectMapper().readerFor(new TypeReference<Set<ReportForJson>>() {});

  @Before
  public void setup() {
    conf = new HdfsConfiguration();
    timer = new FakeTimer();
    tracker = new SlowPeerTracker(conf, timer);
    reportValidityMs = tracker.getReportValidityMs();
  }

  /**
   * Edge case, there are no reports to retrieve.
   */
  @Test
  public void testEmptyReports() {
    assertTrue(tracker.getReportsForAllDataNodes().isEmpty());
    assertTrue(tracker.getReportsForNode("noSuchNode").isEmpty());
  }

  @Test
  public void testReportsAreRetrieved() {
    tracker.addReport("node2", "node1");
    tracker.addReport("node3", "node1");
    tracker.addReport("node3", "node2");

    assertThat(tracker.getReportsForAllDataNodes().size(), is(2));
    assertThat(tracker.getReportsForNode("node2").size(), is(1));
    assertThat(tracker.getReportsForNode("node3").size(), is(2));
    assertThat(tracker.getReportsForNode("node1").size(), is(0));
  }

  /**
   * Test that when all reports are expired, we get back nothing.
   */
  @Test
  public void testAllReportsAreExpired() {
    tracker.addReport("node2", "node1");
    tracker.addReport("node3", "node2");
    tracker.addReport("node1", "node3");

    // No reports should expire after 1ms.
    timer.advance(1);
    assertThat(tracker.getReportsForAllDataNodes().size(), is(3));

    // All reports should expire after REPORT_VALIDITY_MS.
    timer.advance(reportValidityMs);
    assertTrue(tracker.getReportsForAllDataNodes().isEmpty());
    assertTrue(tracker.getReportsForNode("node1").isEmpty());
    assertTrue(tracker.getReportsForNode("node2").isEmpty());
    assertTrue(tracker.getReportsForNode("node3").isEmpty());
  }

  /**
   * Test the case when a subset of reports has expired.
   * Ensure that we only get back non-expired reports.
   */
  @Test
  public void testSomeReportsAreExpired() {
    tracker.addReport("node3", "node1");
    tracker.addReport("node3", "node2");
    timer.advance(reportValidityMs);
    tracker.addReport("node3", "node4");
    assertThat(tracker.getReportsForAllDataNodes().size(), is(1));
    assertThat(tracker.getReportsForNode("node3").size(), is(1));
    assertTrue(tracker.getReportsForNode("node3").contains("node4"));
  }

  /**
   * Test the case when an expired report is replaced by a valid one.
   */
  @Test
  public void testReplacement() {
    tracker.addReport("node2", "node1");
    timer.advance(reportValidityMs); // Expire the report.
    assertThat(tracker.getReportsForAllDataNodes().size(), is(0));

    // This should replace the expired report with a newer valid one.
    tracker.addReport("node2", "node1");
    assertThat(tracker.getReportsForAllDataNodes().size(), is(1));
    assertThat(tracker.getReportsForNode("node2").size(), is(1));
  }

  @Test
  public void testGetJson() throws IOException {
    tracker.addReport("node1", "node2");
    tracker.addReport("node2", "node3");
    tracker.addReport("node2", "node1");
    tracker.addReport("node4", "node1");

    final Set<ReportForJson> reports = getAndDeserializeJson();

    // And ensure its contents are what we expect.
    assertThat(reports.size(), is(3));
    assertTrue(isNodeInReports(reports, "node1"));
    assertTrue(isNodeInReports(reports, "node2"));
    assertTrue(isNodeInReports(reports, "node4"));

    assertFalse(isNodeInReports(reports, "node3"));
  }

  @Test
  public void testGetJsonSizeIsLimited() throws IOException {
    tracker.addReport("node1", "node2");
    tracker.addReport("node1", "node3");
    tracker.addReport("node2", "node3");
    tracker.addReport("node2", "node4");
    tracker.addReport("node3", "node4");
    tracker.addReport("node3", "node5");
    tracker.addReport("node4", "node6");
    tracker.addReport("node5", "node6");
    tracker.addReport("node5", "node7");
    tracker.addReport("node6", "node7");
    tracker.addReport("node6", "node8");

    final Set<ReportForJson> reports = getAndDeserializeJson();

    // Ensure that node4 is not in the list since it was
    // tagged by just one peer and we already have 5 other nodes.
    assertFalse(isNodeInReports(reports, "node4"));

    // Remaining nodes should be in the list.
    assertTrue(isNodeInReports(reports, "node1"));
    assertTrue(isNodeInReports(reports, "node2"));
    assertTrue(isNodeInReports(reports, "node3"));
    assertTrue(isNodeInReports(reports, "node5"));
    assertTrue(isNodeInReports(reports, "node6"));
  }

  @Test
  public void testLowRankedElementsIgnored() throws IOException {
    // Insert 5 nodes with 2 peer reports each.
    for (int i = 0; i < 5; ++i) {
      tracker.addReport("node" + i, "reporter1");
      tracker.addReport("node" + i, "reporter2");
    }

    // Insert 10 nodes with 1 peer report each.
    for (int i = 10; i < 20; ++i) {
      tracker.addReport("node" + i, "reporter1");
    }

    final Set<ReportForJson> reports = getAndDeserializeJson();

    // Ensure that only the first 5 nodes with two reports each were
    // included in the JSON.
    for (int i = 0; i < 5; ++i) {
      assertTrue(isNodeInReports(reports, "node" + i));
    }
  }

  private boolean isNodeInReports(
      Set<ReportForJson> reports, String node) {
    for (ReportForJson report : reports) {
      if (report.getSlowNode().equalsIgnoreCase(node)) {
        return true;
      }
    }
    return false;
  }

  private Set<ReportForJson> getAndDeserializeJson()
      throws IOException {
    final String json = tracker.getJson();
    LOG.info("Got JSON: {}", json);
    return READER.readValue(json);
  }
}
