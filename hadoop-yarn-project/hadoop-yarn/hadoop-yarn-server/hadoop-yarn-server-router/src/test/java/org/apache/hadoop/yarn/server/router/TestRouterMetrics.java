/*
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
package org.apache.hadoop.yarn.server.router;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class validates the correctness of Router Federation Interceptor
 * Metrics.
 */
public class TestRouterMetrics {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestRouterMetrics.class);

  // All the operations in the bad subcluster failed.
  private MockBadSubCluster badSubCluster = new MockBadSubCluster();
  // All the operations in the bad subcluster succeed.
  private MockGoodSubCluster goodSubCluster = new MockGoodSubCluster();

  private static RouterMetrics metrics = RouterMetrics.getMetrics();

  private static final Double ASSERT_DOUBLE_DELTA = 0.01;

  @BeforeClass
  public static void init() {

    LOG.info("Test: aggregate metrics are initialized correctly");

    Assert.assertEquals(0, metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(0, metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(0, metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(0, metrics.getNumSucceededAppsRetrieved());
    Assert.assertEquals(0,
        metrics.getNumSucceededAppAttemptsRetrieved());

    Assert.assertEquals(0, metrics.getAppsFailedCreated());
    Assert.assertEquals(0, metrics.getAppsFailedSubmitted());
    Assert.assertEquals(0, metrics.getAppsFailedKilled());
    Assert.assertEquals(0, metrics.getAppsFailedRetrieved());
    Assert.assertEquals(0,
        metrics.getAppAttemptsFailedRetrieved());

    LOG.info("Test: aggregate metrics are updated correctly");
  }

  /**
   * This test validates the correctness of the metric: Created Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsCreated() {

    long totalGoodBefore = metrics.getNumSucceededAppsCreated();

    goodSubCluster.getNewApplication(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsCreated(), 0);

    goodSubCluster.getNewApplication(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsCreated());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsCreated(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to create Apps.
   */
  @Test
  public void testAppsFailedCreated() {

    long totalBadbefore = metrics.getAppsFailedCreated();

    badSubCluster.getNewApplication();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedCreated());
  }

  /**
   * This test validates the correctness of the metric: Submitted Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsSubmitted() {

    long totalGoodBefore = metrics.getNumSucceededAppsSubmitted();

    goodSubCluster.submitApplication(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsSubmitted(), 0);

    goodSubCluster.submitApplication(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsSubmitted());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsSubmitted(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to submit Apps.
   */
  @Test
  public void testAppsFailedSubmitted() {

    long totalBadbefore = metrics.getAppsFailedSubmitted();

    badSubCluster.submitApplication();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedSubmitted());
  }

  /**
   * This test validates the correctness of the metric: Killed Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsKilled() {

    long totalGoodBefore = metrics.getNumSucceededAppsKilled();

    goodSubCluster.forceKillApplication(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(100, metrics.getLatencySucceededAppsKilled(), 0);

    goodSubCluster.forceKillApplication(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsKilled());
    Assert.assertEquals(150, metrics.getLatencySucceededAppsKilled(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to kill Apps.
   */
  @Test
  public void testAppsFailedKilled() {

    long totalBadbefore = metrics.getAppsFailedKilled();

    badSubCluster.forceKillApplication();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedKilled());
  }

  /**
   * This test validates the correctness of the metric: Retrieved Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededAppsRetrieved();

    goodSubCluster.getApplicationReport(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsRetrieved());
    Assert.assertEquals(100, metrics.getLatencySucceededGetAppReport(), 0);

    goodSubCluster.getApplicationReport(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetAppReport(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to retrieve Apps.
   */
  @Test
  public void testAppsReportFailed() {

    long totalBadbefore = metrics.getAppsFailedRetrieved();

    badSubCluster.getApplicationReport();

    Assert.assertEquals(totalBadbefore + 1, metrics.getAppsFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric:
   * Retrieved AppAttempt Report
   * successfully.
   */
  @Test
  public void testSucceededAppAttemptReport() {

    long totalGoodBefore = metrics.getNumSucceededAppAttemptsRetrieved();

    goodSubCluster.getApplicationAttemptReport(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppAttemptsRetrieved());
    Assert.assertEquals(100,
        metrics.getLatencySucceededGetAppAttemptReport(), 0);

    goodSubCluster.getApplicationAttemptReport(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppAttemptsRetrieved());
    Assert.assertEquals(150,
        metrics.getLatencySucceededGetAppAttemptReport(), 0);
  }

  /**
   * This test validates the correctness of the metric:
   * Failed to retrieve AppAttempt Report.
   */
  @Test
  public void testAppAttemptReportFailed() {

    long totalBadbefore = metrics.getAppAttemptsFailedRetrieved();

    badSubCluster.getApplicationAttemptReport();

    Assert.assertEquals(totalBadbefore + 1,
        metrics.getAppAttemptsFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric: Retrieved Multiple Apps
   * successfully.
   */
  @Test
  public void testSucceededMultipleAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededMultipleAppsRetrieved();

    goodSubCluster.getApplicationsReport(100);

    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededMultipleAppsRetrieved());
    Assert.assertEquals(100, metrics.getLatencySucceededMultipleGetAppReport(),
        0);

    goodSubCluster.getApplicationsReport(200);

    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededMultipleAppsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededMultipleGetAppReport(),
        0);
  }

  /**
   * This test validates the correctness of the metric: Failed to retrieve
   * Multiple Apps.
   */
  @Test
  public void testMulipleAppsReportFailed() {

    long totalBadbefore = metrics.getMultipleAppsFailedRetrieved();

    badSubCluster.getApplicationsReport();

    Assert.assertEquals(totalBadbefore + 1,
        metrics.getMultipleAppsFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric: Retrieved getClusterMetrics
   * multiple times successfully.
   */
  @Test
  public void testSucceededGetClusterMetrics() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterMetricsRetrieved();
    goodSubCluster.getClusterMetrics(100);
    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterMetricsRetrieved());
    Assert.assertEquals(100, metrics.getLatencySucceededGetClusterMetricsRetrieved(),
        0);
    goodSubCluster.getClusterMetrics(200);
    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterMetricsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetClusterMetricsRetrieved(),
        0);
  }

  /**
   * This test validates the correctness of the metric: Failed to
   * retrieve getClusterMetrics.
   */
  @Test
  public void testGetClusterMetricsFailed() {
    long totalBadbefore = metrics.getClusterMetricsFailedRetrieved();
    badSubCluster.getClusterMetrics();
    Assert.assertEquals(totalBadbefore + 1,
        metrics.getClusterMetricsFailedRetrieved());
  }

  // Records failures for all calls
  private class MockBadSubCluster {
    public void getNewApplication() {
      LOG.info("Mocked: failed getNewApplication call");
      metrics.incrAppsFailedCreated();
    }

    public void submitApplication() {
      LOG.info("Mocked: failed submitApplication call");
      metrics.incrAppsFailedSubmitted();
    }

    public void forceKillApplication() {
      LOG.info("Mocked: failed forceKillApplication call");
      metrics.incrAppsFailedKilled();
    }

    public void getApplicationReport() {
      LOG.info("Mocked: failed getApplicationReport call");
      metrics.incrAppsFailedRetrieved();
    }

    public void getApplicationAttemptReport() {
      LOG.info("Mocked: failed getApplicationAttemptReport call");
      metrics.incrAppsFailedRetrieved();
    }

    public void getApplicationsReport() {
      LOG.info("Mocked: failed getApplicationsReport call");
      metrics.incrMultipleAppsFailedRetrieved();
    }

    public void getClusterMetrics() {
      LOG.info("Mocked: failed getClusterMetrics call");
      metrics.incrGetClusterMetricsFailedRetrieved();
    }

    public void getClusterNodes() {
      LOG.info("Mocked: failed getClusterNodes call");
      metrics.incrClusterNodesFailedRetrieved();
    }

    public void getNodeToLabels() {
      LOG.info("Mocked: failed getNodeToLabels call");
      metrics.incrNodeToLabelsFailedRetrieved();
    }

    public void getLabelToNodes() {
      LOG.info("Mocked: failed getLabelToNodes call");
      metrics.incrLabelsToNodesFailedRetrieved();
    }

    public void getClusterNodeLabels() {
      LOG.info("Mocked: failed getClusterNodeLabels call");
      metrics.incrClusterNodeLabelsFailedRetrieved();
    }
  }

  // Records successes for all calls
  private class MockGoodSubCluster {
    public void getNewApplication(long duration) {
      LOG.info("Mocked: successful getNewApplication call with duration {}",
          duration);
      metrics.succeededAppsCreated(duration);
    }

    public void submitApplication(long duration) {
      LOG.info("Mocked: successful submitApplication call with duration {}",
          duration);
      metrics.succeededAppsSubmitted(duration);
    }

    public void forceKillApplication(long duration) {
      LOG.info("Mocked: successful forceKillApplication call with duration {}",
          duration);
      metrics.succeededAppsKilled(duration);
    }

    public void getApplicationReport(long duration) {
      LOG.info("Mocked: successful getApplicationReport call with duration {}",
          duration);
      metrics.succeededAppsRetrieved(duration);
    }

    public void getApplicationAttemptReport(long duration) {
      LOG.info("Mocked: successful " +
              "getApplicationAttemptReport call with duration {}",
          duration);
      metrics.succeededAppAttemptsRetrieved(duration);
    }

    public void getApplicationsReport(long duration) {
      LOG.info("Mocked: successful getApplicationsReport call with duration {}",
          duration);
      metrics.succeededMultipleAppsRetrieved(duration);
    }

    public void getClusterMetrics(long duration){
      LOG.info("Mocked: successful getClusterMetrics call with duration {}",
              duration);
      metrics.succeededGetClusterMetricsRetrieved(duration);
    }

    public void getClusterNodes(long duration) {
      LOG.info("Mocked: successful getClusterNodes call with duration {}", duration);
      metrics.succeededGetClusterNodesRetrieved(duration);
    }

    public void getNodeToLabels(long duration) {
      LOG.info("Mocked: successful getNodeToLabels call with duration {}", duration);
      metrics.succeededGetNodeToLabelsRetrieved(duration);
    }

    public void getLabelToNodes(long duration) {
      LOG.info("Mocked: successful getLabelToNodes call with duration {}", duration);
      metrics.succeededGetLabelsToNodesRetrieved(duration);
    }

    public void getClusterNodeLabels(long duration) {
      LOG.info("Mocked: successful getClusterNodeLabels call with duration {}", duration);
      metrics.succeededGetClusterNodeLabelsRetrieved(duration);
    }
  }

  @Test
  public void testSucceededGetClusterNodes() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterNodesRetrieved();
    goodSubCluster.getClusterNodes(150);
    Assert.assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetClusterNodesRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetClusterNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterNodes(300);
    Assert.assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetClusterNodesRetrieved());
    Assert.assertEquals(225, metrics.getLatencySucceededGetClusterNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetClusterNodesFailed() {
    long totalBadBefore = metrics.getClusterNodesFailedRetrieved();
    badSubCluster.getClusterNodes();
    Assert.assertEquals(totalBadBefore + 1, metrics.getClusterNodesFailedRetrieved());
  }

  @Test
  public void testSucceededGetNodeToLabels() {
    long totalGoodBefore = metrics.getNumSucceededGetNodeToLabelsRetrieved();
    goodSubCluster.getNodeToLabels(150);
    Assert.assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetNodeToLabelsRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetNodeToLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNodeToLabels(300);
    Assert.assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetNodeToLabelsRetrieved());
    Assert.assertEquals(225, metrics.getLatencySucceededGetNodeToLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetNodeToLabelsFailed() {
    long totalBadBefore = metrics.getNodeToLabelsFailedRetrieved();
    badSubCluster.getNodeToLabels();
    Assert.assertEquals(totalBadBefore + 1, metrics.getNodeToLabelsFailedRetrieved());
  }

  @Test
  public void testSucceededLabelsToNodes() {
    long totalGoodBefore = metrics.getNumSucceededGetLabelsToNodesRetrieved();
    goodSubCluster.getLabelToNodes(150);
    Assert.assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetLabelsToNodesRetrieved());
    Assert.assertEquals(150, metrics.getLatencySucceededGetLabelsToNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getLabelToNodes(300);
    Assert.assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetLabelsToNodesRetrieved());
    Assert.assertEquals(225, metrics.getLatencySucceededGetLabelsToNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetLabelsToNodesFailed() {
    long totalBadBefore = metrics.getLabelsToNodesFailedRetrieved();
    badSubCluster.getLabelToNodes();
    Assert.assertEquals(totalBadBefore + 1, metrics.getLabelsToNodesFailedRetrieved());
  }

  @Test
  public void testSucceededClusterNodeLabels() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterNodeLabelsRetrieved();
    goodSubCluster.getClusterNodeLabels(150);
    Assert.assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterNodeLabelsRetrieved());
    Assert.assertEquals(150,
        metrics.getLatencySucceededGetClusterNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterNodeLabels(300);
    Assert.assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterNodeLabelsRetrieved());
    Assert.assertEquals(225, metrics.getLatencySucceededGetClusterNodeLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testClusterNodeLabelsFailed() {
    long totalBadBefore = metrics.getGetClusterNodeLabelsFailedRetrieved();
    badSubCluster.getClusterNodeLabels();
    Assert.assertEquals(totalBadBefore + 1, metrics.getGetClusterNodeLabelsFailedRetrieved());
  }
}
