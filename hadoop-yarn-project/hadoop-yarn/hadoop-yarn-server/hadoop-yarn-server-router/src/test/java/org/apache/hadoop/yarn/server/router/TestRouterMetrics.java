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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

  @BeforeAll
  public static void init() {

    LOG.info("Test: aggregate metrics are initialized correctly");

    assertEquals(0, metrics.getNumSucceededAppsCreated());
    assertEquals(0, metrics.getNumSucceededAppsSubmitted());
    assertEquals(0, metrics.getNumSucceededAppsKilled());
    assertEquals(0, metrics.getNumSucceededAppsRetrieved());
    assertEquals(0, metrics.getNumSucceededAppAttemptsRetrieved());

    assertEquals(0, metrics.getAppsFailedCreated());
    assertEquals(0, metrics.getAppsFailedSubmitted());
    assertEquals(0, metrics.getAppsFailedKilled());
    assertEquals(0, metrics.getAppsFailedRetrieved());
    assertEquals(0,
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

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsCreated());
    assertEquals(100, metrics.getLatencySucceededAppsCreated(), 0);

    goodSubCluster.getNewApplication(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsCreated());
    assertEquals(150, metrics.getLatencySucceededAppsCreated(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to create Apps.
   */
  @Test
  public void testAppsFailedCreated() {

    long totalBadbefore = metrics.getAppsFailedCreated();

    badSubCluster.getNewApplication();

    assertEquals(totalBadbefore + 1, metrics.getAppsFailedCreated());
  }

  /**
   * This test validates the correctness of the metric: Submitted Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsSubmitted() {

    long totalGoodBefore = metrics.getNumSucceededAppsSubmitted();

    goodSubCluster.submitApplication(100);

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsSubmitted());
    assertEquals(100, metrics.getLatencySucceededAppsSubmitted(), 0);

    goodSubCluster.submitApplication(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsSubmitted());
    assertEquals(150, metrics.getLatencySucceededAppsSubmitted(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to submit Apps.
   */
  @Test
  public void testAppsFailedSubmitted() {

    long totalBadbefore = metrics.getAppsFailedSubmitted();

    badSubCluster.submitApplication();

    assertEquals(totalBadbefore + 1, metrics.getAppsFailedSubmitted());
  }

  /**
   * This test validates the correctness of the metric: Killed Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsKilled() {

    long totalGoodBefore = metrics.getNumSucceededAppsKilled();

    goodSubCluster.forceKillApplication(100);

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsKilled());
    assertEquals(100, metrics.getLatencySucceededAppsKilled(), 0);

    goodSubCluster.forceKillApplication(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsKilled());
    assertEquals(150, metrics.getLatencySucceededAppsKilled(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to kill Apps.
   */
  @Test
  public void testAppsFailedKilled() {

    long totalBadbefore = metrics.getAppsFailedKilled();

    badSubCluster.forceKillApplication();

    assertEquals(totalBadbefore + 1, metrics.getAppsFailedKilled());
  }

  /**
   * This test validates the correctness of the metric: Retrieved Apps
   * successfully.
   */
  @Test
  public void testSucceededAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededAppsRetrieved();

    goodSubCluster.getApplicationReport(100);

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppsRetrieved());
    assertEquals(100, metrics.getLatencySucceededGetAppReport(), 0);

    goodSubCluster.getApplicationReport(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppsRetrieved());
    assertEquals(150, metrics.getLatencySucceededGetAppReport(), 0);
  }

  /**
   * This test validates the correctness of the metric: Failed to retrieve Apps.
   */
  @Test
  public void testAppsReportFailed() {

    long totalBadbefore = metrics.getAppsFailedRetrieved();

    badSubCluster.getApplicationReport();

    assertEquals(totalBadbefore + 1, metrics.getAppsFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric:
   * Retrieved AppAttempt Report
   * successfully.
   */
  @Test
  public void testSucceededAppAttemptReport() {

    long totalGoodBefore = metrics.getNumSucceededAppAttemptReportRetrieved();

    goodSubCluster.getApplicationAttemptReport(100);

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppAttemptReportRetrieved());
    assertEquals(100,
        metrics.getLatencySucceededGetAppAttemptReport(), ASSERT_DOUBLE_DELTA);

    goodSubCluster.getApplicationAttemptReport(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppAttemptReportRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppAttemptReport(), ASSERT_DOUBLE_DELTA);
  }

  /**
   * This test validates the correctness of the metric:
   * Failed to retrieve AppAttempt Report.
   */
  @Test
  public void testAppAttemptReportFailed() {

    long totalBadBefore = metrics.getAppAttemptReportFailedRetrieved();

    badSubCluster.getApplicationAttemptReport();

    assertEquals(totalBadBefore + 1,
        metrics.getAppAttemptReportFailedRetrieved());
  }

  /**
   * This test validates the correctness of the metric: Retrieved Multiple Apps
   * successfully.
   */
  @Test
  public void testSucceededMultipleAppsReport() {

    long totalGoodBefore = metrics.getNumSucceededMultipleAppsRetrieved();

    goodSubCluster.getApplicationsReport(100);

    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededMultipleAppsRetrieved());
    assertEquals(100, metrics.getLatencySucceededMultipleGetAppReport(),
        0);

    goodSubCluster.getApplicationsReport(200);

    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededMultipleAppsRetrieved());
    assertEquals(150, metrics.getLatencySucceededMultipleGetAppReport(),
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

    assertEquals(totalBadbefore + 1,
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
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterMetricsRetrieved());
    assertEquals(100, metrics.getLatencySucceededGetClusterMetricsRetrieved(),
        0);
    goodSubCluster.getClusterMetrics(200);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterMetricsRetrieved());
    assertEquals(150, metrics.getLatencySucceededGetClusterMetricsRetrieved(),
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
    assertEquals(totalBadbefore + 1,
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
      metrics.incrAppAttemptReportFailedRetrieved();
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

    public void getQueueUserAcls() {
      LOG.info("Mocked: failed getQueueUserAcls call");
      metrics.incrQueueUserAclsFailedRetrieved();
    }

    public void getListReservations() {
      LOG.info("Mocked: failed listReservations call");
      metrics.incrListReservationsFailedRetrieved();
    }

    public void getApplicationAttempts() {
      LOG.info("Mocked: failed getApplicationAttempts call");
      metrics.incrAppAttemptsFailedRetrieved();
    }

    public void getContainerReport() {
      LOG.info("Mocked: failed getContainerReport call");
      metrics.incrGetContainerReportFailedRetrieved();
    }

    public void getContainers() {
      LOG.info("Mocked: failed getContainer call");
      metrics.incrGetContainersFailedRetrieved();
    }

    public void getResourceTypeInfo() {
      LOG.info("Mocked: failed getResourceTypeInfo call");
      metrics.incrResourceTypeInfoFailedRetrieved();
    }

    public void getFailApplicationAttempt() {
      LOG.info("Mocked: failed failApplicationAttempt call");
      metrics.incrFailAppAttemptFailedRetrieved();
    }

    public void getUpdateApplicationPriority() {
      LOG.info("Mocked: failed updateApplicationPriority call");
      metrics.incrUpdateAppPriorityFailedRetrieved();
    }

    public void getUpdateApplicationTimeouts() {
      LOG.info("Mocked: failed updateApplicationTimeouts call");
      metrics.incrUpdateApplicationTimeoutsRetrieved();
    }

    public void getSignalContainer() {
      LOG.info("Mocked: failed signalContainer call");
      metrics.incrSignalToContainerFailedRetrieved();
    }

    public void getQueueInfo() {
      LOG.info("Mocked: failed getQueueInfo call");
      metrics.incrGetQueueInfoFailedRetrieved();
    }

    public void moveApplicationAcrossQueuesFailed() {
      LOG.info("Mocked: failed moveApplicationAcrossQueuesFailed call");
      metrics.incrMoveApplicationAcrossQueuesFailedRetrieved();
    }

    public void getResourceProfilesFailed() {
      LOG.info("Mocked: failed getResourceProfilesFailed call");
      metrics.incrGetResourceProfilesFailedRetrieved();
    }

    public void getResourceProfileFailed() {
      LOG.info("Mocked: failed getResourceProfileFailed call");
      metrics.incrGetResourceProfileFailedRetrieved();
    }

    public void getAttributesToNodesFailed() {
      LOG.info("Mocked: failed getAttributesToNodesFailed call");
      metrics.incrGetAttributesToNodesFailedRetrieved();
    }

    public void getClusterNodeAttributesFailed() {
      LOG.info("Mocked: failed getClusterNodeAttributesFailed call");
      metrics.incrGetClusterNodeAttributesFailedRetrieved();
    }

    public void getNodesToAttributesFailed() {
      LOG.info("Mocked: failed getNodesToAttributesFailed call");
      metrics.incrGetNodesToAttributesFailedRetrieved();
    }

    public void getNewReservationFailed() {
      LOG.info("Mocked: failed getNewReservationFailed call");
      metrics.incrGetNewReservationFailedRetrieved();
    }

    public void getSubmitReservationFailed() {
      LOG.info("Mocked: failed getSubmitReservationFailed call");
      metrics.incrSubmitReservationFailedRetrieved();
    }

    public void getUpdateReservationFailed() {
      LOG.info("Mocked: failed getUpdateReservationFailed call");
      metrics.incrUpdateReservationFailedRetrieved();
    }

    public void getDeleteReservationFailed() {
      LOG.info("Mocked: failed getDeleteReservationFailed call");
      metrics.incrDeleteReservationFailedRetrieved();
    }

    public void getListReservationFailed() {
      LOG.info("Mocked: failed getListReservationFailed call");
      metrics.incrListReservationFailedRetrieved();
    }

    public void getAppActivitiesFailed() {
      LOG.info("Mocked: failed getAppActivitiesFailed call");
      metrics.incrGetAppActivitiesFailedRetrieved();
    }

    public void getAppStatisticsFailed() {
      LOG.info("Mocked: failed getAppStatisticsFailed call");
      metrics.incrGetAppStatisticsFailedRetrieved();
    }

    public void getAppPriorityFailed() {
      LOG.info("Mocked: failed getAppPriorityFailed call");
      metrics.incrGetAppPriorityFailedRetrieved();
    }

    public void getAppQueueFailed() {
      LOG.info("Mocked: failed getAppQueueFailed call");
      metrics.incrGetAppQueueFailedRetrieved();
    }

    public void getUpdateQueueFailed() {
      LOG.info("Mocked: failed getUpdateQueueFailed call");
      metrics.incrUpdateAppQueueFailedRetrieved();
    }

    public void getAppTimeoutFailed() {
      LOG.info("Mocked: failed getAppTimeoutFailed call");
      metrics.incrGetAppTimeoutFailedRetrieved();
    }

    public void getAppTimeoutsFailed() {
      LOG.info("Mocked: failed getAppTimeoutsFailed call");
      metrics.incrGetAppTimeoutsFailedRetrieved();
    }

    public void getRMNodeLabelsFailed() {
      LOG.info("Mocked: failed getRMNodeLabelsFailed call");
      metrics.incrGetRMNodeLabelsFailedRetrieved();
    }

    public void getCheckUserAccessToQueueFailed() {
      LOG.info("Mocked: failed checkUserAccessToQueue call");
      metrics.incrCheckUserAccessToQueueFailedRetrieved();
    }

    public void getDelegationTokenFailed() {
      LOG.info("Mocked: failed getDelegationToken call");
      metrics.incrGetDelegationTokenFailedRetrieved();
    }

    public void getRenewDelegationTokenFailed() {
      LOG.info("Mocked: failed renewDelegationToken call");
      metrics.incrRenewDelegationTokenFailedRetrieved();
    }

    public void getRefreshAdminAclsFailedRetrieved() {
      LOG.info("Mocked: failed refreshAdminAcls call");
      metrics.incrRefreshAdminAclsFailedRetrieved();
    }

    public void getRefreshServiceAclsFailedRetrieved() {
      LOG.info("Mocked: failed refreshServiceAcls call");
      metrics.incrRefreshServiceAclsFailedRetrieved();
    }

    public void getReplaceLabelsOnNodesFailed() {
      LOG.info("Mocked: failed replaceLabelsOnNodes call");
      metrics.incrReplaceLabelsOnNodesFailedRetrieved();
    }

    public void getReplaceLabelsOnNodeFailed() {
      LOG.info("Mocked: failed ReplaceLabelOnNode call");
      metrics.incrReplaceLabelsOnNodeFailedRetrieved();
    }

    public void getDumpSchedulerLogsFailed() {
      LOG.info("Mocked: failed DumpSchedulerLogs call");
      metrics.incrDumpSchedulerLogsFailedRetrieved();
    }

    public void getActivitiesFailed() {
      LOG.info("Mocked: failed getBulkActivitie call");
      metrics.incrGetActivitiesFailedRetrieved();
    }

    public void getBulkActivitiesFailed() {
      LOG.info("Mocked: failed getBulkActivitie call");
      metrics.incrGetBulkActivitiesFailedRetrieved();
    }

    public void getDeregisterSubClusterFailed() {
      LOG.info("Mocked: failed deregisterSubCluster call");
      metrics.incrDeregisterSubClusterFailedRetrieved();
    }

    public void getSchedulerConfigurationFailed() {
      LOG.info("Mocked: failed getSchedulerConfiguration call");
      metrics.incrGetSchedulerConfigurationFailedRetrieved();
    }

    public void updateSchedulerConfigurationFailedRetrieved() {
      LOG.info("Mocked: failed updateSchedulerConfiguration call");
      metrics.incrUpdateSchedulerConfigurationFailedRetrieved();
    }

    public void getClusterInfoFailed() {
      LOG.info("Mocked: failed getClusterInfo call");
      metrics.incrGetClusterInfoFailedRetrieved();
    }

    public void getClusterUserInfoFailed() {
      LOG.info("Mocked: failed getClusterUserInfo call");
      metrics.incrGetClusterUserInfoFailedRetrieved();
    }

    public void getUpdateNodeResourceFailed() {
      LOG.info("Mocked: failed getClusterUserInfo call");
      metrics.incrUpdateNodeResourceFailedRetrieved();
    }

    public void getRefreshNodesResourcesFailed() {
      LOG.info("Mocked: failed refreshNodesResources call");
      metrics.incrRefreshNodesResourcesFailedRetrieved();
    }

    public void getCheckForDecommissioningNodesFailed() {
      LOG.info("Mocked: failed checkForDecommissioningNodes call");
      metrics.incrCheckForDecommissioningNodesFailedRetrieved();
    }

    public void getRefreshClusterMaxPriorityFailed() {
      LOG.info("Mocked: failed refreshClusterMaxPriority call");
      metrics.incrRefreshClusterMaxPriorityFailedRetrieved();
    }

    public void getMapAttributesToNodesFailed() {
      LOG.info("Mocked: failed getMapAttributesToNode call");
      metrics.incrMapAttributesToNodesFailedRetrieved();
    }

    public void getGroupsForUserFailed() {
      LOG.info("Mocked: failed getGroupsForUser call");
      metrics.incrGetGroupsForUserFailedRetrieved();
    }

    public void getSaveFederationQueuePolicyFailedRetrieved() {
      LOG.info("Mocked: failed refreshClusterMaxPriority call");
      metrics.incrSaveFederationQueuePolicyFailedRetrieved();
    }

    public void getBatchSaveFederationQueuePoliciesFailedRetrieved() {
      LOG.info("Mocked: failed BatchSaveFederationQueuePolicies call");
      metrics.incrBatchSaveFederationQueuePoliciesFailedRetrieved();
    }

    public void getListFederationQueuePoliciesFailedRetrieved() {
      LOG.info("Mocked: failed ListFederationQueuePolicies call");
      metrics.incrListFederationQueuePoliciesFailedRetrieved();
    }

    public void getFederationSubClustersFailedRetrieved() {
      LOG.info("Mocked: failed GetFederationSubClusters call");
      metrics.incrGetFederationSubClustersFailedRetrieved();
    }
    public void getDeleteFederationPoliciesByQueuesFailedRetrieved() {
      LOG.info("Mocked: failed DeleteFederationPoliciesByQueues call");
      metrics.incrDeleteFederationPoliciesByQueuesRetrieved();
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
      LOG.info("Mocked: successful getApplicationAttemptReport call " +
          "with duration {}", duration);
      metrics.succeededAppAttemptReportRetrieved(duration);
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

    public void getQueueUserAcls(long duration) {
      LOG.info("Mocked: successful getQueueUserAcls call with duration {}", duration);
      metrics.succeededGetQueueUserAclsRetrieved(duration);
    }

    public void getListReservations(long duration) {
      LOG.info("Mocked: successful listReservations call with duration {}", duration);
      metrics.succeededListReservationsRetrieved(duration);
    }

    public void getApplicationAttempts(long duration) {
      LOG.info("Mocked: successful getApplicationAttempts call with duration {}", duration);
      metrics.succeededAppAttemptsRetrieved(duration);
    }

    public void getContainerReport(long duration) {
      LOG.info("Mocked: successful getContainerReport call with duration {}", duration);
      metrics.succeededGetContainerReportRetrieved(duration);
    }

    public void getContainers(long duration) {
      LOG.info("Mocked: successful getContainer call with duration {}", duration);
      metrics.succeededGetContainersRetrieved(duration);
    }

    public void getResourceTypeInfo(long duration) {
      LOG.info("Mocked: successful getResourceTypeInfo call with duration {}", duration);
      metrics.succeededGetResourceTypeInfoRetrieved(duration);
    }

    public void getFailApplicationAttempt(long duration) {
      LOG.info("Mocked: successful failApplicationAttempt call with duration {}", duration);
      metrics.succeededFailAppAttemptRetrieved(duration);
    }

    public void getUpdateApplicationPriority(long duration) {
      LOG.info("Mocked: successful updateApplicationPriority call with duration {}", duration);
      metrics.succeededUpdateAppPriorityRetrieved(duration);
    }

    public void getUpdateApplicationTimeouts(long duration) {
      LOG.info("Mocked: successful updateApplicationTimeouts call with duration {}", duration);
      metrics.succeededUpdateAppTimeoutsRetrieved(duration);
    }

    public void getSignalToContainerTimeouts(long duration) {
      LOG.info("Mocked: successful signalToContainer call with duration {}", duration);
      metrics.succeededSignalToContainerRetrieved(duration);
    }

    public void getQueueInfoRetrieved(long duration) {
      LOG.info("Mocked: successful getQueueInfo call with duration {}", duration);
      metrics.succeededGetQueueInfoRetrieved(duration);
    }

    public void moveApplicationAcrossQueuesRetrieved(long duration) {
      LOG.info("Mocked: successful moveApplicationAcrossQueues call with duration {}", duration);
      metrics.succeededMoveApplicationAcrossQueuesRetrieved(duration);
    }

    public void getResourceProfilesRetrieved(long duration) {
      LOG.info("Mocked: successful getResourceProfiles call with duration {}", duration);
      metrics.succeededGetResourceProfilesRetrieved(duration);
    }

    public void getResourceProfileRetrieved(long duration) {
      LOG.info("Mocked: successful getResourceProfile call with duration {}", duration);
      metrics.succeededGetResourceProfileRetrieved(duration);
    }

    public void getAttributesToNodesRetrieved(long duration) {
      LOG.info("Mocked: successful getAttributesToNodes call with duration {}", duration);
      metrics.succeededGetAttributesToNodesRetrieved(duration);
    }

    public void getClusterNodeAttributesRetrieved(long duration) {
      LOG.info("Mocked: successful getClusterNodeAttributes call with duration {}", duration);
      metrics.succeededGetClusterNodeAttributesRetrieved(duration);
    }

    public void getNodesToAttributesRetrieved(long duration) {
      LOG.info("Mocked: successful getNodesToAttributes call with duration {}", duration);
      metrics.succeededGetNodesToAttributesRetrieved(duration);
    }

    public void getNewReservationRetrieved(long duration) {
      LOG.info("Mocked: successful getNewReservation call with duration {}", duration);
      metrics.succeededGetNewReservationRetrieved(duration);
    }

    public void getSubmitReservationRetrieved(long duration) {
      LOG.info("Mocked: successful getSubmitReservation call with duration {}", duration);
      metrics.succeededSubmitReservationRetrieved(duration);
    }

    public void getUpdateReservationRetrieved(long duration) {
      LOG.info("Mocked: successful getUpdateReservation call with duration {}", duration);
      metrics.succeededUpdateReservationRetrieved(duration);
    }

    public void getDeleteReservationRetrieved(long duration) {
      LOG.info("Mocked: successful getDeleteReservation call with duration {}", duration);
      metrics.succeededDeleteReservationRetrieved(duration);
    }

    public void getListReservationRetrieved(long duration) {
      LOG.info("Mocked: successful getListReservation call with duration {}", duration);
      metrics.succeededListReservationRetrieved(duration);
    }

    public void getAppActivitiesRetrieved(long duration) {
      LOG.info("Mocked: successful getAppActivities call with duration {}", duration);
      metrics.succeededGetAppActivitiesRetrieved(duration);
    }

    public void getAppStatisticsRetrieved(long duration) {
      LOG.info("Mocked: successful getAppStatistics call with duration {}", duration);
      metrics.succeededGetAppStatisticsRetrieved(duration);
    }

    public void getAppPriorityRetrieved(long duration) {
      LOG.info("Mocked: successful getAppPriority call with duration {}", duration);
      metrics.succeededGetAppPriorityRetrieved(duration);
    }

    public void getAppQueueRetrieved(long duration) {
      LOG.info("Mocked: successful getAppQueue call with duration {}", duration);
      metrics.succeededGetAppQueueRetrieved(duration);
    }

    public void getUpdateQueueRetrieved(long duration) {
      LOG.info("Mocked: successful getUpdateQueue call with duration {}", duration);
      metrics.succeededUpdateAppQueueRetrieved(duration);
    }

    public void getAppTimeoutRetrieved(long duration) {
      LOG.info("Mocked: successful getAppTimeout call with duration {}", duration);
      metrics.succeededGetAppTimeoutRetrieved(duration);
    }

    public void getAppTimeoutsRetrieved(long duration) {
      LOG.info("Mocked: successful getAppTimeouts call with duration {}", duration);
      metrics.succeededGetAppTimeoutsRetrieved(duration);
    }

    public void getRMNodeLabelsRetrieved(long duration) {
      LOG.info("Mocked: successful getRMNodeLabels call with duration {}", duration);
      metrics.succeededGetRMNodeLabelsRetrieved(duration);
    }

    public void getCheckUserAccessToQueueRetrieved(long duration) {
      LOG.info("Mocked: successful CheckUserAccessToQueue call with duration {}", duration);
      metrics.succeededCheckUserAccessToQueueRetrieved(duration);
    }

    public void getGetDelegationTokenRetrieved(long duration) {
      LOG.info("Mocked: successful GetDelegationToken call with duration {}", duration);
      metrics.succeededGetDelegationTokenRetrieved(duration);
    }

    public void getRenewDelegationTokenRetrieved(long duration) {
      LOG.info("Mocked: successful RenewDelegationToken call with duration {}", duration);
      metrics.succeededRenewDelegationTokenRetrieved(duration);
    }

    public void getRefreshAdminAclsRetrieved(long duration) {
      LOG.info("Mocked: successful RefreshAdminAcls call with duration {}", duration);
      metrics.succeededRefreshAdminAclsRetrieved(duration);
    }

    public void getRefreshServiceAclsRetrieved(long duration) {
      LOG.info("Mocked: successful RefreshServiceAcls call with duration {}", duration);
      metrics.succeededRefreshServiceAclsRetrieved(duration);
    }

    public void getNumSucceededReplaceLabelsOnNodesRetrieved(long duration) {
      LOG.info("Mocked: successful ReplaceLabelsOnNodes call with duration {}", duration);
      metrics.succeededReplaceLabelsOnNodesRetrieved(duration);
    }

    public void getNumSucceededReplaceLabelsOnNodeRetrieved(long duration) {
      LOG.info("Mocked: successful ReplaceLabelOnNode call with duration {}", duration);
      metrics.succeededReplaceLabelsOnNodeRetrieved(duration);
    }

    public void getDumpSchedulerLogsRetrieved(long duration) {
      LOG.info("Mocked: successful DumpSchedulerLogs call with duration {}", duration);
      metrics.succeededDumpSchedulerLogsRetrieved(duration);
    }

    public void getActivitiesRetrieved(long duration) {
      LOG.info("Mocked: successful GetActivities call with duration {}", duration);
      metrics.succeededGetActivitiesLatencyRetrieved(duration);
    }

    public void getBulkActivitiesRetrieved(long duration) {
      LOG.info("Mocked: successful GetBulkActivities call with duration {}", duration);
      metrics.succeededGetBulkActivitiesRetrieved(duration);
    }

    public void getDeregisterSubClusterRetrieved(long duration) {
      LOG.info("Mocked: successful DeregisterSubCluster call with duration {}", duration);
      metrics.succeededDeregisterSubClusterRetrieved(duration);
    }

    public void addToClusterNodeLabelsRetrieved(long duration) {
      LOG.info("Mocked: successful AddToClusterNodeLabels call with duration {}", duration);
      metrics.succeededAddToClusterNodeLabelsRetrieved(duration);
    }

    public void getSchedulerConfigurationRetrieved(long duration) {
      LOG.info("Mocked: successful GetSchedulerConfiguration call with duration {}", duration);
      metrics.succeededGetSchedulerConfigurationRetrieved(duration);
    }

    public void getUpdateSchedulerConfigurationRetrieved(long duration) {
      LOG.info("Mocked: successful UpdateSchedulerConfiguration call with duration {}", duration);
      metrics.succeededUpdateSchedulerConfigurationRetrieved(duration);
    }

    public void getClusterInfoRetrieved(long duration) {
      LOG.info("Mocked: successful GetClusterInfoRetrieved call with duration {}", duration);
      metrics.succeededGetClusterInfoRetrieved(duration);
    }

    public void getClusterUserInfoRetrieved(long duration) {
      LOG.info("Mocked: successful GetClusterUserInfoRetrieved call with duration {}", duration);
      metrics.succeededGetClusterUserInfoRetrieved(duration);
    }

    public void getUpdateNodeResourceRetrieved(long duration) {
      LOG.info("Mocked: successful UpdateNodeResourceRetrieved call with duration {}", duration);
      metrics.succeededUpdateNodeResourceRetrieved(duration);
    }

    public void getRefreshNodesResourcesRetrieved(long duration) {
      LOG.info("Mocked: successful RefreshNodesResourcesRetrieved call with duration {}", duration);
      metrics.succeededRefreshNodesResourcesRetrieved(duration);
    }

    public void getCheckForDecommissioningNodesRetrieved(long duration) {
      LOG.info("Mocked: successful CheckForDecommissioningNodesRetrieved call with duration {}",
          duration);
      metrics.succeededCheckForDecommissioningNodesRetrieved(duration);
    }

    public void getRefreshClusterMaxPriorityRetrieved(long duration) {
      LOG.info("Mocked: successful RefreshClusterMaxPriority call with duration {}",
          duration);
      metrics.succeededRefreshClusterMaxPriorityRetrieved(duration);
    }

    public void getMapAttributesToNodesRetrieved(long duration) {
      LOG.info("Mocked: successful MapAttributesToNodes call with duration {}",
          duration);
      metrics.succeededMapAttributesToNodesRetrieved(duration);
    }

    public void getGroupsForUsersRetrieved(long duration) {
      LOG.info("Mocked: successful GetGroupsForUsers call with duration {}",
          duration);
      metrics.succeededGetGroupsForUsersRetrieved(duration);
    }

    public void getSaveFederationQueuePolicyRetrieved(long duration) {
      LOG.info("Mocked: successful SaveFederationQueuePolicy call with duration {}",
          duration);
      metrics.succeededSaveFederationQueuePolicyRetrieved(duration);
    }

    public void getBatchSaveFederationQueuePoliciesRetrieved(long duration) {
      LOG.info("Mocked: successful BatchSaveFederationQueuePoliciesRetrieved " +
          " call with duration {}", duration);
      metrics.succeededBatchSaveFederationQueuePoliciesRetrieved(duration);
    }

    public void getListFederationQueuePoliciesRetrieved(long duration) {
      LOG.info("Mocked: successful ListFederationQueuePoliciesRetrieved " +
          " call with duration {}", duration);
      metrics.succeededListFederationQueuePoliciesRetrieved(duration);
    }

    public void getFederationSubClustersRetrieved(long duration) {
      LOG.info("Mocked: successful GetFederationSubClustersRetrieved " +
          " call with duration {}", duration);
      metrics.succeededGetFederationSubClustersRetrieved(duration);
    }
    public void deleteFederationPoliciesByQueuesRetrieved(long duration) {
      LOG.info("Mocked: successful DeleteFederationPoliciesByQueuesRetrieved " +
          " call with duration {}", duration);
      metrics.succeededDeleteFederationPoliciesByQueuesRetrieved(duration);
    }
  }

  @Test
  public void testSucceededGetClusterNodes() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterNodesRetrieved();
    goodSubCluster.getClusterNodes(150);
    assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetClusterNodesRetrieved());
    assertEquals(150, metrics.getLatencySucceededGetClusterNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterNodes(300);
    assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetClusterNodesRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetClusterNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetClusterNodesFailed() {
    long totalBadBefore = metrics.getClusterNodesFailedRetrieved();
    badSubCluster.getClusterNodes();
    assertEquals(totalBadBefore + 1, metrics.getClusterNodesFailedRetrieved());
  }

  @Test
  public void testSucceededGetNodeToLabels() {
    long totalGoodBefore = metrics.getNumSucceededGetNodeToLabelsRetrieved();
    goodSubCluster.getNodeToLabels(150);
    assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetNodeToLabelsRetrieved());
    assertEquals(150, metrics.getLatencySucceededGetNodeToLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNodeToLabels(300);
    assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetNodeToLabelsRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetNodeToLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetNodeToLabelsFailed() {
    long totalBadBefore = metrics.getNodeToLabelsFailedRetrieved();
    badSubCluster.getNodeToLabels();
    assertEquals(totalBadBefore + 1, metrics.getNodeToLabelsFailedRetrieved());
  }

  @Test
  public void testSucceededLabelsToNodes() {
    long totalGoodBefore = metrics.getNumSucceededGetLabelsToNodesRetrieved();
    goodSubCluster.getLabelToNodes(150);
    assertEquals(totalGoodBefore + 1, metrics.getNumSucceededGetLabelsToNodesRetrieved());
    assertEquals(150, metrics.getLatencySucceededGetLabelsToNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getLabelToNodes(300);
    assertEquals(totalGoodBefore + 2, metrics.getNumSucceededGetLabelsToNodesRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetLabelsToNodesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetLabelsToNodesFailed() {
    long totalBadBefore = metrics.getLabelsToNodesFailedRetrieved();
    badSubCluster.getLabelToNodes();
    assertEquals(totalBadBefore + 1, metrics.getLabelsToNodesFailedRetrieved());
  }

  @Test
  public void testSucceededClusterNodeLabels() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterNodeLabelsRetrieved();
    goodSubCluster.getClusterNodeLabels(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterNodeLabelsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetClusterNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterNodeLabels(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterNodeLabelsRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetClusterNodeLabelsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testClusterNodeLabelsFailed() {
    long totalBadBefore = metrics.getGetClusterNodeLabelsFailedRetrieved();
    badSubCluster.getClusterNodeLabels();
    assertEquals(totalBadBefore + 1, metrics.getGetClusterNodeLabelsFailedRetrieved());
  }

  @Test
  public void testSucceededQueueUserAcls() {
    long totalGoodBefore = metrics.getNumSucceededGetQueueUserAclsRetrieved();
    goodSubCluster.getQueueUserAcls(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetQueueUserAclsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetQueueUserAclsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getQueueUserAcls(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetQueueUserAclsRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetQueueUserAclsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testQueueUserAclsFailed() {
    long totalBadBefore = metrics.getQueueUserAclsFailedRetrieved();
    badSubCluster.getQueueUserAcls();
    assertEquals(totalBadBefore + 1, metrics.getQueueUserAclsFailedRetrieved());
  }
  @Test
  public void testSucceededListReservations() {
    long totalGoodBefore = metrics.getNumSucceededListReservationsRetrieved();
    goodSubCluster.getListReservations(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededListReservationsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededListReservationsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getListReservations(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededListReservationsRetrieved());
    assertEquals(225, metrics.getLatencySucceededListReservationsRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testListReservationsFailed() {
    long totalBadBefore = metrics.getListReservationsFailedRetrieved();
    badSubCluster.getListReservations();
    assertEquals(totalBadBefore + 1, metrics.getListReservationsFailedRetrieved());
  }

  @Test
  public void testSucceededGetApplicationAttempts() {
    long totalGoodBefore = metrics.getNumSucceededAppAttemptsRetrieved();
    goodSubCluster.getApplicationAttempts(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAppAttemptsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededAppAttemptRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getApplicationAttempts(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAppAttemptsRetrieved());
    assertEquals(225, metrics.getLatencySucceededAppAttemptRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetApplicationAttemptsFailed() {
    long totalBadBefore = metrics.getAppAttemptsFailedRetrieved();
    badSubCluster.getApplicationAttempts();
    assertEquals(totalBadBefore + 1, metrics.getAppAttemptsFailedRetrieved());
  }

  @Test
  public void testSucceededGetContainerReport() {
    long totalGoodBefore = metrics.getNumSucceededGetContainerReportRetrieved();
    goodSubCluster.getContainerReport(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetContainerReportRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetContainerReportRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getContainerReport(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetContainerReportRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetContainerReportRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetContainerReportFailed() {
    long totalBadBefore = metrics.getContainerReportFailedRetrieved();
    badSubCluster.getContainerReport();
    assertEquals(totalBadBefore + 1, metrics.getContainerReportFailedRetrieved());
  }

  @Test
  public void testSucceededGetContainers() {
    long totalGoodBefore = metrics.getNumSucceededGetContainersRetrieved();
    goodSubCluster.getContainers(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetContainersRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetContainersRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getContainers(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetContainersRetrieved());
    assertEquals(225, metrics.getLatencySucceededGetContainersRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetContainersFailed() {
    long totalBadBefore = metrics.getContainersFailedRetrieved();
    badSubCluster.getContainers();
    assertEquals(totalBadBefore + 1, metrics.getContainersFailedRetrieved());
  }

  @Test
  public void testSucceededGetResourceTypeInfo() {
    long totalGoodBefore = metrics.getNumSucceededGetResourceTypeInfoRetrieved();
    goodSubCluster.getResourceTypeInfo(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetResourceTypeInfoRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetResourceTypeInfoRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getResourceTypeInfo(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetResourceTypeInfoRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetResourceTypeInfoRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetResourceTypeInfoFailed() {
    long totalBadBefore = metrics.getGetResourceTypeInfoRetrieved();
    badSubCluster.getResourceTypeInfo();
    assertEquals(totalBadBefore + 1, metrics.getGetResourceTypeInfoRetrieved());
  }

  @Test
  public void testSucceededFailApplicationAttempt() {
    long totalGoodBefore = metrics.getNumSucceededFailAppAttemptRetrieved();
    goodSubCluster.getFailApplicationAttempt(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededFailAppAttemptRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededFailAppAttemptRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getFailApplicationAttempt(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededFailAppAttemptRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededFailAppAttemptRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testFailApplicationAttemptFailed() {
    long totalBadBefore = metrics.getFailApplicationAttemptFailedRetrieved();
    badSubCluster.getFailApplicationAttempt();
    assertEquals(totalBadBefore + 1, metrics.getFailApplicationAttemptFailedRetrieved());
  }

  @Test
  public void testSucceededUpdateApplicationPriority() {
    long totalGoodBefore = metrics.getNumSucceededUpdateAppPriorityRetrieved();
    goodSubCluster.getUpdateApplicationPriority(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateAppPriorityRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateAppPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateApplicationPriority(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateAppPriorityRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateAppPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testUpdateApplicationPriorityFailed() {
    long totalBadBefore = metrics.getUpdateApplicationPriorityFailedRetrieved();
    badSubCluster.getUpdateApplicationPriority();
    assertEquals(totalBadBefore + 1,
        metrics.getUpdateApplicationPriorityFailedRetrieved());
  }

  @Test
  public void testSucceededUpdateAppTimeoutsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededUpdateAppTimeoutsRetrieved();
    goodSubCluster.getUpdateApplicationTimeouts(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateAppTimeoutsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateAppTimeoutsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateApplicationTimeouts(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateAppTimeoutsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateAppTimeoutsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testUpdateAppTimeoutsFailed() {
    long totalBadBefore = metrics.getUpdateApplicationTimeoutsFailedRetrieved();
    badSubCluster.getUpdateApplicationTimeouts();
    assertEquals(totalBadBefore + 1,
        metrics.getUpdateApplicationTimeoutsFailedRetrieved());
  }

  @Test
  public void testSucceededSignalToContainerRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededSignalToContainerRetrieved();
    goodSubCluster.getSignalToContainerTimeouts(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededSignalToContainerRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededSignalToContainerRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getSignalToContainerTimeouts(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededSignalToContainerRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededSignalToContainerRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testSignalToContainerFailed() {
    long totalBadBefore = metrics.getSignalToContainerFailedRetrieved();
    badSubCluster.getSignalContainer();
    assertEquals(totalBadBefore + 1,
        metrics.getSignalToContainerFailedRetrieved());
  }

  @Test
  public void testSucceededGetQueueInfoRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetQueueInfoRetrieved();
    goodSubCluster.getQueueInfoRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetQueueInfoRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetQueueInfoRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getQueueInfoRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetQueueInfoRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetQueueInfoRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetQueueInfoFailed() {
    long totalBadBefore = metrics.getQueueInfoFailedRetrieved();
    badSubCluster.getQueueInfo();
    assertEquals(totalBadBefore + 1,
        metrics.getQueueInfoFailedRetrieved());
  }

  @Test
  public void testSucceededMoveApplicationAcrossQueuesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededMoveApplicationAcrossQueuesRetrieved();
    goodSubCluster.moveApplicationAcrossQueuesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededMoveApplicationAcrossQueuesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededMoveApplicationAcrossQueuesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.moveApplicationAcrossQueuesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededMoveApplicationAcrossQueuesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededMoveApplicationAcrossQueuesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testMoveApplicationAcrossQueuesRetrievedFailed() {
    long totalBadBefore = metrics.getMoveApplicationAcrossQueuesFailedRetrieved();
    badSubCluster.moveApplicationAcrossQueuesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getMoveApplicationAcrossQueuesFailedRetrieved());
  }

  @Test
  public void testSucceededGetResourceProfilesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetResourceProfilesRetrieved();
    goodSubCluster.getResourceProfilesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetResourceProfilesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetResourceProfilesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getResourceProfilesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetResourceProfilesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetResourceProfilesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetResourceProfilesRetrievedFailed() {
    long totalBadBefore = metrics.getResourceProfilesFailedRetrieved();
    badSubCluster.getResourceProfilesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getResourceProfilesFailedRetrieved());
  }

  @Test
  public void testSucceededGetResourceProfileRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetResourceProfileRetrieved();
    goodSubCluster.getResourceProfileRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetResourceProfileRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetResourceProfileRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getResourceProfileRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetResourceProfileRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetResourceProfileRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetResourceProfileRetrievedFailed() {
    long totalBadBefore = metrics.getResourceProfileFailedRetrieved();
    badSubCluster.getResourceProfileFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getResourceProfileFailedRetrieved());
  }

  @Test
  public void testSucceededGetAttributesToNodesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAttributesToNodesRetrieved();
    goodSubCluster.getAttributesToNodesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAttributesToNodesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAttributesToNodesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAttributesToNodesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAttributesToNodesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAttributesToNodesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAttributesToNodesRetrievedFailed() {
    long totalBadBefore = metrics.getAttributesToNodesFailedRetrieved();
    badSubCluster.getAttributesToNodesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAttributesToNodesFailedRetrieved());
  }

  @Test
  public void testGetClusterNodeAttributesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterNodeAttributesRetrieved();
    goodSubCluster.getClusterNodeAttributesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterNodeAttributesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetClusterNodeAttributesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterNodeAttributesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterNodeAttributesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetClusterNodeAttributesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetClusterNodeAttributesRetrievedFailed() {
    long totalBadBefore = metrics.getClusterNodeAttributesFailedRetrieved();
    badSubCluster.getClusterNodeAttributesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getClusterNodeAttributesFailedRetrieved());
  }

  @Test
  public void testGetNodesToAttributesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetNodesToAttributesRetrieved();
    goodSubCluster.getNodesToAttributesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetNodesToAttributesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetNodesToAttributesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNodesToAttributesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetNodesToAttributesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetNodesToAttributesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetNodesToAttributesRetrievedFailed() {
    long totalBadBefore = metrics.getNodesToAttributesFailedRetrieved();
    badSubCluster.getNodesToAttributesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getNodesToAttributesFailedRetrieved());
  }

  @Test
  public void testGetNewReservationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetNewReservationRetrieved();
    goodSubCluster.getNewReservationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetNewReservationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetNewReservationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNewReservationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetNewReservationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetNewReservationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetNewReservationRetrievedFailed() {
    long totalBadBefore = metrics.getNewReservationFailedRetrieved();
    badSubCluster.getNewReservationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getNewReservationFailedRetrieved());
  }

  @Test
  public void testGetSubmitReservationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededSubmitReservationRetrieved();
    goodSubCluster.getSubmitReservationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededSubmitReservationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededSubmitReservationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getSubmitReservationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededSubmitReservationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededSubmitReservationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetSubmitReservationRetrievedFailed() {
    long totalBadBefore = metrics.getSubmitReservationFailedRetrieved();
    badSubCluster.getSubmitReservationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getSubmitReservationFailedRetrieved());
  }

  @Test
  public void testGetUpdateReservationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededUpdateReservationRetrieved();
    goodSubCluster.getUpdateReservationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateReservationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateReservationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateReservationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateReservationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateReservationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetUpdateReservationRetrievedFailed() {
    long totalBadBefore = metrics.getUpdateReservationFailedRetrieved();
    badSubCluster.getUpdateReservationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getUpdateReservationFailedRetrieved());
  }

  @Test
  public void testGetDeleteReservationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededDeleteReservationRetrieved();
    goodSubCluster.getDeleteReservationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededDeleteReservationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededDeleteReservationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getDeleteReservationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededDeleteReservationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededDeleteReservationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetDeleteReservationRetrievedFailed() {
    long totalBadBefore = metrics.getDeleteReservationFailedRetrieved();
    badSubCluster.getDeleteReservationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getDeleteReservationFailedRetrieved());
  }

  @Test
  public void testGetListReservationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededListReservationRetrieved();
    goodSubCluster.getListReservationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededListReservationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededListReservationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getListReservationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededListReservationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededListReservationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetListReservationRetrievedFailed() {
    long totalBadBefore = metrics.getListReservationFailedRetrieved();
    badSubCluster.getListReservationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getListReservationFailedRetrieved());
  }

  @Test
  public void testGetAppActivitiesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppActivitiesRetrieved();
    goodSubCluster.getAppActivitiesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppActivitiesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppActivitiesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppActivitiesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppActivitiesRetrievedFailed() {
    long totalBadBefore = metrics.getAppActivitiesFailedRetrieved();
    badSubCluster.getAppActivitiesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppActivitiesFailedRetrieved());
  }

  @Test
  public void testGetAppStatisticsLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppStatisticsRetrieved();
    goodSubCluster.getAppStatisticsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppStatisticsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppStatisticsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppStatisticsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppStatisticsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppStatisticsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppStatisticsRetrievedFailed() {
    long totalBadBefore = metrics.getAppStatisticsFailedRetrieved();
    badSubCluster.getAppStatisticsFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppStatisticsFailedRetrieved());
  }

  @Test
  public void testGetAppPriorityLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppPriorityRetrieved();
    goodSubCluster.getAppPriorityRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppPriorityRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppPriorityRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppPriorityRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppPriorityRetrievedFailed() {
    long totalBadBefore = metrics.getAppPriorityFailedRetrieved();
    badSubCluster.getAppPriorityFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppPriorityFailedRetrieved());
  }

  @Test
  public void testGetAppQueueLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppQueueRetrieved();
    goodSubCluster.getAppQueueRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppQueueRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppQueueRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppQueueRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppQueueRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppQueueRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppQueueRetrievedFailed() {
    long totalBadBefore = metrics.getAppQueueFailedRetrieved();
    badSubCluster.getAppQueueFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppQueueFailedRetrieved());
  }

  @Test
  public void testUpdateAppQueueLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededUpdateAppQueueRetrieved();
    goodSubCluster.getUpdateQueueRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateAppQueueRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateAppQueueRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateQueueRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateAppQueueRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateAppQueueRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testUpdateAppQueueRetrievedFailed() {
    long totalBadBefore = metrics.getUpdateAppQueueFailedRetrieved();
    badSubCluster.getUpdateQueueFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getUpdateAppQueueFailedRetrieved());
  }

  @Test
  public void testGetAppTimeoutLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppTimeoutRetrieved();
    goodSubCluster.getAppTimeoutRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppTimeoutRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppTimeoutRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppTimeoutRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppTimeoutRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppTimeoutRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppTimeoutRetrievedFailed() {
    long totalBadBefore = metrics.getAppTimeoutFailedRetrieved();
    badSubCluster.getAppTimeoutFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppTimeoutFailedRetrieved());
  }

  @Test
  public void testGetAppTimeoutsLatencyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetAppTimeoutsRetrieved();
    goodSubCluster.getAppTimeoutsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetAppTimeoutsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetAppTimeoutsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getAppTimeoutsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetAppTimeoutsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetAppTimeoutsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetAppTimeoutsRetrievedFailed() {
    long totalBadBefore = metrics.getAppTimeoutsFailedRetrieved();
    badSubCluster.getAppTimeoutsFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getAppTimeoutsFailedRetrieved());
  }

  @Test
  public void testGetRMNodeLabelsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetRMNodeLabelsRetrieved();
    goodSubCluster.getRMNodeLabelsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetRMNodeLabelsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetRMNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRMNodeLabelsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetRMNodeLabelsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetRMNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetRMNodeLabelsRetrievedFailed() {
    long totalBadBefore = metrics.getRMNodeLabelsFailedRetrieved();
    badSubCluster.getRMNodeLabelsFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getRMNodeLabelsFailedRetrieved());
  }

  @Test
  public void testCheckUserAccessToQueueRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededCheckUserAccessToQueueRetrieved();
    goodSubCluster.getCheckUserAccessToQueueRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededCheckUserAccessToQueueRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededCheckUserAccessToQueueRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getCheckUserAccessToQueueRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededCheckUserAccessToQueueRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededCheckUserAccessToQueueRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testCheckUserAccessToQueueRetrievedFailed() {
    long totalBadBefore = metrics.getCheckUserAccessToQueueFailedRetrieved();
    badSubCluster.getCheckUserAccessToQueueFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getCheckUserAccessToQueueFailedRetrieved());
  }

  @Test
  public void testGetDelegationTokenRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetDelegationTokenRetrieved();
    goodSubCluster.getGetDelegationTokenRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetDelegationTokenRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetDelegationTokenRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getGetDelegationTokenRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetDelegationTokenRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetDelegationTokenRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetDelegationTokenRetrievedFailed() {
    long totalBadBefore = metrics.getDelegationTokenFailedRetrieved();
    badSubCluster.getDelegationTokenFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getDelegationTokenFailedRetrieved());
  }

  @Test
  public void testRenewDelegationTokenRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededRenewDelegationTokenRetrieved();
    goodSubCluster.getRenewDelegationTokenRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededRenewDelegationTokenRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededRenewDelegationTokenRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRenewDelegationTokenRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededRenewDelegationTokenRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededRenewDelegationTokenRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testRenewDelegationTokenRetrievedFailed() {
    long totalBadBefore = metrics.getRenewDelegationTokenFailedRetrieved();
    badSubCluster.getRenewDelegationTokenFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getRenewDelegationTokenFailedRetrieved());
  }

  @Test
  public void testRefreshAdminAclsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededRefreshAdminAclsRetrieved();
    goodSubCluster.getRefreshAdminAclsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededRefreshAdminAclsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededRefreshAdminAclsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRefreshAdminAclsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededRefreshAdminAclsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededRefreshAdminAclsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testRefreshAdminAclsRetrievedFailed() {
    long totalBadBefore = metrics.getNumRefreshAdminAclsFailedRetrieved();
    badSubCluster.getRefreshAdminAclsFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getNumRefreshAdminAclsFailedRetrieved());
  }

  @Test
  public void testRefreshServiceAclsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededRefreshServiceAclsRetrieved();
    goodSubCluster.getRefreshServiceAclsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededRefreshServiceAclsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededRefreshServiceAclsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRefreshServiceAclsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededRefreshServiceAclsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededRefreshServiceAclsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testRefreshServiceAclsRetrievedFailed() {
    long totalBadBefore = metrics.getNumRefreshServiceAclsFailedRetrieved();
    badSubCluster.getRefreshServiceAclsFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getNumRefreshServiceAclsFailedRetrieved());
  }

  @Test
  public void testReplaceLabelsOnNodesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededReplaceLabelsOnNodesRetrieved();
    goodSubCluster.getNumSucceededReplaceLabelsOnNodesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededReplaceLabelsOnNodesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededReplaceLabelsOnNodesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNumSucceededReplaceLabelsOnNodesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededReplaceLabelsOnNodesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededReplaceLabelsOnNodesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testReplaceLabelsOnNodesRetrievedFailed() {
    long totalBadBefore = metrics.getNumReplaceLabelsOnNodesFailedRetrieved();
    badSubCluster.getReplaceLabelsOnNodesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getNumReplaceLabelsOnNodesFailedRetrieved());
  }

  @Test
  public void testReplaceLabelsOnNodeRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededReplaceLabelsOnNodeRetrieved();
    goodSubCluster.getNumSucceededReplaceLabelsOnNodeRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededReplaceLabelsOnNodeRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededReplaceLabelsOnNodeRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getNumSucceededReplaceLabelsOnNodeRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededReplaceLabelsOnNodeRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededReplaceLabelsOnNodeRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testReplaceLabelOnNodeRetrievedFailed() {
    long totalBadBefore = metrics.getNumReplaceLabelsOnNodeFailedRetrieved();
    badSubCluster.getReplaceLabelsOnNodeFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getNumReplaceLabelsOnNodeFailedRetrieved());
  }

  @Test
  public void testDumpSchedulerLogsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededDumpSchedulerLogsRetrieved();
    goodSubCluster.getDumpSchedulerLogsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededDumpSchedulerLogsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededDumpSchedulerLogsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getDumpSchedulerLogsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededDumpSchedulerLogsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededDumpSchedulerLogsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testDumpSchedulerLogsRetrievedFailed() {
    long totalBadBefore = metrics.getDumpSchedulerLogsFailedRetrieved();
    badSubCluster.getDumpSchedulerLogsFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getDumpSchedulerLogsFailedRetrieved());
  }

  @Test
  public void testGetActivitiesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetActivitiesRetrieved();
    goodSubCluster.getActivitiesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetActivitiesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getActivitiesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetActivitiesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetActivitiesRetrievedFailed() {
    long totalBadBefore = metrics.getActivitiesFailedRetrieved();
    badSubCluster.getActivitiesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getActivitiesFailedRetrieved());
  }

  @Test
  public void testGetBulkActivitiesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetBulkActivitiesRetrieved();
    goodSubCluster.getBulkActivitiesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetBulkActivitiesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetBulkActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getBulkActivitiesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetBulkActivitiesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetBulkActivitiesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetBulkActivitiesRetrievedFailed() {
    long totalBadBefore = metrics.getBulkActivitiesFailedRetrieved();
    badSubCluster.getBulkActivitiesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getBulkActivitiesFailedRetrieved());
  }

  @Test
  public void testDeregisterSubClusterRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededDeregisterSubClusterRetrieved();
    goodSubCluster.getDeregisterSubClusterRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededDeregisterSubClusterRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededDeregisterSubClusterRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getDeregisterSubClusterRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededDeregisterSubClusterRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededDeregisterSubClusterRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testDeregisterSubClusterRetrievedFailed() {
    long totalBadBefore = metrics.getDeregisterSubClusterFailedRetrieved();
    badSubCluster.getDeregisterSubClusterFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getDeregisterSubClusterFailedRetrieved());
  }

  @Test
  public void testAddToClusterNodeLabelsRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededAddToClusterNodeLabelsRetrieved();
    goodSubCluster.addToClusterNodeLabelsRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededAddToClusterNodeLabelsRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededAddToClusterNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.addToClusterNodeLabelsRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededAddToClusterNodeLabelsRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededAddToClusterNodeLabelsRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetSchedulerConfigurationRetrievedFailed() {
    long totalBadBefore = metrics.getSchedulerConfigurationFailedRetrieved();
    badSubCluster.getSchedulerConfigurationFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getSchedulerConfigurationFailedRetrieved());
  }

  @Test
  public void testGetSchedulerConfigurationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetSchedulerConfigurationRetrieved();
    goodSubCluster.getSchedulerConfigurationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetSchedulerConfigurationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetSchedulerConfigurationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getSchedulerConfigurationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetSchedulerConfigurationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetSchedulerConfigurationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testUpdateSchedulerConfigurationRetrievedFailed() {
    long totalBadBefore = metrics.getUpdateSchedulerConfigurationFailedRetrieved();
    badSubCluster.updateSchedulerConfigurationFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getUpdateSchedulerConfigurationFailedRetrieved());
  }

  @Test
  public void testUpdateSchedulerConfigurationRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededUpdateSchedulerConfigurationRetrieved();
    goodSubCluster.getUpdateSchedulerConfigurationRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateSchedulerConfigurationRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateSchedulerConfigurationRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateSchedulerConfigurationRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateSchedulerConfigurationRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateSchedulerConfigurationRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetClusterInfoRetrievedFailed() {
    long totalBadBefore = metrics.getClusterInfoFailedRetrieved();
    badSubCluster.getClusterInfoFailed();
    assertEquals(totalBadBefore + 1, metrics.getClusterInfoFailedRetrieved());
  }

  @Test
  public void testGetClusterInfoRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterInfoRetrieved();
    goodSubCluster.getClusterInfoRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterInfoRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetClusterInfoRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterInfoRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterInfoRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetClusterInfoRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetClusterUserInfoRetrievedFailed() {
    long totalBadBefore = metrics.getClusterUserInfoFailedRetrieved();
    badSubCluster.getClusterUserInfoFailed();
    assertEquals(totalBadBefore + 1, metrics.getClusterUserInfoFailedRetrieved());
  }

  @Test
  public void testGetClusterUserInfoRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetClusterUserInfoRetrieved();
    goodSubCluster.getClusterUserInfoRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetClusterUserInfoRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetClusterUserInfoRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getClusterUserInfoRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetClusterUserInfoRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetClusterUserInfoRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testUpdateNodeResourceRetrievedFailed() {
    long totalBadBefore = metrics.getUpdateNodeResourceFailedRetrieved();
    badSubCluster.getUpdateNodeResourceFailed();
    assertEquals(totalBadBefore + 1, metrics.getUpdateNodeResourceFailedRetrieved());
  }

  @Test
  public void testUpdateNodeResourceRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededUpdateNodeResourceRetrieved();
    goodSubCluster.getUpdateNodeResourceRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededUpdateNodeResourceRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededUpdateNodeResourceRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getUpdateNodeResourceRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededUpdateNodeResourceRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededUpdateNodeResourceRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testRefreshNodesResourcesRetrievedFailed() {
    long totalBadBefore = metrics.getRefreshNodesResourcesFailedRetrieved();
    badSubCluster.getRefreshNodesResourcesFailed();
    assertEquals(totalBadBefore + 1, metrics.getRefreshNodesResourcesFailedRetrieved());
  }

  @Test
  public void testRefreshNodesResourcesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededRefreshNodesResourcesRetrieved();
    goodSubCluster.getRefreshNodesResourcesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededRefreshNodesResourcesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededRefreshNodesResourcesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRefreshNodesResourcesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededRefreshNodesResourcesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededRefreshNodesResourcesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testCheckForDecommissioningNodesFailedRetrieved() {
    long totalBadBefore = metrics.getCheckForDecommissioningNodesFailedRetrieved();
    badSubCluster.getCheckForDecommissioningNodesFailed();
    assertEquals(totalBadBefore + 1,
        metrics.getCheckForDecommissioningNodesFailedRetrieved());
  }

  @Test
  public void testCheckForDecommissioningNodesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededCheckForDecommissioningNodesRetrieved();
    goodSubCluster.getCheckForDecommissioningNodesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededCheckForDecommissioningNodesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededCheckForDecommissioningNodesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getCheckForDecommissioningNodesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededCheckForDecommissioningNodesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededCheckForDecommissioningNodesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testRefreshClusterMaxPriorityFailedRetrieved() {
    long totalBadBefore = metrics.getRefreshClusterMaxPriorityFailedRetrieved();
    badSubCluster.getRefreshClusterMaxPriorityFailed();
    assertEquals(totalBadBefore + 1, metrics.getRefreshClusterMaxPriorityFailedRetrieved());
  }

  @Test
  public void testRefreshClusterMaxPriorityRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededRefreshClusterMaxPriorityRetrieved();
    goodSubCluster.getRefreshClusterMaxPriorityRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededRefreshClusterMaxPriorityRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededRefreshClusterMaxPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getRefreshClusterMaxPriorityRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededRefreshClusterMaxPriorityRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededRefreshClusterMaxPriorityRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetMapAttributesToNodesFailedRetrieved() {
    long totalBadBefore = metrics.getMapAttributesToNodesFailedRetrieved();
    badSubCluster.getMapAttributesToNodesFailed();
    assertEquals(totalBadBefore + 1, metrics.getMapAttributesToNodesFailedRetrieved());
  }

  @Test
  public void testGetMapAttributesToNodesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededMapAttributesToNodesRetrieved();
    goodSubCluster.getMapAttributesToNodesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededMapAttributesToNodesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededMapAttributesToNodesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getMapAttributesToNodesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededMapAttributesToNodesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededMapAttributesToNodesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetGroupsForUserFailedRetrieved() {
    long totalBadBefore = metrics.getGroupsForUserFailedRetrieved();
    badSubCluster.getGroupsForUserFailed();
    assertEquals(totalBadBefore + 1, metrics.getGroupsForUserFailedRetrieved());
  }

  @Test
  public void testGetGroupsForUserRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetGroupsForUsersRetrieved();
    goodSubCluster.getGroupsForUsersRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetGroupsForUsersRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetGroupsForUsersRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getGroupsForUsersRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetGroupsForUsersRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetGroupsForUsersRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testSaveFederationQueuePolicyFailedRetrieved() {
    long totalBadBefore = metrics.getSaveFederationQueuePolicyFailedRetrieved();
    badSubCluster.getSaveFederationQueuePolicyFailedRetrieved();
    assertEquals(totalBadBefore + 1, metrics.getSaveFederationQueuePolicyFailedRetrieved());
  }

  @Test
  public void testSaveFederationQueuePolicyRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededSaveFederationQueuePolicyRetrieved();
    goodSubCluster.getSaveFederationQueuePolicyRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededSaveFederationQueuePolicyRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededSaveFederationQueuePolicyRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getSaveFederationQueuePolicyRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededSaveFederationQueuePolicyRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededSaveFederationQueuePolicyRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetBatchSaveFederationQueuePoliciesFailedRetrieved() {
    long totalBadBefore = metrics.getBatchSaveFederationQueuePoliciesFailedRetrieved();
    badSubCluster.getBatchSaveFederationQueuePoliciesFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getBatchSaveFederationQueuePoliciesFailedRetrieved());
  }

  @Test
  public void testGetBatchSaveFederationQueuePoliciesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededBatchSaveFederationQueuePoliciesRetrieved();
    goodSubCluster.getBatchSaveFederationQueuePoliciesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededBatchSaveFederationQueuePoliciesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededBatchSaveFederationQueuePoliciesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.getBatchSaveFederationQueuePoliciesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededBatchSaveFederationQueuePoliciesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededBatchSaveFederationQueuePoliciesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testListFederationQueuePoliciesFailedRetrieved() {
    long totalBadBefore = metrics.getListFederationQueuePoliciesFailedRetrieved();
    badSubCluster.getListFederationQueuePoliciesFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getListFederationQueuePoliciesFailedRetrieved());
  }

  @Test
  public void testListFederationQueuePoliciesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededListFederationQueuePoliciesFailedRetrieved();
    goodSubCluster.getListFederationQueuePoliciesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededListFederationQueuePoliciesFailedRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededListFederationQueuePoliciesRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getListFederationQueuePoliciesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededListFederationQueuePoliciesFailedRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededListFederationQueuePoliciesRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testGetFederationSubClustersFailedRetrieved() {
    long totalBadBefore = metrics.getFederationSubClustersFailedRetrieved();
    badSubCluster.getFederationSubClustersFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getFederationSubClustersFailedRetrieved());
  }

  @Test
  public void testGetFederationSubClustersRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededGetFederationSubClustersRetrieved();
    goodSubCluster.getFederationSubClustersRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededGetFederationSubClustersRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededGetFederationSubClustersRetrieved(), ASSERT_DOUBLE_DELTA);
    goodSubCluster.getFederationSubClustersRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededGetFederationSubClustersRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededGetFederationSubClustersRetrieved(), ASSERT_DOUBLE_DELTA);
  }

  @Test
  public void testDeleteFederationPoliciesByQueuesFailedRetrieved() {
    long totalBadBefore = metrics.getDeleteFederationPoliciesByQueuesRetrieved();
    badSubCluster.getDeleteFederationPoliciesByQueuesFailedRetrieved();
    assertEquals(totalBadBefore + 1,
        metrics.getDeleteFederationPoliciesByQueuesRetrieved());
  }

  @Test
  public void testDeleteFederationPoliciesByQueuesRetrieved() {
    long totalGoodBefore = metrics.getNumSucceededDeleteFederationPoliciesByQueuesRetrieved();
    goodSubCluster.deleteFederationPoliciesByQueuesRetrieved(150);
    assertEquals(totalGoodBefore + 1,
        metrics.getNumSucceededDeleteFederationPoliciesByQueuesRetrieved());
    assertEquals(150,
        metrics.getLatencySucceededDeleteFederationPoliciesByQueuesRetrieved(),
        ASSERT_DOUBLE_DELTA);
    goodSubCluster.deleteFederationPoliciesByQueuesRetrieved(300);
    assertEquals(totalGoodBefore + 2,
        metrics.getNumSucceededDeleteFederationPoliciesByQueuesRetrieved());
    assertEquals(225,
        metrics.getLatencySucceededDeleteFederationPoliciesByQueuesRetrieved(),
        ASSERT_DOUBLE_DELTA);
  }
}