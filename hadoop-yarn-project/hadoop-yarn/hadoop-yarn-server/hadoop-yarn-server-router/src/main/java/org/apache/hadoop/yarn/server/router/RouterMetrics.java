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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * This class is for maintaining the various Router Federation Interceptor
 * activity statistics and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about = "Metrics for Router Federation Interceptor", context = "fedr")
public final class RouterMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("RouterMetrics", "Router Federation Interceptor");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // Metrics for operation failed
  @Metric("# of applications failed to be submitted")
  private MutableGaugeInt numAppsFailedSubmitted;
  @Metric("# of applications failed to be created")
  private MutableGaugeInt numAppsFailedCreated;
  @Metric("# of applications failed to be killed")
  private MutableGaugeInt numAppsFailedKilled;
  @Metric("# of application reports failed to be retrieved")
  private MutableGaugeInt numAppsFailedRetrieved;
  @Metric("# of multiple applications reports failed to be retrieved")
  private MutableGaugeInt numMultipleAppsFailedRetrieved;
  @Metric("# of getApplicationAttempts failed to be retrieved")
  private MutableGaugeInt numAppAttemptsFailedRetrieved;
  @Metric("# of getClusterMetrics failed to be retrieved")
  private MutableGaugeInt numGetClusterMetricsFailedRetrieved;
  @Metric("# of getClusterNodes failed to be retrieved")
  private MutableGaugeInt numGetClusterNodesFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetNodeToLabelsFailedRetrieved;
  @Metric("# of getNodeToLabels failed to be retrieved")
  private MutableGaugeInt numGetLabelsToNodesFailedRetrieved;
  @Metric("# of getClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numGetClusterNodeLabelsFailedRetrieved;
  @Metric("# of getApplicationAttemptReports failed to be retrieved")
  private MutableGaugeInt numAppAttemptReportFailedRetrieved;
  @Metric("# of getQueueUserAcls failed to be retrieved")
  private MutableGaugeInt numGetQueueUserAclsFailedRetrieved;
  @Metric("# of getContainerReport failed to be retrieved")
  private MutableGaugeInt numGetContainerReportFailedRetrieved;
  @Metric("# of getContainers failed to be retrieved")
  private MutableGaugeInt numGetContainersFailedRetrieved;
  @Metric("# of listReservations failed to be retrieved")
  private MutableGaugeInt numListReservationsFailedRetrieved;
  @Metric("# of getResourceTypeInfo failed to be retrieved")
  private MutableGaugeInt numGetResourceTypeInfo;
  @Metric("# of failApplicationAttempt failed to be retrieved")
  private MutableGaugeInt numFailAppAttemptFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppPriorityFailedRetrieved;
  @Metric("# of updateApplicationPriority failed to be retrieved")
  private MutableGaugeInt numUpdateAppTimeoutsFailedRetrieved;
  @Metric("# of signalToContainer failed to be retrieved")
  private MutableGaugeInt numSignalToContainerFailedRetrieved;
  @Metric("# of getQueueInfo failed to be retrieved")
  private MutableGaugeInt numGetQueueInfoFailedRetrieved;
  @Metric("# of moveApplicationAcrossQueues failed to be retrieved")
  private MutableGaugeInt numMoveApplicationAcrossQueuesFailedRetrieved;
  @Metric("# of getResourceProfiles failed to be retrieved")
  private MutableGaugeInt numGetResourceProfilesFailedRetrieved;
  @Metric("# of getResourceProfile failed to be retrieved")
  private MutableGaugeInt numGetResourceProfileFailedRetrieved;
  @Metric("# of getAttributesToNodes failed to be retrieved")
  private MutableGaugeInt numGetAttributesToNodesFailedRetrieved;
  @Metric("# of getClusterNodeAttributes failed to be retrieved")
  private MutableGaugeInt numGetClusterNodeAttributesFailedRetrieved;
  @Metric("# of getNodesToAttributes failed to be retrieved")
  private MutableGaugeInt numGetNodesToAttributesFailedRetrieved;
  @Metric("# of getNewReservation failed to be retrieved")
  private MutableGaugeInt numGetNewReservationFailedRetrieved;
  @Metric("# of submitReservation failed to be retrieved")
  private MutableGaugeInt numSubmitReservationFailedRetrieved;
  @Metric("# of submitReservation failed to be retrieved")
  private MutableGaugeInt numUpdateReservationFailedRetrieved;
  @Metric("# of deleteReservation failed to be retrieved")
  private MutableGaugeInt numDeleteReservationFailedRetrieved;
  @Metric("# of listReservation failed to be retrieved")
  private MutableGaugeInt numListReservationFailedRetrieved;
  @Metric("# of getAppActivities failed to be retrieved")
  private MutableGaugeInt numGetAppActivitiesFailedRetrieved;
  @Metric("# of getAppStatistics failed to be retrieved")
  private MutableGaugeInt numGetAppStatisticsFailedRetrieved;
  @Metric("# of getAppPriority failed to be retrieved")
  private MutableGaugeInt numGetAppPriorityFailedRetrieved;
  @Metric("# of getAppQueue failed to be retrieved")
  private MutableGaugeInt numGetAppQueueFailedRetrieved;
  @Metric("# of updateAppQueue failed to be retrieved")
  private MutableGaugeInt numUpdateAppQueueFailedRetrieved;
  @Metric("# of getAppTimeout failed to be retrieved")
  private MutableGaugeInt numGetAppTimeoutFailedRetrieved;
  @Metric("# of getAppTimeouts failed to be retrieved")
  private MutableGaugeInt numGetAppTimeoutsFailedRetrieved;
  @Metric("# of refreshQueues failed to be retrieved")
  private MutableGaugeInt numRefreshQueuesFailedRetrieved;
  @Metric("# of getRMNodeLabels failed to be retrieved")
  private MutableGaugeInt numGetRMNodeLabelsFailedRetrieved;
  @Metric("# of checkUserAccessToQueue failed to be retrieved")
  private MutableGaugeInt numCheckUserAccessToQueueFailedRetrieved;
  @Metric("# of refreshNodes failed to be retrieved")
  private MutableGaugeInt numRefreshNodesFailedRetrieved;
  @Metric("# of getDelegationToken failed to be retrieved")
  private MutableGaugeInt numGetDelegationTokenFailedRetrieved;
  @Metric("# of renewDelegationToken failed to be retrieved")
  private MutableGaugeInt numRenewDelegationTokenFailedRetrieved;
  @Metric("# of renewDelegationToken failed to be retrieved")
  private MutableGaugeInt numCancelDelegationTokenFailedRetrieved;
  @Metric("# of dumpSchedulerLogs failed to be retrieved")
  private MutableGaugeInt numDumpSchedulerLogsFailedRetrieved;
  @Metric("# of getActivities failed to be retrieved")
  private MutableGaugeInt numGetActivitiesFailedRetrieved;
  @Metric("# of getBulkActivities failed to be retrieved")
  private MutableGaugeInt numGetBulkActivitiesFailedRetrieved;
  @Metric("# of getSchedulerInfo failed to be retrieved")
  private MutableGaugeInt numGetSchedulerInfoFailedRetrieved;
  @Metric("# of refreshSuperUserGroupsConfiguration failed to be retrieved")
  private MutableGaugeInt numRefreshSuperUserGroupsConfigurationFailedRetrieved;
  @Metric("# of refreshUserToGroupsMappings failed to be retrieved")
  private MutableGaugeInt numRefreshUserToGroupsMappingsFailedRetrieved;
  @Metric("# of deregisterSubCluster failed to be retrieved")
  private MutableGaugeInt numDeregisterSubClusterFailedRetrieved;
  @Metric("# of saveFederationQueuePolicy failed to be retrieved")
  private MutableGaugeInt numSaveFederationQueuePolicyFailedRetrieved;
  @Metric("# of batchSaveFederationQueuePolicies failed to be retrieved")
  private MutableGaugeInt numBatchSaveFederationQueuePoliciesFailedRetrieved;
  @Metric("# of refreshAdminAcls failed to be retrieved")
  private MutableGaugeInt numRefreshAdminAclsFailedRetrieved;
  @Metric("# of refreshServiceAcls failed to be retrieved")
  private MutableGaugeInt numRefreshServiceAclsFailedRetrieved;
  @Metric("# of replaceLabelsOnNodes failed to be retrieved")
  private MutableGaugeInt numReplaceLabelsOnNodesFailedRetrieved;
  @Metric("# of replaceLabelsOnNode failed to be retrieved")
  private MutableGaugeInt numReplaceLabelsOnNodeFailedRetrieved;
  @Metric("# of addToClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numAddToClusterNodeLabelsFailedRetrieved;
  @Metric("# of removeFromClusterNodeLabels failed to be retrieved")
  private MutableGaugeInt numRemoveFromClusterNodeLabelsFailedRetrieved;
  @Metric("# of numUpdateSchedulerConfiguration failed to be retrieved")
  private MutableGaugeInt numUpdateSchedulerConfigurationFailedRetrieved;
  @Metric("# of numGetSchedulerConfiguration failed to be retrieved")
  private MutableGaugeInt numGetSchedulerConfigurationFailedRetrieved;
  @Metric("# of getClusterInfo failed to be retrieved")
  private MutableGaugeInt numGetClusterInfoFailedRetrieved;
  @Metric("# of getClusterUserInfo failed to be retrieved")
  private MutableGaugeInt numGetClusterUserInfoFailedRetrieved;
  @Metric("# of updateNodeResource failed to be retrieved")
  private MutableGaugeInt numUpdateNodeResourceFailedRetrieved;
  @Metric("# of refreshNodesResources failed to be retrieved")
  private MutableGaugeInt numRefreshNodesResourcesFailedRetrieved;
  @Metric("# of checkForDecommissioningNodes failed to be retrieved")
  private MutableGaugeInt numCheckForDecommissioningNodesFailedRetrieved;
  @Metric("# of refreshClusterMaxPriority failed to be retrieved")
  private MutableGaugeInt numRefreshClusterMaxPriorityFailedRetrieved;
  @Metric("# of mapAttributesToNodes failed to be retrieved")
  private MutableGaugeInt numMapAttributesToNodesFailedRetrieved;
  @Metric("# of getGroupsForUser failed to be retrieved")
  private MutableGaugeInt numGetGroupsForUserFailedRetrieved;

  // Aggregate metrics are shared, and don't have to be looked up per call
  @Metric("Total number of successful Submitted apps and latency(ms)")
  private MutableRate totalSucceededAppsSubmitted;
  @Metric("Total number of successful Killed apps and latency(ms)")
  private MutableRate totalSucceededAppsKilled;
  @Metric("Total number of successful Created apps and latency(ms)")
  private MutableRate totalSucceededAppsCreated;
  @Metric("Total number of successful Retrieved app reports and latency(ms)")
  private MutableRate totalSucceededAppsRetrieved;
  @Metric("Total number of successful Retrieved multiple apps reports and latency(ms)")
  private MutableRate totalSucceededMultipleAppsRetrieved;
  @Metric("Total number of successful Retrieved appAttempt reports and latency(ms)")
  private MutableRate totalSucceededAppAttemptsRetrieved;
  @Metric("Total number of successful Retrieved getClusterMetrics and latency(ms)")
  private MutableRate totalSucceededGetClusterMetricsRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodes and latency(ms)")
  private MutableRate totalSucceededGetClusterNodesRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetNodeToLabelsRetrieved;
  @Metric("Total number of successful Retrieved getNodeToLabels and latency(ms)")
  private MutableRate totalSucceededGetLabelsToNodesRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodeLabels and latency(ms)")
  private MutableRate totalSucceededGetClusterNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved getApplicationAttemptReport and latency(ms)")
  private MutableRate totalSucceededAppAttemptReportRetrieved;
  @Metric("Total number of successful Retrieved getQueueUserAcls and latency(ms)")
  private MutableRate totalSucceededGetQueueUserAclsRetrieved;
  @Metric("Total number of successful Retrieved getContainerReport and latency(ms)")
  private MutableRate totalSucceededGetContainerReportRetrieved;
  @Metric("Total number of successful Retrieved getContainers and latency(ms)")
  private MutableRate totalSucceededGetContainersRetrieved;
  @Metric("Total number of successful Retrieved listReservations and latency(ms)")
  private MutableRate totalSucceededListReservationsRetrieved;
  @Metric("Total number of successful Retrieved getResourceTypeInfo and latency(ms)")
  private MutableRate totalSucceededGetResourceTypeInfoRetrieved;
  @Metric("Total number of successful Retrieved failApplicationAttempt and latency(ms)")
  private MutableRate totalSucceededFailAppAttemptRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationPriority and latency(ms)")
  private MutableRate totalSucceededUpdateAppPriorityRetrieved;
  @Metric("Total number of successful Retrieved updateApplicationTimeouts and latency(ms)")
  private MutableRate totalSucceededUpdateAppTimeoutsRetrieved;
  @Metric("Total number of successful Retrieved signalToContainer and latency(ms)")
  private MutableRate totalSucceededSignalToContainerRetrieved;
  @Metric("Total number of successful Retrieved getQueueInfo and latency(ms)")
  private MutableRate totalSucceededGetQueueInfoRetrieved;
  @Metric("Total number of successful Retrieved moveApplicationAcrossQueues and latency(ms)")
  private MutableRate totalSucceededMoveApplicationAcrossQueuesRetrieved;
  @Metric("Total number of successful Retrieved getResourceProfiles and latency(ms)")
  private MutableRate totalSucceededGetResourceProfilesRetrieved;
  @Metric("Total number of successful Retrieved getResourceProfile and latency(ms)")
  private MutableRate totalSucceededGetResourceProfileRetrieved;
  @Metric("Total number of successful Retrieved getAttributesToNodes and latency(ms)")
  private MutableRate totalSucceededGetAttributesToNodesRetrieved;
  @Metric("Total number of successful Retrieved getClusterNodeAttributes and latency(ms)")
  private MutableRate totalSucceededGetClusterNodeAttributesRetrieved;
  @Metric("Total number of successful Retrieved getNodesToAttributes and latency(ms)")
  private MutableRate totalSucceededGetNodesToAttributesRetrieved;
  @Metric("Total number of successful Retrieved GetNewReservation and latency(ms)")
  private MutableRate totalSucceededGetNewReservationRetrieved;
  @Metric("Total number of successful Retrieved SubmitReservation and latency(ms)")
  private MutableRate totalSucceededSubmitReservationRetrieved;
  @Metric("Total number of successful Retrieved UpdateReservation and latency(ms)")
  private MutableRate totalSucceededUpdateReservationRetrieved;
  @Metric("Total number of successful Retrieved DeleteReservation and latency(ms)")
  private MutableRate totalSucceededDeleteReservationRetrieved;
  @Metric("Total number of successful Retrieved ListReservation and latency(ms)")
  private MutableRate totalSucceededListReservationRetrieved;
  @Metric("Total number of successful Retrieved GetAppActivities and latency(ms)")
  private MutableRate totalSucceededGetAppActivitiesRetrieved;
  @Metric("Total number of successful Retrieved GetAppStatistics and latency(ms)")
  private MutableRate totalSucceededGetAppStatisticsRetrieved;
  @Metric("Total number of successful Retrieved GetAppPriority and latency(ms)")
  private MutableRate totalSucceededGetAppPriorityRetrieved;
  @Metric("Total number of successful Retrieved GetAppQueue and latency(ms)")
  private MutableRate totalSucceededGetAppQueueRetrieved;
  @Metric("Total number of successful Retrieved UpdateAppQueue and latency(ms)")
  private MutableRate totalSucceededUpdateAppQueueRetrieved;
  @Metric("Total number of successful Retrieved GetAppTimeout and latency(ms)")
  private MutableRate totalSucceededGetAppTimeoutRetrieved;
  @Metric("Total number of successful Retrieved GetAppTimeouts and latency(ms)")
  private MutableRate totalSucceededGetAppTimeoutsRetrieved;
  @Metric("Total number of successful Retrieved RefreshQueues and latency(ms)")
  private MutableRate totalSucceededRefreshQueuesRetrieved;
  @Metric("Total number of successful Retrieved GetRMNodeLabels and latency(ms)")
  private MutableRate totalSucceededGetRMNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved CheckUserAccessToQueue and latency(ms)")
  private MutableRate totalSucceededCheckUserAccessToQueueRetrieved;
  @Metric("Total number of successful Retrieved RefreshNodes and latency(ms)")
  private MutableRate totalSucceededRefreshNodesRetrieved;
  @Metric("Total number of successful Retrieved GetDelegationToken and latency(ms)")
  private MutableRate totalSucceededGetDelegationTokenRetrieved;
  @Metric("Total number of successful Retrieved RenewDelegationToken and latency(ms)")
  private MutableRate totalSucceededRenewDelegationTokenRetrieved;
  @Metric("Total number of successful Retrieved CancelDelegationToken and latency(ms)")
  private MutableRate totalSucceededCancelDelegationTokenRetrieved;
  @Metric("Total number of successful Retrieved DumpSchedulerLogs and latency(ms)")
  private MutableRate totalSucceededDumpSchedulerLogsRetrieved;
  @Metric("Total number of successful Retrieved GetActivities and latency(ms)")
  private MutableRate totalSucceededGetActivitiesRetrieved;
  @Metric("Total number of successful Retrieved GetBulkActivities and latency(ms)")
  private MutableRate totalSucceededGetBulkActivitiesRetrieved;
  @Metric("Total number of successful Retrieved RefreshSuperUserGroupsConfig and latency(ms)")
  private MutableRate totalSucceededRefreshSuperUserGroupsConfigurationRetrieved;
  @Metric("Total number of successful Retrieved RefreshUserToGroupsMappings and latency(ms)")
  private MutableRate totalSucceededRefreshUserToGroupsMappingsRetrieved;
  @Metric("Total number of successful Retrieved ReplaceLabelsOnNodes and latency(ms)")
  private MutableRate totalSucceededReplaceLabelsOnNodesRetrieved;
  @Metric("Total number of successful Retrieved ReplaceLabelsOnNode and latency(ms)")
  private MutableRate totalSucceededReplaceLabelsOnNodeRetrieved;
  @Metric("Total number of successful Retrieved GetSchedulerInfo and latency(ms)")
  private MutableRate totalSucceededGetSchedulerInfoRetrieved;
  @Metric("Total number of successful Retrieved DeregisterSubCluster and latency(ms)")
  private MutableRate totalSucceededDeregisterSubClusterRetrieved;
  @Metric("Total number of successful Retrieved SaveFederationQueuePolicy and latency(ms)")
  private MutableRate totalSucceededSaveFederationQueuePolicyRetrieved;
  @Metric("Total number of successful Retrieved BatchSaveFederationQueuePolicies and latency(ms)")
  private MutableRate totalSucceededBatchSaveFederationQueuePoliciesRetrieved;
  @Metric("Total number of successful Retrieved RefreshAdminAcls and latency(ms)")
  private MutableRate totalSucceededRefreshAdminAclsRetrieved;
  @Metric("Total number of successful Retrieved RefreshServiceAcls and latency(ms)")
  private MutableRate totalSucceededRefreshServiceAclsRetrieved;
  @Metric("Total number of successful Retrieved AddToClusterNodeLabels and latency(ms)")
  private MutableRate totalSucceededAddToClusterNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved RemoveFromClusterNodeLabels and latency(ms)")
  private MutableRate totalSucceededRemoveFromClusterNodeLabelsRetrieved;
  @Metric("Total number of successful Retrieved updateSchedulerConfiguration and latency(ms)")
  private MutableRate totalSucceededUpdateSchedulerConfigurationRetrieved;
  @Metric("Total number of successful Retrieved getSchedulerConfiguration and latency(ms)")
  private MutableRate totalSucceededGetSchedulerConfigurationRetrieved;
  @Metric("Total number of successful Retrieved GetClusterInfoRetrieved and latency(ms)")
  private MutableRate totalSucceededGetClusterInfoRetrieved;
  @Metric("Total number of successful Retrieved GetClusterUserInfoRetrieved and latency(ms)")
  private MutableRate totalSucceededGetClusterUserInfoRetrieved;
  @Metric("Total number of successful Retrieved UpdateNodeResource and latency(ms)")
  private MutableRate totalSucceededUpdateNodeResourceRetrieved;
  @Metric("Total number of successful Retrieved RefreshNodesResources and latency(ms)")
  private MutableRate totalSucceededRefreshNodesResourcesRetrieved;
  @Metric("Total number of successful Retrieved CheckForDecommissioningNodes and latency(ms)")
  private MutableRate totalSucceededCheckForDecommissioningNodesRetrieved;
  @Metric("Total number of successful Retrieved RefreshClusterMaxPriority and latency(ms)")
  private MutableRate totalSucceededRefreshClusterMaxPriorityRetrieved;
  @Metric("Total number of successful Retrieved MapAttributesToNodes and latency(ms)")
  private MutableRate totalSucceededMapAttributesToNodesRetrieved;
  @Metric("Total number of successful Retrieved GetGroupsForUser and latency(ms)")
  private MutableRate totalSucceededGetGroupsForUsersRetrieved;

  /**
   * Provide quantile counters for all latencies.
   */
  private MutableQuantiles submitApplicationLatency;
  private MutableQuantiles getNewApplicationLatency;
  private MutableQuantiles killApplicationLatency;
  private MutableQuantiles getApplicationReportLatency;
  private MutableQuantiles getApplicationsReportLatency;
  private MutableQuantiles getApplicationAttemptReportLatency;
  private MutableQuantiles getClusterMetricsLatency;
  private MutableQuantiles getClusterNodesLatency;
  private MutableQuantiles getNodeToLabelsLatency;
  private MutableQuantiles getLabelToNodesLatency;
  private MutableQuantiles getClusterNodeLabelsLatency;
  private MutableQuantiles getApplicationAttemptsLatency;
  private MutableQuantiles getQueueUserAclsLatency;
  private MutableQuantiles getContainerReportLatency;
  private MutableQuantiles getContainerLatency;
  private MutableQuantiles listReservationsLatency;
  private MutableQuantiles listResourceTypeInfoLatency;
  private MutableQuantiles failAppAttemptLatency;
  private MutableQuantiles updateAppPriorityLatency;
  private MutableQuantiles updateAppTimeoutsLatency;
  private MutableQuantiles signalToContainerLatency;
  private MutableQuantiles getQueueInfoLatency;
  private MutableQuantiles moveApplicationAcrossQueuesLatency;
  private MutableQuantiles getResourceProfilesLatency;
  private MutableQuantiles getResourceProfileLatency;
  private MutableQuantiles getAttributesToNodesLatency;
  private MutableQuantiles getClusterNodeAttributesLatency;
  private MutableQuantiles getNodesToAttributesLatency;
  private MutableQuantiles getNewReservationLatency;
  private MutableQuantiles submitReservationLatency;
  private MutableQuantiles updateReservationLatency;
  private MutableQuantiles deleteReservationLatency;
  private MutableQuantiles listReservationLatency;
  private MutableQuantiles getAppActivitiesLatency;
  private MutableQuantiles getAppStatisticsLatency;
  private MutableQuantiles getAppPriorityLatency;
  private MutableQuantiles getAppQueueLatency;
  private MutableQuantiles getUpdateQueueLatency;
  private MutableQuantiles getAppTimeoutLatency;
  private MutableQuantiles getAppTimeoutsLatency;
  private MutableQuantiles refreshQueuesLatency;
  private MutableQuantiles getRMNodeLabelsLatency;
  private MutableQuantiles checkUserAccessToQueueLatency;
  private MutableQuantiles refreshNodesLatency;
  private MutableQuantiles getDelegationTokenLatency;
  private MutableQuantiles renewDelegationTokenLatency;
  private MutableQuantiles cancelDelegationTokenLatency;
  private MutableQuantiles dumpSchedulerLogsLatency;
  private MutableQuantiles getActivitiesLatency;
  private MutableQuantiles getBulkActivitiesLatency;
  private MutableQuantiles getSchedulerInfoRetrievedLatency;
  private MutableQuantiles refreshSuperUserGroupsConfLatency;
  private MutableQuantiles refreshUserToGroupsMappingsLatency;
  private MutableQuantiles refreshDeregisterSubClusterLatency;
  private MutableQuantiles saveFederationQueuePolicyLatency;
  private MutableQuantiles batchSaveFederationQueuePoliciesLatency;
  private MutableQuantiles refreshAdminAclsLatency;
  private MutableQuantiles refreshServiceAclsLatency;
  private MutableQuantiles replaceLabelsOnNodesLatency;
  private MutableQuantiles replaceLabelsOnNodeLatency;
  private MutableQuantiles addToClusterNodeLabelsLatency;
  private MutableQuantiles removeFromClusterNodeLabelsLatency;
  private MutableQuantiles updateSchedulerConfigLatency;
  private MutableQuantiles getSchedulerConfigurationLatency;
  private MutableQuantiles getClusterInfoLatency;
  private MutableQuantiles getClusterUserInfoLatency;
  private MutableQuantiles updateNodeResourceLatency;
  private MutableQuantiles refreshNodesResourcesLatency;
  private MutableQuantiles checkForDecommissioningNodesLatency;
  private MutableQuantiles refreshClusterMaxPriorityLatency;
  private MutableQuantiles mapAttributesToNodesLatency;
  private MutableQuantiles getGroupsForUserLatency;

  private static volatile RouterMetrics instance = null;
  private static MetricsRegistry registry;

  @SuppressWarnings("checkstyle:MethodLength")
  private RouterMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "Router");
    getNewApplicationLatency = registry.newQuantiles("getNewApplicationLatency",
        "latency of get new application", "ops", "latency", 10);
    submitApplicationLatency = registry.newQuantiles("submitApplicationLatency",
        "latency of submit application", "ops", "latency", 10);
    killApplicationLatency = registry.newQuantiles("killApplicationLatency",
        "latency of kill application", "ops", "latency", 10);
    getApplicationReportLatency =
        registry.newQuantiles("getApplicationReportLatency",
            "latency of get application report", "ops", "latency", 10);
    getApplicationsReportLatency =
        registry.newQuantiles("getApplicationsReportLatency",
            "latency of get applications report", "ops", "latency", 10);
    getApplicationAttemptReportLatency =
        registry.newQuantiles("getApplicationAttemptReportLatency",
                    "latency of get applicationattempt " +
                            "report", "ops", "latency", 10);
    getClusterMetricsLatency =
        registry.newQuantiles("getClusterMetricsLatency",
            "latency of get cluster metrics", "ops", "latency", 10);

    getClusterNodesLatency =
        registry.newQuantiles("getClusterNodesLatency",
            "latency of get cluster nodes", "ops", "latency", 10);

    getNodeToLabelsLatency =
        registry.newQuantiles("getNodeToLabelsLatency",
            "latency of get node labels", "ops", "latency", 10);

    getLabelToNodesLatency =
        registry.newQuantiles("getLabelToNodesLatency",
            "latency of get label nodes", "ops", "latency", 10);

    getClusterNodeLabelsLatency =
        registry.newQuantiles("getClusterNodeLabelsLatency",
            "latency of get cluster node labels", "ops", "latency", 10);

    getApplicationAttemptsLatency =
        registry.newQuantiles("getApplicationAttemptsLatency",
            "latency of get application attempts", "ops", "latency", 10);

    getQueueUserAclsLatency =
        registry.newQuantiles("getQueueUserAclsLatency",
            "latency of get queue user acls", "ops", "latency", 10);

    getContainerReportLatency =
        registry.newQuantiles("getContainerReportLatency",
            "latency of get container report", "ops", "latency", 10);

    getContainerLatency =
        registry.newQuantiles("getContainerLatency",
            "latency of get container", "ops", "latency", 10);

    listReservationsLatency =
        registry.newQuantiles("listReservationsLatency",
            "latency of list reservations", "ops", "latency", 10);

    listResourceTypeInfoLatency =
        registry.newQuantiles("getResourceTypeInfoLatency",
            "latency of get resource type info", "ops", "latency", 10);

    failAppAttemptLatency =
        registry.newQuantiles("failApplicationAttemptLatency",
            "latency of fail application attempt", "ops", "latency", 10);

    updateAppPriorityLatency =
        registry.newQuantiles("updateApplicationPriorityLatency",
            "latency of update application priority", "ops", "latency", 10);

    updateAppTimeoutsLatency =
        registry.newQuantiles("updateApplicationTimeoutsLatency",
            "latency of update application timeouts", "ops", "latency", 10);

    signalToContainerLatency =
        registry.newQuantiles("signalToContainerLatency",
            "latency of signal to container timeouts", "ops", "latency", 10);

    getQueueInfoLatency =
        registry.newQuantiles("getQueueInfoLatency",
            "latency of get queue info timeouts", "ops", "latency", 10);

    moveApplicationAcrossQueuesLatency =
        registry.newQuantiles("moveApplicationAcrossQueuesLatency",
            "latency of move application across queues timeouts", "ops", "latency", 10);

    getResourceProfilesLatency =
        registry.newQuantiles("getResourceProfilesLatency",
            "latency of get resource profiles timeouts", "ops", "latency", 10);

    getResourceProfileLatency =
        registry.newQuantiles("getResourceProfileLatency",
            "latency of get resource profile timeouts", "ops", "latency", 10);

    getAttributesToNodesLatency =
        registry.newQuantiles("getAttributesToNodesLatency",
            "latency of get attributes to nodes timeouts", "ops", "latency", 10);

    getClusterNodeAttributesLatency =
        registry.newQuantiles("getClusterNodeAttributesLatency",
            "latency of get cluster node attributes timeouts", "ops", "latency", 10);

    getNodesToAttributesLatency =
        registry.newQuantiles("getNodesToAttributesLatency",
            "latency of get nodes to attributes timeouts", "ops", "latency", 10);

    getNewReservationLatency =
        registry.newQuantiles("getNewReservationLatency",
            "latency of get new reservation timeouts", "ops", "latency", 10);

    submitReservationLatency =
        registry.newQuantiles("submitReservationLatency",
            "latency of submit reservation timeouts", "ops", "latency", 10);

    updateReservationLatency =
        registry.newQuantiles("updateReservationLatency",
            "latency of update reservation timeouts", "ops", "latency", 10);

    deleteReservationLatency =
        registry.newQuantiles("deleteReservationLatency",
            "latency of delete reservation timeouts", "ops", "latency", 10);

    listReservationLatency =
        registry.newQuantiles("listReservationLatency",
            "latency of list reservation timeouts", "ops", "latency", 10);

    getAppActivitiesLatency = registry.newQuantiles("getAppActivitiesLatency",
         "latency of get app activities timeouts", "ops", "latency", 10);

    getAppStatisticsLatency = registry.newQuantiles("getAppStatisticsLatency",
         "latency of get app statistics timeouts", "ops", "latency", 10);

    getAppPriorityLatency = registry.newQuantiles("getAppPriorityLatency",
         "latency of get app priority timeouts", "ops", "latency", 10);

    getAppQueueLatency = registry.newQuantiles("getAppQueueLatency",
         "latency of get app queue timeouts", "ops", "latency", 10);

    getUpdateQueueLatency = registry.newQuantiles("getUpdateQueueLatency",
        "latency of update app queue timeouts", "ops", "latency", 10);

    getAppTimeoutLatency = registry.newQuantiles("getAppTimeoutLatency",
        "latency of get apptimeout timeouts", "ops", "latency", 10);

    getAppTimeoutsLatency = registry.newQuantiles("getAppTimeoutsLatency",
         "latency of get apptimeouts timeouts", "ops", "latency", 10);

    refreshQueuesLatency = registry.newQuantiles("refreshQueuesLatency",
         "latency of get refresh queues timeouts", "ops", "latency", 10);

    getRMNodeLabelsLatency = registry.newQuantiles("getRMNodeLabelsLatency",
        "latency of get rmnodelabels timeouts", "ops", "latency", 10);

    checkUserAccessToQueueLatency = registry.newQuantiles("checkUserAccessToQueueLatency",
        "latency of get apptimeouts timeouts", "ops", "latency", 10);

    refreshNodesLatency = registry.newQuantiles("refreshNodesLatency",
        "latency of get refresh nodes timeouts", "ops", "latency", 10);

    getDelegationTokenLatency = registry.newQuantiles("getDelegationTokenLatency",
        "latency of get delegation token timeouts", "ops", "latency", 10);

    renewDelegationTokenLatency = registry.newQuantiles("renewDelegationTokenLatency",
       "latency of renew delegation token timeouts", "ops", "latency", 10);

    cancelDelegationTokenLatency = registry.newQuantiles("cancelDelegationTokenLatency",
        "latency of cancel delegation token timeouts", "ops", "latency", 10);

    dumpSchedulerLogsLatency = registry.newQuantiles("dumpSchedulerLogsLatency",
        "latency of dump scheduler logs timeouts", "ops", "latency", 10);

    getActivitiesLatency = registry.newQuantiles("getActivitiesLatency",
        "latency of get activities timeouts", "ops", "latency", 10);

    getBulkActivitiesLatency = registry.newQuantiles("getBulkActivitiesLatency",
         "latency of get bulk activities timeouts", "ops", "latency", 10);

    getSchedulerInfoRetrievedLatency = registry.newQuantiles("getSchedulerInfoRetrievedLatency",
        "latency of get scheduler info timeouts", "ops", "latency", 10);

    refreshSuperUserGroupsConfLatency = registry.newQuantiles("refreshSuperUserGroupsConfLatency",
        "latency of refresh superuser groups configuration timeouts", "ops", "latency", 10);

    refreshUserToGroupsMappingsLatency = registry.newQuantiles("refreshUserToGroupsMappingsLatency",
        "latency of refresh user to groups mappings timeouts", "ops", "latency", 10);

    refreshDeregisterSubClusterLatency = registry.newQuantiles("refreshDeregisterSubClusterLatency",
        "latency of deregister subcluster timeouts", "ops", "latency", 10);

    saveFederationQueuePolicyLatency = registry.newQuantiles("saveFederationQueuePolicyLatency",
        "latency of save federation queue policy timeouts", "ops", "latency", 10);

    batchSaveFederationQueuePoliciesLatency = registry.newQuantiles(
        "batchSaveFederationQueuePoliciesLatency",
        "latency of batch save federationqueuepolicies timeouts", "ops", "latency", 10);

    refreshAdminAclsLatency = registry.newQuantiles("refreshAdminAclsLatency",
        "latency of refresh admin acls timeouts", "ops", "latency", 10);

    refreshServiceAclsLatency = registry.newQuantiles("refreshServiceAclsLatency",
        "latency of refresh service acls timeouts", "ops", "latency", 10);

    replaceLabelsOnNodesLatency = registry.newQuantiles("replaceLabelsOnNodesLatency",
        "latency of replace labels on nodes timeouts", "ops", "latency", 10);

    replaceLabelsOnNodeLatency = registry.newQuantiles("replaceLabelsOnNodeLatency",
        "latency of replace labels on node timeouts", "ops", "latency", 10);

    addToClusterNodeLabelsLatency = registry.newQuantiles("addToClusterNodeLabelsLatency",
        "latency of add cluster nodelabels timeouts", "ops", "latency", 10);

    removeFromClusterNodeLabelsLatency = registry.newQuantiles("removeFromClusterNodeLabelsLatency",
        "latency of remove cluster nodelabels timeouts", "ops", "latency", 10);

    updateSchedulerConfigLatency = registry.newQuantiles("updateSchedulerConfigurationLatency",
        "latency of update scheduler configuration timeouts", "ops", "latency", 10);

    getSchedulerConfigurationLatency = registry.newQuantiles("getSchedulerConfigurationLatency",
        "latency of get scheduler configuration timeouts", "ops", "latency", 10);

    getClusterInfoLatency = registry.newQuantiles("getClusterInfoLatency",
        "latency of get cluster info timeouts", "ops", "latency", 10);

    getClusterUserInfoLatency = registry.newQuantiles("getClusterUserInfoLatency",
        "latency of get cluster user info timeouts", "ops", "latency", 10);

    updateNodeResourceLatency = registry.newQuantiles("updateNodeResourceLatency",
        "latency of update node resource timeouts", "ops", "latency", 10);

    refreshNodesResourcesLatency = registry.newQuantiles("refreshNodesResourcesLatency",
        "latency of refresh nodes resources timeouts", "ops", "latency", 10);

    checkForDecommissioningNodesLatency = registry.newQuantiles(
        "checkForDecommissioningNodesLatency", "latency of check for decommissioningnodes timeouts",
        "ops", "latency", 10);

    refreshClusterMaxPriorityLatency = registry.newQuantiles("refreshClusterMaxPriorityLatency",
        "latency of refresh cluster max priority timeouts", "ops", "latency", 10);

    mapAttributesToNodesLatency = registry.newQuantiles("mapAttributesToNodesLatency",
        "latency of map attributes to nodes timeouts", "ops", "latency", 10);

    getGroupsForUserLatency = registry.newQuantiles("getGroupsForUserLatency",
        "latency of get groups for user timeouts", "ops", "latency", 10);
  }

  public static RouterMetrics getMetrics() {
    if (!isInitialized.get()) {
      synchronized (RouterMetrics.class) {
        if (instance == null) {
          instance = DefaultMetricsSystem.instance().register("RouterMetrics",
              "Metrics for the Yarn Router", new RouterMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  @VisibleForTesting
  synchronized static void destroy() {
    isInitialized.set(false);
    instance = null;
  }

  @VisibleForTesting
  public long getNumSucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppsRetrieved() {
    return totalSucceededAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppAttemptsRetrieved() {
    return totalSucceededAppAttemptsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededMultipleAppsRetrieved() {
    return totalSucceededMultipleAppsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterMetricsRetrieved(){
    return totalSucceededGetClusterMetricsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterNodesRetrieved(){
    return totalSucceededGetClusterNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetNodeToLabelsRetrieved(){
    return totalSucceededGetNodeToLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetLabelsToNodesRetrieved(){
    return totalSucceededGetLabelsToNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterNodeLabelsRetrieved(){
    return totalSucceededGetClusterNodeLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAppAttemptReportRetrieved(){
    return totalSucceededAppAttemptReportRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetQueueUserAclsRetrieved(){
    return totalSucceededGetQueueUserAclsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetContainerReportRetrieved() {
    return totalSucceededGetContainerReportRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetContainersRetrieved() {
    return totalSucceededGetContainersRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededListReservationsRetrieved() {
    return totalSucceededListReservationsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetResourceTypeInfoRetrieved() {
    return totalSucceededGetResourceTypeInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededFailAppAttemptRetrieved() {
    return totalSucceededFailAppAttemptRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateAppPriorityRetrieved() {
    return totalSucceededUpdateAppPriorityRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateAppTimeoutsRetrieved() {
    return totalSucceededUpdateAppTimeoutsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededSignalToContainerRetrieved() {
    return totalSucceededSignalToContainerRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetQueueInfoRetrieved() {
    return totalSucceededGetQueueInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededMoveApplicationAcrossQueuesRetrieved() {
    return totalSucceededMoveApplicationAcrossQueuesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetResourceProfilesRetrieved() {
    return totalSucceededGetResourceProfilesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetResourceProfileRetrieved() {
    return totalSucceededGetResourceProfileRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAttributesToNodesRetrieved() {
    return totalSucceededGetAttributesToNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterNodeAttributesRetrieved() {
    return totalSucceededGetClusterNodeAttributesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetNodesToAttributesRetrieved() {
    return totalSucceededGetNodesToAttributesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetNewReservationRetrieved() {
    return totalSucceededGetNewReservationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededSubmitReservationRetrieved() {
    return totalSucceededSubmitReservationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateReservationRetrieved() {
    return totalSucceededUpdateReservationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededDeleteReservationRetrieved() {
    return totalSucceededDeleteReservationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededListReservationRetrieved() {
    return totalSucceededListReservationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppActivitiesRetrieved() {
    return totalSucceededGetAppActivitiesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppStatisticsRetrieved() {
    return totalSucceededGetAppStatisticsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppPriorityRetrieved() {
    return totalSucceededGetAppPriorityRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppQueueRetrieved() {
    return totalSucceededGetAppQueueRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateAppQueueRetrieved() {
    return totalSucceededUpdateAppQueueRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppTimeoutRetrieved() {
    return totalSucceededGetAppTimeoutRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetAppTimeoutsRetrieved() {
    return totalSucceededGetAppTimeoutsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshQueuesRetrieved() {
    return totalSucceededRefreshQueuesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshNodesRetrieved() {
    return totalSucceededRefreshNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetRMNodeLabelsRetrieved() {
    return totalSucceededGetRMNodeLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededCheckUserAccessToQueueRetrieved() {
    return totalSucceededCheckUserAccessToQueueRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetDelegationTokenRetrieved() {
    return totalSucceededGetDelegationTokenRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRenewDelegationTokenRetrieved() {
    return totalSucceededRenewDelegationTokenRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededCancelDelegationTokenRetrieved() {
    return totalSucceededCancelDelegationTokenRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededDumpSchedulerLogsRetrieved() {
    return totalSucceededDumpSchedulerLogsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetActivitiesRetrieved() {
    return totalSucceededGetActivitiesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetBulkActivitiesRetrieved() {
    return totalSucceededGetBulkActivitiesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetSchedulerInfoRetrieved() {
    return totalSucceededGetSchedulerInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededDeregisterSubClusterRetrieved() {
    return totalSucceededDeregisterSubClusterRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededSaveFederationQueuePolicyRetrieved() {
    return totalSucceededSaveFederationQueuePolicyRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededBatchSaveFederationQueuePoliciesRetrieved() {
    return totalSucceededBatchSaveFederationQueuePoliciesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshAdminAclsRetrieved() {
    return totalSucceededRefreshAdminAclsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshServiceAclsRetrieved() {
    return totalSucceededRefreshServiceAclsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededAddToClusterNodeLabelsRetrieved() {
    return totalSucceededAddToClusterNodeLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRemoveFromClusterNodeLabelsRetrieved() {
    return totalSucceededRemoveFromClusterNodeLabelsRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateSchedulerConfigurationRetrieved() {
    return totalSucceededUpdateSchedulerConfigurationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetSchedulerConfigurationRetrieved() {
    return totalSucceededGetSchedulerConfigurationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterInfoRetrieved() {
    return totalSucceededGetClusterInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetClusterUserInfoRetrieved() {
    return totalSucceededGetClusterUserInfoRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededUpdateNodeResourceRetrieved() {
    return totalSucceededUpdateNodeResourceRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshNodesResourcesRetrieved() {
    return totalSucceededRefreshNodesResourcesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededCheckForDecommissioningNodesRetrieved() {
    return totalSucceededCheckForDecommissioningNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshClusterMaxPriorityRetrieved() {
    return totalSucceededRefreshClusterMaxPriorityRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededMapAttributesToNodesRetrieved() {
    return totalSucceededMapAttributesToNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededGetGroupsForUsersRetrieved() {
    return totalSucceededGetGroupsForUsersRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededRefreshSuperUserGroupsConfigurationRetrieved() {
    return totalSucceededRefreshSuperUserGroupsConfigurationRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededReplaceLabelsOnNodesRetrieved() {
    return totalSucceededReplaceLabelsOnNodesRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public long getNumSucceededReplaceLabelsOnNodeRetrieved() {
    return totalSucceededReplaceLabelsOnNodeRetrieved.lastStat().numSamples();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsCreated() {
    return totalSucceededAppsCreated.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsSubmitted() {
    return totalSucceededAppsSubmitted.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppsKilled() {
    return totalSucceededAppsKilled.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppAttemptReport() {
    return totalSucceededAppAttemptReportRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppReport() {
    return totalSucceededAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededMultipleGetAppReport() {
    return totalSucceededMultipleAppsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterMetricsRetrieved() {
    return totalSucceededGetClusterMetricsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterNodesRetrieved() {
    return totalSucceededGetClusterNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetNodeToLabelsRetrieved() {
    return totalSucceededGetNodeToLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetLabelsToNodesRetrieved() {
    return totalSucceededGetLabelsToNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterNodeLabelsRetrieved() {
    return totalSucceededGetClusterNodeLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAppAttemptRetrieved() {
    return totalSucceededAppAttemptsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetQueueUserAclsRetrieved() {
    return totalSucceededGetQueueUserAclsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetContainerReportRetrieved() {
    return totalSucceededGetContainerReportRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetContainersRetrieved() {
    return totalSucceededGetContainersRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededListReservationsRetrieved() {
    return totalSucceededListReservationsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetResourceTypeInfoRetrieved() {
    return totalSucceededGetResourceTypeInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededFailAppAttemptRetrieved() {
    return totalSucceededFailAppAttemptRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateAppPriorityRetrieved() {
    return totalSucceededUpdateAppPriorityRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateAppTimeoutsRetrieved() {
    return totalSucceededUpdateAppTimeoutsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededSignalToContainerRetrieved() {
    return totalSucceededSignalToContainerRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetQueueInfoRetrieved() {
    return totalSucceededGetQueueInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededMoveApplicationAcrossQueuesRetrieved() {
    return totalSucceededMoveApplicationAcrossQueuesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetResourceProfilesRetrieved() {
    return totalSucceededGetResourceProfilesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetResourceProfileRetrieved() {
    return totalSucceededGetResourceProfileRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAttributesToNodesRetrieved() {
    return totalSucceededGetAttributesToNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterNodeAttributesRetrieved() {
    return totalSucceededGetClusterNodeAttributesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetNodesToAttributesRetrieved() {
    return totalSucceededGetNodesToAttributesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetNewReservationRetrieved() {
    return totalSucceededGetNewReservationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededSubmitReservationRetrieved() {
    return totalSucceededSubmitReservationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateReservationRetrieved() {
    return totalSucceededUpdateReservationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededDeleteReservationRetrieved() {
    return totalSucceededDeleteReservationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededListReservationRetrieved() {
    return totalSucceededListReservationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppActivitiesRetrieved() {
    return totalSucceededGetAppActivitiesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppStatisticsRetrieved() {
    return totalSucceededGetAppStatisticsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppPriorityRetrieved() {
    return totalSucceededGetAppPriorityRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppQueueRetrieved() {
    return totalSucceededGetAppQueueRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateAppQueueRetrieved() {
    return totalSucceededUpdateAppQueueRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppTimeoutRetrieved() {
    return totalSucceededGetAppTimeoutRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetAppTimeoutsRetrieved() {
    return totalSucceededGetAppTimeoutsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshQueuesRetrieved() {
    return totalSucceededRefreshQueuesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshNodesRetrieved() {
    return totalSucceededRefreshNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetRMNodeLabelsRetrieved() {
    return totalSucceededGetRMNodeLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededCheckUserAccessToQueueRetrieved() {
    return totalSucceededCheckUserAccessToQueueRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetDelegationTokenRetrieved() {
    return totalSucceededGetDelegationTokenRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRenewDelegationTokenRetrieved() {
    return totalSucceededRenewDelegationTokenRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededCancelDelegationTokenRetrieved() {
    return totalSucceededCancelDelegationTokenRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededDumpSchedulerLogsRetrieved() {
    return totalSucceededDumpSchedulerLogsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetActivitiesRetrieved() {
    return totalSucceededGetActivitiesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetBulkActivitiesRetrieved() {
    return totalSucceededGetBulkActivitiesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetSchedulerInfoRetrieved() {
    return totalSucceededGetSchedulerInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededDeregisterSubClusterRetrieved() {
    return totalSucceededDeregisterSubClusterRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededSaveFederationQueuePolicyRetrieved() {
    return totalSucceededSaveFederationQueuePolicyRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededBatchSaveFederationQueuePoliciesRetrieved() {
    return totalSucceededBatchSaveFederationQueuePoliciesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshAdminAclsRetrieved() {
    return totalSucceededRefreshAdminAclsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshServiceAclsRetrieved() {
    return totalSucceededRefreshServiceAclsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededAddToClusterNodeLabelsRetrieved() {
    return totalSucceededAddToClusterNodeLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRemoveFromClusterNodeLabelsRetrieved() {
    return totalSucceededRemoveFromClusterNodeLabelsRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateSchedulerConfigurationRetrieved() {
    return totalSucceededUpdateSchedulerConfigurationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetSchedulerConfigurationRetrieved() {
    return totalSucceededGetSchedulerConfigurationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterInfoRetrieved() {
    return totalSucceededGetClusterInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetClusterUserInfoRetrieved() {
    return totalSucceededGetClusterUserInfoRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededUpdateNodeResourceRetrieved() {
    return totalSucceededUpdateNodeResourceRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshNodesResourcesRetrieved() {
    return totalSucceededRefreshNodesResourcesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededCheckForDecommissioningNodesRetrieved() {
    return totalSucceededCheckForDecommissioningNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshClusterMaxPriorityRetrieved() {
    return totalSucceededRefreshClusterMaxPriorityRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededMapAttributesToNodesRetrieved() {
    return totalSucceededMapAttributesToNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededGetGroupsForUsersRetrieved() {
    return totalSucceededGetGroupsForUsersRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededRefreshSuperUserGroupsConfigurationRetrieved() {
    return totalSucceededRefreshSuperUserGroupsConfigurationRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededReplaceLabelsOnNodesRetrieved() {
    return totalSucceededReplaceLabelsOnNodesRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public double getLatencySucceededReplaceLabelsOnNodeRetrieved() {
    return totalSucceededReplaceLabelsOnNodeRetrieved.lastStat().mean();
  }

  @VisibleForTesting
  public int getAppsFailedCreated() {
    return numAppsFailedCreated.value();
  }

  @VisibleForTesting
  public int getAppsFailedSubmitted() {
    return numAppsFailedSubmitted.value();
  }

  @VisibleForTesting
  public int getAppsFailedKilled() {
    return numAppsFailedKilled.value();
  }

  @VisibleForTesting
  public int getAppsFailedRetrieved() {
    return numAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getAppAttemptsFailedRetrieved() {
    return numAppAttemptsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getMultipleAppsFailedRetrieved() {
    return numMultipleAppsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getClusterMetricsFailedRetrieved() {
    return numGetClusterMetricsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getClusterNodesFailedRetrieved() {
    return numGetClusterNodesFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getNodeToLabelsFailedRetrieved() {
    return numGetNodeToLabelsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getLabelsToNodesFailedRetrieved() {
    return numGetLabelsToNodesFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getGetClusterNodeLabelsFailedRetrieved() {
    return numGetClusterNodeLabelsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getAppAttemptReportFailedRetrieved() {
    return numAppAttemptReportFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getQueueUserAclsFailedRetrieved() {
    return numGetQueueUserAclsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getContainerReportFailedRetrieved() {
    return numGetContainerReportFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getContainersFailedRetrieved() {
    return numGetContainersFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getListReservationsFailedRetrieved() {
    return numListReservationsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getGetResourceTypeInfoRetrieved() {
    return numGetResourceTypeInfo.value();
  }

  @VisibleForTesting
  public int getFailApplicationAttemptFailedRetrieved() {
    return numFailAppAttemptFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getUpdateApplicationPriorityFailedRetrieved() {
    return numUpdateAppPriorityFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getUpdateApplicationTimeoutsFailedRetrieved() {
    return numUpdateAppTimeoutsFailedRetrieved.value();
  }

  @VisibleForTesting
  public int getSignalToContainerFailedRetrieved() {
    return numSignalToContainerFailedRetrieved.value();
  }

  public int getQueueInfoFailedRetrieved() {
    return numGetQueueInfoFailedRetrieved.value();
  }

  public int getMoveApplicationAcrossQueuesFailedRetrieved() {
    return numMoveApplicationAcrossQueuesFailedRetrieved.value();
  }

  public int getResourceProfilesFailedRetrieved() {
    return numGetResourceProfilesFailedRetrieved.value();
  }

  public int getResourceProfileFailedRetrieved() {
    return numGetResourceProfileFailedRetrieved.value();
  }

  public int getAttributesToNodesFailedRetrieved() {
    return numGetAttributesToNodesFailedRetrieved.value();
  }

  public int getClusterNodeAttributesFailedRetrieved() {
    return numGetClusterNodeAttributesFailedRetrieved.value();
  }

  public int getNodesToAttributesFailedRetrieved() {
    return numGetNodesToAttributesFailedRetrieved.value();
  }

  public int getNewReservationFailedRetrieved() {
    return numGetNewReservationFailedRetrieved.value();
  }

  public int getSubmitReservationFailedRetrieved() {
    return numSubmitReservationFailedRetrieved.value();
  }

  public int getUpdateReservationFailedRetrieved() {
    return numUpdateReservationFailedRetrieved.value();
  }

  public int getDeleteReservationFailedRetrieved() {
    return numDeleteReservationFailedRetrieved.value();
  }

  public int getListReservationFailedRetrieved() {
    return numListReservationFailedRetrieved.value();
  }

  public int getAppActivitiesFailedRetrieved() {
    return numGetAppActivitiesFailedRetrieved.value();
  }

  public int getAppStatisticsFailedRetrieved() {
    return numGetAppStatisticsFailedRetrieved.value();
  }

  public int getAppPriorityFailedRetrieved() {
    return numGetAppPriorityFailedRetrieved.value();
  }

  public int getAppQueueFailedRetrieved() {
    return numGetAppQueueFailedRetrieved.value();
  }

  public int getUpdateAppQueueFailedRetrieved() {
    return numUpdateAppQueueFailedRetrieved.value();
  }

  public int getAppTimeoutFailedRetrieved() {
    return numGetAppTimeoutFailedRetrieved.value();
  }

  public int getAppTimeoutsFailedRetrieved() {
    return numGetAppTimeoutsFailedRetrieved.value();
  }


  public int getRefreshQueuesFailedRetrieved() {
    return numRefreshQueuesFailedRetrieved.value();
  }

  public int getRMNodeLabelsFailedRetrieved() {
    return numGetRMNodeLabelsFailedRetrieved.value();
  }

  public int getCheckUserAccessToQueueFailedRetrieved() {
    return numCheckUserAccessToQueueFailedRetrieved.value();
  }

  public int getNumRefreshNodesFailedRetrieved() {
    return numRefreshNodesFailedRetrieved.value();
  }

  public int getNumRefreshSuperUserGroupsConfigurationFailedRetrieved() {
    return numRefreshSuperUserGroupsConfigurationFailedRetrieved.value();
  }

  public int getNumRefreshUserToGroupsMappingsFailedRetrieved() {
    return numRefreshUserToGroupsMappingsFailedRetrieved.value();
  }

  public int getDeregisterSubClusterFailedRetrieved() {
    return numDeregisterSubClusterFailedRetrieved.value();
  }

  public int getSaveFederationQueuePolicyFailedRetrieved() {
    return numSaveFederationQueuePolicyFailedRetrieved.value();
  }

  public int getBatchSaveFederationQueuePoliciesFailedRetrieved() {
    return numBatchSaveFederationQueuePoliciesFailedRetrieved.value();
  }

  public int getNumRefreshAdminAclsFailedRetrieved() {
    return numRefreshAdminAclsFailedRetrieved.value();
  }

  public int getNumRefreshServiceAclsFailedRetrieved() {
    return numRefreshServiceAclsFailedRetrieved.value();
  }

  public int getNumReplaceLabelsOnNodesFailedRetrieved() {
    return numReplaceLabelsOnNodesFailedRetrieved.value();
  }

  public int getNumReplaceLabelsOnNodeFailedRetrieved() {
    return numReplaceLabelsOnNodeFailedRetrieved.value();
  }

  public int getNumAddToClusterNodeLabelsFailedRetrieved() {
    return numAddToClusterNodeLabelsFailedRetrieved.value();
  }

  public int getNumRemoveFromClusterNodeLabelsFailedRetrieved() {
    return numRemoveFromClusterNodeLabelsFailedRetrieved.value();
  }

  public int getUpdateSchedulerConfigurationFailedRetrieved() {
    return numUpdateSchedulerConfigurationFailedRetrieved.value();
  }

  public int getSchedulerConfigurationFailedRetrieved() {
    return numGetSchedulerConfigurationFailedRetrieved.value();
  }

  public int getClusterInfoFailedRetrieved() {
    return numGetClusterInfoFailedRetrieved.value();
  }

  public int getClusterUserInfoFailedRetrieved() {
    return numGetClusterUserInfoFailedRetrieved.value();
  }

  public int getUpdateNodeResourceFailedRetrieved() {
    return numUpdateNodeResourceFailedRetrieved.value();
  }

  public int getRefreshNodesResourcesFailedRetrieved() {
    return numRefreshNodesResourcesFailedRetrieved.value();
  }

  public int getCheckForDecommissioningNodesFailedRetrieved() {
    return numCheckForDecommissioningNodesFailedRetrieved.value();
  }

  public int getRefreshClusterMaxPriorityFailedRetrieved() {
    return numRefreshClusterMaxPriorityFailedRetrieved.value();
  }

  public int getMapAttributesToNodesFailedRetrieved() {
    return numMapAttributesToNodesFailedRetrieved.value();
  }

  public int getGroupsForUserFailedRetrieved() {
    return numGetGroupsForUserFailedRetrieved.value();
  }

  public int getDelegationTokenFailedRetrieved() {
    return numGetDelegationTokenFailedRetrieved.value();
  }

  public int getRenewDelegationTokenFailedRetrieved() {
    return numRenewDelegationTokenFailedRetrieved.value();
  }

  public int getCancelDelegationTokenFailedRetrieved() {
    return numCancelDelegationTokenFailedRetrieved.value();
  }

  public int getDumpSchedulerLogsFailedRetrieved() {
    return numDumpSchedulerLogsFailedRetrieved.value();
  }

  public int getActivitiesFailedRetrieved() {
    return numGetActivitiesFailedRetrieved.value();
  }

  public int getBulkActivitiesFailedRetrieved(){
    return numGetBulkActivitiesFailedRetrieved.value();
  }

  public int getSchedulerInfoFailedRetrieved() {
    return numGetSchedulerInfoFailedRetrieved.value();
  }

  public void succeededAppsCreated(long duration) {
    totalSucceededAppsCreated.add(duration);
    getNewApplicationLatency.add(duration);
  }

  public void succeededAppsSubmitted(long duration) {
    totalSucceededAppsSubmitted.add(duration);
    submitApplicationLatency.add(duration);
  }

  public void succeededAppsKilled(long duration) {
    totalSucceededAppsKilled.add(duration);
    killApplicationLatency.add(duration);
  }

  public void succeededAppsRetrieved(long duration) {
    totalSucceededAppsRetrieved.add(duration);
    getApplicationReportLatency.add(duration);
  }

  public void succeededMultipleAppsRetrieved(long duration) {
    totalSucceededMultipleAppsRetrieved.add(duration);
    getApplicationsReportLatency.add(duration);
  }

  public void succeededAppAttemptsRetrieved(long duration) {
    totalSucceededAppAttemptsRetrieved.add(duration);
    getApplicationAttemptsLatency.add(duration);
  }

  public void succeededGetClusterMetricsRetrieved(long duration) {
    totalSucceededGetClusterMetricsRetrieved.add(duration);
    getClusterMetricsLatency.add(duration);
  }

  public void succeededGetClusterNodesRetrieved(long duration) {
    totalSucceededGetClusterNodesRetrieved.add(duration);
    getClusterNodesLatency.add(duration);
  }

  public void succeededGetNodeToLabelsRetrieved(long duration) {
    totalSucceededGetNodeToLabelsRetrieved.add(duration);
    getNodeToLabelsLatency.add(duration);
  }

  public void succeededGetLabelsToNodesRetrieved(long duration) {
    totalSucceededGetLabelsToNodesRetrieved.add(duration);
    getLabelToNodesLatency.add(duration);
  }

  public void succeededGetClusterNodeLabelsRetrieved(long duration) {
    totalSucceededGetClusterNodeLabelsRetrieved.add(duration);
    getClusterNodeLabelsLatency.add(duration);
  }

  public void succeededAppAttemptReportRetrieved(long duration) {
    totalSucceededAppAttemptReportRetrieved.add(duration);
    getApplicationAttemptReportLatency.add(duration);
  }

  public void succeededGetQueueUserAclsRetrieved(long duration) {
    totalSucceededGetQueueUserAclsRetrieved.add(duration);
    getQueueUserAclsLatency.add(duration);
  }

  public void succeededGetContainerReportRetrieved(long duration) {
    totalSucceededGetContainerReportRetrieved.add(duration);
    getContainerReportLatency.add(duration);
  }

  public void succeededGetContainersRetrieved(long duration) {
    totalSucceededGetContainersRetrieved.add(duration);
    getContainerLatency.add(duration);
  }

  public void succeededListReservationsRetrieved(long duration) {
    totalSucceededListReservationsRetrieved.add(duration);
    listReservationsLatency.add(duration);
  }

  public void succeededGetResourceTypeInfoRetrieved(long duration) {
    totalSucceededGetResourceTypeInfoRetrieved.add(duration);
    listResourceTypeInfoLatency.add(duration);
  }

  public void succeededFailAppAttemptRetrieved(long duration) {
    totalSucceededFailAppAttemptRetrieved.add(duration);
    failAppAttemptLatency.add(duration);
  }

  public void succeededUpdateAppPriorityRetrieved(long duration) {
    totalSucceededUpdateAppPriorityRetrieved.add(duration);
    updateAppPriorityLatency.add(duration);
  }

  public void succeededUpdateAppTimeoutsRetrieved(long duration) {
    totalSucceededUpdateAppTimeoutsRetrieved.add(duration);
    updateAppTimeoutsLatency.add(duration);
  }

  public void succeededSignalToContainerRetrieved(long duration) {
    totalSucceededSignalToContainerRetrieved.add(duration);
    signalToContainerLatency.add(duration);
  }

  public void succeededGetQueueInfoRetrieved(long duration) {
    totalSucceededGetQueueInfoRetrieved.add(duration);
    getQueueInfoLatency.add(duration);
  }

  public void succeededMoveApplicationAcrossQueuesRetrieved(long duration) {
    totalSucceededMoveApplicationAcrossQueuesRetrieved.add(duration);
    moveApplicationAcrossQueuesLatency.add(duration);
  }

  public void succeededGetResourceProfilesRetrieved(long duration) {
    totalSucceededGetResourceProfilesRetrieved.add(duration);
    getResourceProfilesLatency.add(duration);
  }

  public void succeededGetResourceProfileRetrieved(long duration) {
    totalSucceededGetResourceProfileRetrieved.add(duration);
    getResourceProfileLatency.add(duration);
  }

  public void succeededGetAttributesToNodesRetrieved(long duration) {
    totalSucceededGetAttributesToNodesRetrieved.add(duration);
    getAttributesToNodesLatency.add(duration);
  }

  public void succeededGetClusterNodeAttributesRetrieved(long duration) {
    totalSucceededGetClusterNodeAttributesRetrieved.add(duration);
    getClusterNodeAttributesLatency.add(duration);
  }

  public void succeededGetNodesToAttributesRetrieved(long duration) {
    totalSucceededGetNodesToAttributesRetrieved.add(duration);
    getNodesToAttributesLatency.add(duration);
  }

  public void succeededGetNewReservationRetrieved(long duration) {
    totalSucceededGetNewReservationRetrieved.add(duration);
    getNewReservationLatency.add(duration);
  }

  public void succeededSubmitReservationRetrieved(long duration) {
    totalSucceededSubmitReservationRetrieved.add(duration);
    submitReservationLatency.add(duration);
  }

  public void succeededUpdateReservationRetrieved(long duration) {
    totalSucceededUpdateReservationRetrieved.add(duration);
    updateReservationLatency.add(duration);
  }

  public void succeededDeleteReservationRetrieved(long duration) {
    totalSucceededDeleteReservationRetrieved.add(duration);
    deleteReservationLatency.add(duration);
  }

  public void succeededListReservationRetrieved(long duration) {
    totalSucceededListReservationRetrieved.add(duration);
    listReservationLatency.add(duration);
  }

  public void succeededGetAppActivitiesRetrieved(long duration) {
    totalSucceededGetAppActivitiesRetrieved.add(duration);
    getAppActivitiesLatency.add(duration);
  }

  public void succeededGetAppStatisticsRetrieved(long duration) {
    totalSucceededGetAppStatisticsRetrieved.add(duration);
    getAppStatisticsLatency.add(duration);
  }

  public void succeededGetAppPriorityRetrieved(long duration) {
    totalSucceededGetAppPriorityRetrieved.add(duration);
    getAppPriorityLatency.add(duration);
  }

  public void succeededGetAppQueueRetrieved(long duration) {
    totalSucceededGetAppQueueRetrieved.add(duration);
    getAppQueueLatency.add(duration);
  }

  public void succeededUpdateAppQueueRetrieved(long duration) {
    totalSucceededUpdateAppQueueRetrieved.add(duration);
    getUpdateQueueLatency.add(duration);
  }

  public void succeededGetAppTimeoutRetrieved(long duration) {
    totalSucceededGetAppTimeoutRetrieved.add(duration);
    getAppTimeoutLatency.add(duration);
  }

  public void succeededGetAppTimeoutsRetrieved(long duration) {
    totalSucceededGetAppTimeoutsRetrieved.add(duration);
    getAppTimeoutsLatency.add(duration);
  }

  public void succeededRefreshQueuesRetrieved(long duration) {
    totalSucceededRefreshQueuesRetrieved.add(duration);
    refreshQueuesLatency.add(duration);
  }

  public void succeededRefreshNodesRetrieved(long duration) {
    totalSucceededRefreshNodesRetrieved.add(duration);
    refreshNodesLatency.add(duration);
  }

  public void succeededGetRMNodeLabelsRetrieved(long duration) {
    totalSucceededGetRMNodeLabelsRetrieved.add(duration);
    getRMNodeLabelsLatency.add(duration);
  }

  public void succeededCheckUserAccessToQueueRetrieved(long duration) {
    totalSucceededCheckUserAccessToQueueRetrieved.add(duration);
    checkUserAccessToQueueLatency.add(duration);
  }

  public void succeededGetDelegationTokenRetrieved(long duration) {
    totalSucceededGetDelegationTokenRetrieved.add(duration);
    getDelegationTokenLatency.add(duration);
  }

  public void succeededRenewDelegationTokenRetrieved(long duration) {
    totalSucceededRenewDelegationTokenRetrieved.add(duration);
    renewDelegationTokenLatency.add(duration);
  }

  public void succeededCancelDelegationTokenRetrieved(long duration) {
    totalSucceededCancelDelegationTokenRetrieved.add(duration);
    cancelDelegationTokenLatency.add(duration);
  }

  public void succeededDumpSchedulerLogsRetrieved(long duration) {
    totalSucceededDumpSchedulerLogsRetrieved.add(duration);
    dumpSchedulerLogsLatency.add(duration);
  }

  public void succeededGetActivitiesLatencyRetrieved(long duration) {
    totalSucceededGetActivitiesRetrieved.add(duration);
    getActivitiesLatency.add(duration);
  }

  public void succeededGetBulkActivitiesRetrieved(long duration) {
    totalSucceededGetBulkActivitiesRetrieved.add(duration);
    getBulkActivitiesLatency.add(duration);
  }

  public void succeededGetSchedulerInfoRetrieved(long duration) {
    totalSucceededGetSchedulerInfoRetrieved.add(duration);
    getSchedulerInfoRetrievedLatency.add(duration);
  }

  public void succeededDeregisterSubClusterRetrieved(long duration) {
    totalSucceededDeregisterSubClusterRetrieved.add(duration);
    refreshDeregisterSubClusterLatency.add(duration);
  }

  public void succeededSaveFederationQueuePolicyRetrieved(long duration) {
    totalSucceededSaveFederationQueuePolicyRetrieved.add(duration);
    saveFederationQueuePolicyLatency.add(duration);
  }

  public void succeededBatchSaveFederationQueuePoliciesRetrieved(long duration) {
    totalSucceededBatchSaveFederationQueuePoliciesRetrieved.add(duration);
    batchSaveFederationQueuePoliciesLatency.add(duration);
  }

  public void succeededRefreshAdminAclsRetrieved(long duration) {
    totalSucceededRefreshAdminAclsRetrieved.add(duration);
    refreshAdminAclsLatency.add(duration);
  }

  public void succeededRefreshServiceAclsRetrieved(long duration) {
    totalSucceededRefreshServiceAclsRetrieved.add(duration);
    refreshServiceAclsLatency.add(duration);
  }

  public void succeededAddToClusterNodeLabelsRetrieved(long duration) {
    totalSucceededAddToClusterNodeLabelsRetrieved.add(duration);
    addToClusterNodeLabelsLatency.add(duration);
  }

  public void succeededRemoveFromClusterNodeLabelsRetrieved(long duration) {
    totalSucceededRemoveFromClusterNodeLabelsRetrieved.add(duration);
    removeFromClusterNodeLabelsLatency.add(duration);
  }

  public void succeededUpdateSchedulerConfigurationRetrieved(long duration) {
    totalSucceededUpdateSchedulerConfigurationRetrieved.add(duration);
    updateSchedulerConfigLatency.add(duration);
  }

  public void succeededGetSchedulerConfigurationRetrieved(long duration) {
    totalSucceededGetSchedulerConfigurationRetrieved.add(duration);
    getSchedulerConfigurationLatency.add(duration);
  }

  public void succeededGetClusterInfoRetrieved(long duration) {
    totalSucceededGetClusterInfoRetrieved.add(duration);
    getClusterInfoLatency.add(duration);
  }

  public void succeededGetClusterUserInfoRetrieved(long duration) {
    totalSucceededGetClusterUserInfoRetrieved.add(duration);
    getClusterUserInfoLatency.add(duration);
  }

  public void succeededUpdateNodeResourceRetrieved(long duration) {
    totalSucceededUpdateNodeResourceRetrieved.add(duration);
    updateNodeResourceLatency.add(duration);
  }

  public void succeededRefreshNodesResourcesRetrieved(long duration) {
    totalSucceededRefreshNodesResourcesRetrieved.add(duration);
    refreshNodesResourcesLatency.add(duration);
  }

  public void succeededCheckForDecommissioningNodesRetrieved(long duration) {
    totalSucceededCheckForDecommissioningNodesRetrieved.add(duration);
    checkForDecommissioningNodesLatency.add(duration);
  }

  public void succeededRefreshClusterMaxPriorityRetrieved(long duration) {
    totalSucceededRefreshClusterMaxPriorityRetrieved.add(duration);
    refreshClusterMaxPriorityLatency.add(duration);
  }

  public void succeededMapAttributesToNodesRetrieved(long duration) {
    totalSucceededMapAttributesToNodesRetrieved.add(duration);
    mapAttributesToNodesLatency.add(duration);
  }

  public void succeededGetGroupsForUsersRetrieved(long duration) {
    totalSucceededGetGroupsForUsersRetrieved.add(duration);
    getGroupsForUserLatency.add(duration);
  }

  public void succeededRefreshSuperUserGroupsConfRetrieved(long duration) {
    totalSucceededRefreshSuperUserGroupsConfigurationRetrieved.add(duration);
    refreshSuperUserGroupsConfLatency.add(duration);
  }

  public void succeededRefreshUserToGroupsMappingsRetrieved(long duration) {
    totalSucceededRefreshUserToGroupsMappingsRetrieved.add(duration);
    refreshUserToGroupsMappingsLatency.add(duration);
  }

  public void succeededReplaceLabelsOnNodesRetrieved(long duration) {
    totalSucceededReplaceLabelsOnNodesRetrieved.add(duration);
    replaceLabelsOnNodesLatency.add(duration);
  }

  public void succeededReplaceLabelsOnNodeRetrieved(long duration) {
    totalSucceededReplaceLabelsOnNodeRetrieved.add(duration);
    replaceLabelsOnNodeLatency.add(duration);
  }

  public void incrAppsFailedCreated() {
    numAppsFailedCreated.incr();
  }

  public void incrAppsFailedSubmitted() {
    numAppsFailedSubmitted.incr();
  }

  public void incrAppsFailedKilled() {
    numAppsFailedKilled.incr();
  }

  public void incrAppsFailedRetrieved() {
    numAppsFailedRetrieved.incr();
  }

  public void incrMultipleAppsFailedRetrieved() {
    numMultipleAppsFailedRetrieved.incr();
  }

  public void incrAppAttemptsFailedRetrieved() {
    numAppAttemptsFailedRetrieved.incr();
  }

  public void incrGetClusterMetricsFailedRetrieved() {
    numGetClusterMetricsFailedRetrieved.incr();
  }

  public void incrClusterNodesFailedRetrieved() {
    numGetClusterNodesFailedRetrieved.incr();
  }

  public void incrNodeToLabelsFailedRetrieved() {
    numGetNodeToLabelsFailedRetrieved.incr();
  }

  public void incrLabelsToNodesFailedRetrieved() {
    numGetLabelsToNodesFailedRetrieved.incr();
  }

  public void incrClusterNodeLabelsFailedRetrieved() {
    numGetClusterNodeLabelsFailedRetrieved.incr();
  }

  public void incrAppAttemptReportFailedRetrieved() {
    numAppAttemptReportFailedRetrieved.incr();
  }

  public void incrQueueUserAclsFailedRetrieved() {
    numGetQueueUserAclsFailedRetrieved.incr();
  }

  public void incrGetContainerReportFailedRetrieved() {
    numGetContainerReportFailedRetrieved.incr();
  }

  public void incrGetContainersFailedRetrieved() {
    numGetContainersFailedRetrieved.incr();
  }

  public void incrListReservationsFailedRetrieved() {
    numListReservationsFailedRetrieved.incr();
  }

  public void incrResourceTypeInfoFailedRetrieved() {
    numGetResourceTypeInfo.incr();
  }

  public void incrFailAppAttemptFailedRetrieved() {
    numFailAppAttemptFailedRetrieved.incr();
  }

  public void incrUpdateAppPriorityFailedRetrieved() {
    numUpdateAppPriorityFailedRetrieved.incr();
  }

  public void incrUpdateApplicationTimeoutsRetrieved() {
    numUpdateAppTimeoutsFailedRetrieved.incr();
  }

  public void incrSignalToContainerFailedRetrieved() {
    numSignalToContainerFailedRetrieved.incr();
  }

  public void incrGetQueueInfoFailedRetrieved() {
    numGetQueueInfoFailedRetrieved.incr();
  }

  public void incrMoveApplicationAcrossQueuesFailedRetrieved() {
    numMoveApplicationAcrossQueuesFailedRetrieved.incr();
  }

  public void incrGetResourceProfilesFailedRetrieved() {
    numGetResourceProfilesFailedRetrieved.incr();
  }

  public void incrGetResourceProfileFailedRetrieved() {
    numGetResourceProfileFailedRetrieved.incr();
  }

  public void incrGetAttributesToNodesFailedRetrieved() {
    numGetAttributesToNodesFailedRetrieved.incr();
  }

  public void incrGetClusterNodeAttributesFailedRetrieved() {
    numGetClusterNodeAttributesFailedRetrieved.incr();
  }

  public void incrGetNodesToAttributesFailedRetrieved() {
    numGetNodesToAttributesFailedRetrieved.incr();
  }

  public void incrGetNewReservationFailedRetrieved() {
    numGetNewReservationFailedRetrieved.incr();
  }

  public void incrSubmitReservationFailedRetrieved() {
    numSubmitReservationFailedRetrieved.incr();
  }

  public void incrUpdateReservationFailedRetrieved() {
    numUpdateReservationFailedRetrieved.incr();
  }

  public void incrDeleteReservationFailedRetrieved() {
    numDeleteReservationFailedRetrieved.incr();
  }

  public void incrListReservationFailedRetrieved() {
    numListReservationFailedRetrieved.incr();
  }

  public void incrGetAppActivitiesFailedRetrieved() {
    numGetAppActivitiesFailedRetrieved.incr();
  }

  public void incrGetAppStatisticsFailedRetrieved() {
    numGetAppStatisticsFailedRetrieved.incr();
  }

  public void incrGetAppPriorityFailedRetrieved() {
    numGetAppPriorityFailedRetrieved.incr();
  }

  public void incrGetAppQueueFailedRetrieved() {
    numGetAppQueueFailedRetrieved.incr();
  }

  public void incrUpdateAppQueueFailedRetrieved() {
    numUpdateAppQueueFailedRetrieved.incr();
  }

  public void incrGetAppTimeoutFailedRetrieved() {
    numGetAppTimeoutFailedRetrieved.incr();
  }

  public void incrGetAppTimeoutsFailedRetrieved() {
    numGetAppTimeoutsFailedRetrieved.incr();
  }

  public void incrRefreshQueuesFailedRetrieved() {
    numRefreshQueuesFailedRetrieved.incr();
  }

  public void incrGetRMNodeLabelsFailedRetrieved() {
    numGetRMNodeLabelsFailedRetrieved.incr();
  }

  public void incrCheckUserAccessToQueueFailedRetrieved() {
    numCheckUserAccessToQueueFailedRetrieved.incr();
  }

  public void incrRefreshNodesFailedRetrieved() {
    numRefreshNodesFailedRetrieved.incr();
  }

  public void incrRefreshSuperUserGroupsConfigurationFailedRetrieved() {
    numRefreshSuperUserGroupsConfigurationFailedRetrieved.incr();
  }

  public void incrRefreshUserToGroupsMappingsFailedRetrieved() {
    numRefreshUserToGroupsMappingsFailedRetrieved.incr();
  }

  public void incrDeregisterSubClusterFailedRetrieved() {
    numDeregisterSubClusterFailedRetrieved.incr();
  }

  public void incrSaveFederationQueuePolicyFailedRetrieved() {
    numSaveFederationQueuePolicyFailedRetrieved.incr();
  }

  public void incrBatchSaveFederationQueuePoliciesFailedRetrieved() {
    numBatchSaveFederationQueuePoliciesFailedRetrieved.incr();
  }

  public void incrRefreshAdminAclsFailedRetrieved() {
    numRefreshAdminAclsFailedRetrieved.incr();
  }

  public void incrRefreshServiceAclsFailedRetrieved() {
    numRefreshServiceAclsFailedRetrieved.incr();
  }

  public void incrAddToClusterNodeLabelsFailedRetrieved() {
    numAddToClusterNodeLabelsFailedRetrieved.incr();
  }

  public void incrRemoveFromClusterNodeLabelsFailedRetrieved() {
    numRemoveFromClusterNodeLabelsFailedRetrieved.incr();
  }

  public void incrUpdateSchedulerConfigurationFailedRetrieved() {
    numUpdateSchedulerConfigurationFailedRetrieved.incr();
  }

  public void incrGetSchedulerConfigurationFailedRetrieved() {
    numGetSchedulerConfigurationFailedRetrieved.incr();
  }

  public void incrGetClusterInfoFailedRetrieved() {
    numGetClusterInfoFailedRetrieved.incr();
  }

  public void incrGetClusterUserInfoFailedRetrieved() {
    numGetClusterUserInfoFailedRetrieved.incr();
  }

  public void incrUpdateNodeResourceFailedRetrieved() {
    numUpdateNodeResourceFailedRetrieved.incr();
  }

  public void incrRefreshNodesResourcesFailedRetrieved() {
    numRefreshNodesResourcesFailedRetrieved.incr();
  }

  public void incrCheckForDecommissioningNodesFailedRetrieved() {
    numCheckForDecommissioningNodesFailedRetrieved.incr();
  }

  public void incrRefreshClusterMaxPriorityFailedRetrieved() {
    numRefreshClusterMaxPriorityFailedRetrieved.incr();
  }

  public void incrMapAttributesToNodesFailedRetrieved() {
    numMapAttributesToNodesFailedRetrieved.incr();
  }

  public void incrGetGroupsForUserFailedRetrieved() {
    numGetGroupsForUserFailedRetrieved.incr();
  }

  public void incrGetDelegationTokenFailedRetrieved() {
    numGetDelegationTokenFailedRetrieved.incr();
  }

  public void incrRenewDelegationTokenFailedRetrieved() {
    numRenewDelegationTokenFailedRetrieved.incr();
  }

  public void incrCancelDelegationTokenFailedRetrieved() {
    numCancelDelegationTokenFailedRetrieved.incr();
  }

  public void incrReplaceLabelsOnNodesFailedRetrieved() {
    numReplaceLabelsOnNodesFailedRetrieved.incr();
  }

  public void incrReplaceLabelsOnNodeFailedRetrieved() {
    numReplaceLabelsOnNodeFailedRetrieved.incr();
  }

  public void incrDumpSchedulerLogsFailedRetrieved() {
    numDumpSchedulerLogsFailedRetrieved.incr();
  }

  public void incrGetActivitiesFailedRetrieved() {
    numGetActivitiesFailedRetrieved.incr();
  }

  public void incrGetBulkActivitiesFailedRetrieved() {
    numGetBulkActivitiesFailedRetrieved.incr();
  }

  public void incrGetSchedulerInfoFailedRetrieved() {
    numGetSchedulerInfoFailedRetrieved.incr();
  }
}
