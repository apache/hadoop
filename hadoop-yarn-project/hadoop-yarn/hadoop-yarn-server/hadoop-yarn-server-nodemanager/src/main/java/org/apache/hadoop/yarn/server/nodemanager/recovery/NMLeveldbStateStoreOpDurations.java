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
package org.apache.hadoop.yarn.server.nodemanager.recovery;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Class to capture the performance metrics of NM LeveldbStateStore.
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="NMLeveldbStateStore-op-durations")
public final class NMLeveldbStateStoreOpDurations implements MetricsSource {

    @Metric("Duration for load applications state")
    MutableRate loadApplicationsStateDuration;

    @Metric("Duration for store application state")
    MutableRate storeApplicationStateDuration;

    @Metric("Duration to handle remove application state")
    MutableRate removeApplicationStateDuration;

    @Metric("Duration for store deletion task state")
    MutableRate storeDeletionTaskStateDuration;

    @Metric("Duration to handle remove deletion task state")
    MutableRate removeDeletionTaskStateDuration;

    @Metric("Duration for the state of the deletion service")
    MutableRate loadDeletionServiceStateDuration;

    @Metric("Duration for store the start request of container")
    MutableRate storeContainerStateDuration;

    @Metric("Duration for store a queued container state")
    MutableRate storeContainerQueuedStateDuration;

    @Metric("Duration for store a paused container state")
    MutableRate storeContainerPausedStateDuration;

    @Metric("Duration for store a launched container state")
    MutableRate storeContainerLaunchedStateDuration;

    @Metric("Duration for store a updated container state")
    MutableRate storeContainerUpdateTokenStateDuration;

    @Metric("Duration for store a completed container state")
    MutableRate storeContainerCompletedStateDuration;

    @Metric("Duration for store a Killed container state")
    MutableRate storeContainerKilledStateDuration;

    @Metric("Duration for store the diagnostics of container")
    MutableRate storeContainerDiagnosticsStateDuration;

    @Metric("Duration for store the remaining retry attempts of container")
    MutableRate storeContainerRemainingRetryAttemptsDuration;

    @Metric("Duration for store the restart times of container")
    MutableRate storeContainerRestartTimesDuration;

    @Metric("Duration for store the working directory of container")
    MutableRate storeContainerWorkDirStateDuration;

    @Metric("Duration for store the log directory of container")
    MutableRate storeContainerLogDirStateDuration;

    @Metric("Duration to handle remove container state")
    MutableRate removeContainerStateDuration;

    @Metric("Duration for load localized resources state")
    MutableRate loadLocalizationStateDuration;

    @Metric("Duration for store the start of localization for resource")
    MutableRate storeStartResourceLocalizationStateCall;

    @Metric("Duration for store the completion of a resource localization")
    MutableRate storeFinishResourceLocalizationStateDuration;

    @Metric("Duration to handle remove a resource localization")
    MutableRate removeLocalizedResourceStateDuration;

    @Metric("Duration to handle remove container has been resumed")
    MutableRate removeContainerPausedStateDuration;

    @Metric("Duration to handle remove container has been queued")
    MutableRate removeContainerQueuedStateDuration;

    @Metric("Duration for load the state of NM tokens state")
    MutableRate loadNMTokensStateDuration;

    @Metric("Duration for store the current NM token master key")
    MutableRate storeNMTokenCurrentMasterKeyStateDuration;

    @Metric("Duration for store the previous NM token master key")
    MutableRate storeNMTokenPreviousMasterKeyStateDuration;

    @Metric("Duration for store the master key of application")
    MutableRate storeNMTokenApplicationMasterKeyStateDuration;

    @Metric("Duration to handle remove the master key of application")
    MutableRate removeNMTokenApplicationMasterKeyStateDuration;

    @Metric("Duration for load the state of container tokens")
    MutableRate loadContainerTokensStateDuration;

    @Metric("Duration for store the current container token master key")
    MutableRate storeContainerTokenCurrentMasterKeyStateDuration;

    @Metric("Duration for store the previous container token master key")
    MutableRate storeContainerTokenPreviousMasterKeyStateDuration;

    @Metric("Duration for store the expiration time for a container token")
    MutableRate storeContainerTokenStateDuration;

    @Metric("Duration to handle remove records for a container token")
    MutableRate removeContainerTokenStateDuration;

    @Metric("Duration for load the state of log deleters")
    MutableRate loadLogDeleterStateDuration;

    @Metric("Duration for store the state of a log deleter")
    MutableRate storeLogDeleterStateDuration;

    @Metric("Duration to handle remove the state of a log deleter")
    MutableRate removeLogDeleterStateDuration;

    @Metric("Duration for load the state of AMRMProxy")
    MutableRate loadAMRMProxyStateDuration;

    @Metric("Duration for store " +
            "the current AMRMProxyTokenSecretManager master key")
    MutableRate storeAMRMProxyCurrentMasterKeyStateDuration;

    @Metric("Duration for store a context " +
            "entry of application attempt in AMRMProxyService")
    MutableRate storeAMRMProxyAppContextEntryStateDuration;

    @Metric("Duration for store " +
            "the next AMRMProxyTokenSecretManager master key")
    MutableRate storeAMRMProxyNextMasterKeyStateDuration;

    @Metric("Duration to handle remove a context entry for an application")
    MutableRate removeAMRMProxyAppContextEntryStateDuration;

    @Metric("Duration to handle remove a context entry " +
            "for an application attempt in AMRMProxyService")
    MutableRate removeAMRMProxyAppContextStateDuration;

    @Metric("Duration for store the assigned resources to a container")
    MutableRate storeAssignedResourcesStateDuration;


    protected static final MetricsInfo RECORD_INFO =
        info("NMLeveldbStateStoreOpDurations",
  "Durations of NMLeveldbStateStore calls");

    private final MetricsRegistry registry;

    private static final NMLeveldbStateStoreOpDurations INSTANCE
        = new NMLeveldbStateStoreOpDurations();

    public static NMLeveldbStateStoreOpDurations getInstance() {
        return INSTANCE;
    }

    private NMLeveldbStateStoreOpDurations() {
        registry = new MetricsRegistry(RECORD_INFO);
        registry.tag(RECORD_INFO, "NMLeveldbStateStoreOpDurations");

        MetricsSystem ms = DefaultMetricsSystem.instance();
        if (ms != null) {
            ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
        }
    }

    @Override
    public synchronized void getMetrics(MetricsCollector collector, boolean all) {
        registry.snapshot(collector.addRecord(registry.info()), all);
    }

    public void addLoadApplicationsStateDuration(long value) {
        loadApplicationsStateDuration.add(value);
    }

    public void addStoreApplicationStateDuration(long value) {
        storeApplicationStateDuration.add(value);
    }

    public void addRemoveApplicationStateDuration(long value) {
        removeApplicationStateDuration.add(value);
    }

    public void addsStoreDeletionTaskStateDuration(long value) {
        storeDeletionTaskStateDuration.add(value);
    }

    public void addRemoveDeletionTaskStateDuration(long value) {
        removeDeletionTaskStateDuration.add(value);
    }

    public void addLoadDeletionServiceStateDuration(long value) {
        loadDeletionServiceStateDuration.add(value);
    }

    public void addStoreContainerStateDuration(long value) {
        storeContainerStateDuration.add(value);
    }

    public void addStoreContainerQueuedStateDuration(long value) {
        storeContainerQueuedStateDuration.add(value);
    }

    public void addStoreContainerPausedStateDuration(long value) {
        storeContainerPausedStateDuration.add(value);
    }

    public void addStoreContainerLaunchedStateDuration(long value) {
        storeContainerLaunchedStateDuration.add(value);
    }

    public void addStoreContainerUpdateTokenStateDuration(long value) {
        storeContainerUpdateTokenStateDuration.add(value);
    }

    public void addStoreContainerCompletedStateDuration(long value) {
        storeContainerCompletedStateDuration.add(value);
    }

    public void addStoreContainerKilledStateDuration(long value) {
        storeContainerKilledStateDuration.add(value);
    }

    public void addStoreContainerDiagnosticsStateDuration(long value) {
        storeContainerDiagnosticsStateDuration.add(value);
    }

    public void addStoreContainerRemainingRetryAttemptsDuration(long value) {
        storeContainerRemainingRetryAttemptsDuration.add(value);
    }

    public void addStoreContainerRestartTimesDuration(long value) {
        storeContainerRestartTimesDuration.add(value);
    }

    public void addStoreContainerWorkDirStateDuration(long value) {
        storeContainerWorkDirStateDuration.add(value);
    }

    public void addStoreContainerLogDirStateDuration(long value) {
        storeContainerLogDirStateDuration.add(value);
    }

    public void addRemoveContainerStateDuration(long value) {
        removeContainerStateDuration.add(value);
    }
    public void addLoadLocalizationStateDuration(long value) {
        loadLocalizationStateDuration.add(value);
    }

    public void addStoreStartResourceLocalizationDuration(long value) {
        storeStartResourceLocalizationStateCall.add(value);
    }

    public void addStoreFinishResourceLocalizationDuration(long value) {
        storeFinishResourceLocalizationStateDuration.add(value);
    }
    public void addRemoveLocalizedResourceStateDuration(long value) {
        removeLocalizedResourceStateDuration.add(value);
    }

    public void addRemoveContainerPausedStateDuration(long value) {
        removeContainerPausedStateDuration.add(value);
    }

    public void addLoadNMTokensStateDuration(long value) {
        loadNMTokensStateDuration.add(value);
    }

    public void addStoreNMTokenCurrentMasterKeyDuration(long value) {
        storeNMTokenCurrentMasterKeyStateDuration.add(value);
    }

    public void addStoreNMTokenPreviousMasterKeyDuration(long value) {
        storeNMTokenPreviousMasterKeyStateDuration.add(value);
    }

    public void addStoreNMTokenApplicationMasterKeyDuration(long value) {
        storeNMTokenApplicationMasterKeyStateDuration.add(value);
    }
    public void addRemoveNMTokenApplicationMasterKeyDuration(long value) {
        removeNMTokenApplicationMasterKeyStateDuration.add(value);
    }

    public void addLoadContainerTokensStateDuration(long value) {
        loadContainerTokensStateDuration.add(value);
    }

    public void addStoreContainerTokenCurrentMasterKeyDuration(long value) {
        storeContainerTokenCurrentMasterKeyStateDuration.add(value);
    }

    public void addStoreContainerTokenPreviousMasterKeyDuration(long value) {
        storeContainerTokenPreviousMasterKeyStateDuration.add(value);
    }

    public void addStoreContainerTokenStateDuration(long value) {
        storeContainerTokenStateDuration.add(value);
    }

    public void addLoadLogDeleterStateDuration(long value) {
        loadLogDeleterStateDuration.add(value);
    }

    public void addStoreLogDeleterStateDuration(long value) {
        storeLogDeleterStateDuration.add(value);
    }

    public void addRemoveLogDeleterStateDuration(long value) {
        removeLogDeleterStateDuration.add(value);
    }

    public void addLoadAMRMProxyStateDuration(long value) {
        loadAMRMProxyStateDuration.add(value);
    }

    public void addStoreAMRMProxyCurrentMasterKeyDuration(long value) {
        storeAMRMProxyCurrentMasterKeyStateDuration.add(value);
    }

    public void addStoreAMRMProxyAppContextEntryDuration(long value) {
        storeAMRMProxyAppContextEntryStateDuration.add(value);
    }

    public void addStoreAMRMProxyNextMasterKeyDuration(long value) {
        storeAMRMProxyNextMasterKeyStateDuration.add(value);
    }

    public void addRemoveAMRMProxyAppContextEntryDuration(long value) {
        removeAMRMProxyAppContextEntryStateDuration.add(value);
    }

    public void addRemoveAMRMProxyAppContextDuration(long value) {
        removeAMRMProxyAppContextStateDuration.add(value);
    }

    public void addStoreAssignedResourcesDuration(long value) {
        storeAssignedResourcesStateDuration.add(value);
    }

    public void addRemoveContainerTokenDuration(long value) {
        removeContainerTokenStateDuration.add(value);
    }

    public void addRemoveContainerQueuedDuration(long value) {
        removeContainerQueuedStateDuration.add(value);
    }
}