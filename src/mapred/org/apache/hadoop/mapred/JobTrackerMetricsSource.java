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

package org.apache.hadoop.mapred;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;

/**
 *
 */
@SuppressWarnings("deprecation")
class JobTrackerMetricsSource extends JobTrackerInstrumentation
                          implements MetricsSource {

  final MetricsRegistry registry = new MetricsRegistry("jobtracker");
  final MetricMutableGaugeInt mapSlots =
      registry.newGauge("map_slots", "", 0);
  final MetricMutableGaugeInt redSlots =
      registry.newGauge("reduce_slots", "", 0);
  final MetricMutableGaugeInt blMapSlots =
      registry.newGauge("blacklisted_maps", "", 0);
  final MetricMutableGaugeInt blRedSlots =
      registry.newGauge("blacklisted_reduces", "", 0);
  final MetricMutableCounterInt mapsLaunched =
      registry.newCounter("maps_launched", "", 0);
  final MetricMutableCounterInt mapsCompleted =
      registry.newCounter("maps_completed", "", 0);
  final MetricMutableCounterInt mapsFailed =
      registry.newCounter("maps_failed", "", 0);
  final MetricMutableCounterInt redsLaunched =
      registry.newCounter("reduces_launched", "", 0);
  final MetricMutableCounterInt redsCompleted =
      registry.newCounter("reduces_completed", "", 0);
  final MetricMutableCounterInt redsFailed =
      registry.newCounter("reduces_failed", "", 0);
  final MetricMutableCounterInt jobsSubmitted =
      registry.newCounter("jobs_submitted", "", 0);
  final MetricMutableCounterInt jobsCompleted =
      registry.newCounter("jobs_completed", "", 0);
  final MetricMutableGaugeInt waitingMaps =
      registry.newGauge("waiting_maps", "", 0);
  final MetricMutableGaugeInt waitingReds =
      registry.newGauge("waiting_reduces", "", 0);
  final MetricMutableGaugeInt reservedMapSlots =
      registry.newGauge("reserved_map_slots", "", 0);
  final MetricMutableGaugeInt reservedRedSlots =
      registry.newGauge("reserved_reduce_slots", "", 0);
  final MetricMutableGaugeInt occupiedMapSlots =
      registry.newGauge("occupied_map_slots", "", 0);
  final MetricMutableGaugeInt occupiedRedSlots =
      registry.newGauge("occupied_reduce_slots", "", 0);
  final MetricMutableCounterInt jobsFailed =
      registry.newCounter("jobs_failed", "", 0);
  final MetricMutableCounterInt jobsKilled =
      registry.newCounter("jobs_killed", "", 0);
  final MetricMutableGaugeInt jobsPreparing =
      registry.newGauge("jobs_preparing", "", 0);
  final MetricMutableGaugeInt jobsRunning =
      registry.newGauge("jobs_running", "", 0);
  final MetricMutableGaugeInt runningMaps =
      registry.newGauge("running_maps", "", 0);
  final MetricMutableGaugeInt runningReds =
      registry.newGauge("running_reduces", "", 0);
  final MetricMutableCounterInt mapsKilled =
      registry.newCounter("maps_killed", "", 0);
  final MetricMutableCounterInt redsKilled =
      registry.newCounter("reduces_killed", "", 0);
  final MetricMutableGaugeInt numTrackers =
      registry.newGauge("trackers", "", 0);
  final MetricMutableGaugeInt blacklistedTrackers =
      registry.newGauge("trackers_blacklisted", "", 0);
  final MetricMutableGaugeInt graylistedTrackers =
      registry.newGauge("trackers_graylisted", "", 0);
  final MetricMutableGaugeInt decTrackers =
      registry.newGauge("trackers_decommissioned", "", 0);
  final MetricMutableCounterLong numHeartbeats =
      registry.newCounter("heartbeats", "", 0L);

  final String sessionId;

  public JobTrackerMetricsSource(JobTracker jt, JobConf conf) {
    super(jt, conf);
    sessionId = conf.getSessionId();
    registry.setContext("mapred").tag("sessionId", "", sessionId);
    JvmMetricsSource.create("JobTracker", sessionId);
  }

  public void getMetrics(MetricsBuilder builder, boolean all) {
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  @Override
  public void launchMap(TaskAttemptID taskAttemptID) {
    mapsLaunched.incr();
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void completeMap(TaskAttemptID taskAttemptID) {
    mapsCompleted.incr();
  }

  @Override
  public void failedMap(TaskAttemptID taskAttemptID) {
    mapsFailed.incr();
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void launchReduce(TaskAttemptID taskAttemptID) {
    redsLaunched.incr();
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void completeReduce(TaskAttemptID taskAttemptID) {
    redsCompleted.incr();
  }

  @Override
  public void failedReduce(TaskAttemptID taskAttemptID) {
    redsFailed.incr();
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public void submitJob(JobConf conf, JobID id) {
    jobsSubmitted.incr();
  }

  @Override
  public void completeJob(JobConf conf, JobID id) {
    jobsCompleted.incr();
  }

  @Override
  public void addWaitingMaps(JobID id, int task) {
    waitingMaps.incr(task);
  }

  @Override
  public void decWaitingMaps(JobID id, int task) {
    waitingMaps.decr(task);
  }

  @Override
  public void addWaitingReduces(JobID id, int task) {
    waitingReds.incr(task);
  }

  @Override
  public void decWaitingReduces(JobID id, int task){
    waitingReds.decr(task);
  }

  @Override
  public void setMapSlots(int slots) {
    mapSlots.set(slots);
  }

  @Override
  public void setReduceSlots(int slots) {
    redSlots.set(slots);
  }

  @Override
  public void addBlackListedMapSlots(int slots){
    blMapSlots.incr(slots);
  }

  @Override
  public void decBlackListedMapSlots(int slots){
    blMapSlots.decr(slots);
  }

  @Override
  public void addBlackListedReduceSlots(int slots){
    blRedSlots.incr(slots);
  }

  @Override
  public void decBlackListedReduceSlots(int slots){
    blRedSlots.decr(slots);
  }

  @Override
  public void addReservedMapSlots(int slots) {
    reservedMapSlots.incr(slots);;
  }

  @Override
  public void decReservedMapSlots(int slots) {
    reservedMapSlots.decr(slots);
  }

  @Override
  public void addReservedReduceSlots(int slots) {
    reservedRedSlots.incr(slots);
  }

  @Override
  public void decReservedReduceSlots(int slots) {
    reservedRedSlots.decr(slots);
  }

  @Override
  public void addOccupiedMapSlots(int slots) {
    occupiedMapSlots.incr(slots);
  }

  @Override
  public void decOccupiedMapSlots(int slots) {
    occupiedMapSlots.decr(slots);
  }

  @Override
  public void addOccupiedReduceSlots(int slots) {
    occupiedRedSlots.incr(slots);
  }

  @Override
  public void decOccupiedReduceSlots(int slots) {
    occupiedRedSlots.decr(slots);
  }

  @Override
  public void failedJob(JobConf conf, JobID id) {
    jobsFailed.incr();
  }

  @Override
  public void killedJob(JobConf conf, JobID id) {
    jobsKilled.incr();
  }

  @Override
  public void addPrepJob(JobConf conf, JobID id) {
    jobsPreparing.incr();
  }

  @Override
  public void decPrepJob(JobConf conf, JobID id) {
    jobsPreparing.decr();
  }

  @Override
  public void addRunningJob(JobConf conf, JobID id) {
    jobsRunning.incr();
  }

  @Override
  public void decRunningJob(JobConf conf, JobID id) {
    jobsRunning.decr();
  }

  @Override
  public void addRunningMaps(int task) {
    runningMaps.incr(task);
  }

  @Override
  public void decRunningMaps(int task) {
    runningMaps.decr(task);
  }

  @Override
  public void addRunningReduces(int task) {
    runningReds.incr(task);
  }

  @Override
  public void decRunningReduces(int task) {
    runningReds.decr(task);
  }

  @Override
  public void killedMap(TaskAttemptID taskAttemptID) {
    mapsKilled.incr();
  }

  @Override
  public void killedReduce(TaskAttemptID taskAttemptID) {
    redsKilled.incr();
  }

  @Override
  public void addTrackers(int trackers) {
    numTrackers.incr(trackers);
  }

  @Override
  public void decTrackers(int trackers) {
    numTrackers.decr(trackers);
  }

  @Override
  public void addBlackListedTrackers(int trackers) {
    blacklistedTrackers.incr(trackers);
  }

  @Override
  public void decBlackListedTrackers(int trackers) {
    blacklistedTrackers.decr(trackers);
  }

  @Override
  public void addGrayListedTrackers(int trackers) {
    graylistedTrackers.incr(trackers);
  }

  @Override
  public void decGrayListedTrackers(int trackers) {
    graylistedTrackers.decr(trackers);
  }

  @Override
  public void setDecommissionedTrackers(int trackers) {
    decTrackers.set(trackers);
  }

  @Override
  public void heartbeat() {
    numHeartbeats.incr();
  }
}
