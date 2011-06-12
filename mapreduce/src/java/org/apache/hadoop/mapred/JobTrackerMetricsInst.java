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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import static org.apache.hadoop.metrics2.impl.MsInfo.*;

@Metrics(name="JobTrackerMetrics", context="mapred")
class JobTrackerMetricsInst extends JobTrackerInstrumentation {

  // need the registry for record name (different from metrics name) and tags
  final MetricsRegistry registry = new MetricsRegistry("jobtracker");

  @Metric MutableCounterInt mapsLaunched;
  @Metric MutableCounterInt mapsCompleted;
  @Metric MutableCounterInt mapsFailed;
  @Metric MutableCounterInt mapsKilled;
  @Metric MutableCounterInt reducesLaunched;
  @Metric MutableCounterInt reducesCompleted;
  @Metric MutableCounterInt reducesFailed;
  @Metric MutableCounterInt reducesKilled;

  @Metric MutableCounterInt jobsSubmitted;
  @Metric MutableCounterInt jobsCompleted;
  @Metric MutableCounterInt jobsFailed;
  @Metric MutableCounterInt jobsKilled;
  @Metric MutableGaugeInt jobsPreparing;
  @Metric MutableGaugeInt jobsRunning;

  @Metric MutableGaugeInt waitingMaps;
  @Metric MutableGaugeInt waitingReduces;
  @Metric MutableGaugeInt runningMaps;
  @Metric MutableGaugeInt runningReduces;
  @Metric MutableCounterInt speculativeMaps;
  @Metric MutableCounterInt speculativeReduces;
  @Metric MutableCounterInt dataLocalMaps;
  @Metric MutableCounterInt rackLocalMaps;

  //Cluster status fields.
  @Metric MutableGaugeInt mapSlots;
  @Metric MutableGaugeInt reduceSlots;
  @Metric MutableGaugeInt blackListedMapSlots;
  @Metric MutableGaugeInt blackListedReduceSlots;
  @Metric MutableGaugeInt reservedMapSlots;
  @Metric MutableGaugeInt reservedReduceSlots;
  @Metric MutableGaugeInt occupiedMapSlots;
  @Metric MutableGaugeInt occupiedReduceSlots;

  @Metric MutableGaugeInt trackers;
  @Metric MutableGaugeInt trackersBlackListed;
  @Metric MutableGaugeInt trackersDecommissioned;

  @Metric MutableCounterLong heartbeats;
  
  public JobTrackerMetricsInst(JobTracker tracker, JobConf conf) {
    super(tracker, conf);
    String sessionId = conf.getSessionId();
    // Ideally we should the registering in an init method.
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics.create("JobTracker", sessionId, ms);
    registry.tag(SessionId, sessionId);
    ms.register(this);
  }

  @Override
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    mapsLaunched.incr();
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void launchDataLocalMap(TaskAttemptID taskAttemptID) {
    dataLocalMaps.incr();
  }

  @Override
  public synchronized void launchRackLocalMap(TaskAttemptID taskAttemptID) {
    rackLocalMaps.incr();
  }

  @Override
  public synchronized void completeMap(TaskAttemptID taskAttemptID) {
    mapsCompleted.incr();
  }

  @Override
  public synchronized void failedMap(TaskAttemptID taskAttemptID) {
    mapsFailed.incr();
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void speculateMap(TaskAttemptID taskAttemptID) {
    speculativeMaps.incr();
  }

  @Override
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    reducesLaunched.incr();
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void completeReduce(TaskAttemptID taskAttemptID) {
    reducesCompleted.incr();
  }

  @Override
  public synchronized void failedReduce(TaskAttemptID taskAttemptID) {
    reducesFailed.incr();
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  @Override
  public synchronized void speculateReduce(TaskAttemptID taskAttemptID) {
    speculativeReduces.incr();
  }

  @Override
  public synchronized void submitJob(JobConf conf, JobID id) {
    jobsSubmitted.incr();
  }

  @Override
  public synchronized void completeJob(JobConf conf, JobID id) {
    jobsCompleted.incr();
  }

  @Override
  public synchronized void addWaitingMaps(JobID id, int task) {
    waitingMaps.incr(task);
  }
  
  @Override
  public synchronized void decWaitingMaps(JobID id, int task) {
    waitingMaps.decr(task);
  }
  
  @Override
  public synchronized void addWaitingReduces(JobID id, int task) {
    waitingReduces.incr(task);
  }
  
  @Override
  public synchronized void decWaitingReduces(JobID id, int task){
    waitingReduces.decr(task);
  }

  @Override
  public synchronized void setMapSlots(int slots) {
    mapSlots.set(slots);
  }

  @Override
  public synchronized void setReduceSlots(int slots) {
    reduceSlots.set(slots);
  }

  @Override
  public synchronized void addBlackListedMapSlots(int slots) {
    blackListedMapSlots.incr(slots);
  }

  @Override
  public synchronized void decBlackListedMapSlots(int slots) {
    blackListedMapSlots.decr(slots);
  }

  @Override
  public synchronized void addBlackListedReduceSlots(int slots) {
    blackListedReduceSlots.incr(slots);
  }

  @Override
  public synchronized void decBlackListedReduceSlots(int slots) {
    blackListedReduceSlots.decr(slots);
  }

  @Override
  public synchronized void addReservedMapSlots(int slots) {
    reservedMapSlots.incr(slots);
  }

  @Override
  public synchronized void decReservedMapSlots(int slots) {
    reservedMapSlots.decr(slots);
  }

  @Override
  public synchronized void addReservedReduceSlots(int slots) {
    reservedReduceSlots.incr(slots);
  }

  @Override
  public synchronized void decReservedReduceSlots(int slots) {
    reservedReduceSlots.decr(slots);
  }

  @Override
  public synchronized void addOccupiedMapSlots(int slots) {
    occupiedMapSlots.incr(slots);
  }

  @Override
  public synchronized void decOccupiedMapSlots(int slots) {
    occupiedMapSlots.decr(slots);
  }

  @Override
  public synchronized void addOccupiedReduceSlots(int slots) {
    occupiedReduceSlots.incr(slots);
  }

  @Override
  public synchronized void decOccupiedReduceSlots(int slots) {
    occupiedReduceSlots.decr(slots);
  }

  @Override
  public synchronized void failedJob(JobConf conf, JobID id) {
    jobsFailed.incr();
  }

  @Override
  public synchronized void killedJob(JobConf conf, JobID id) {
    jobsKilled.incr();
  }

  @Override
  public synchronized void addPrepJob(JobConf conf, JobID id) {
    jobsPreparing.incr();
  }

  @Override
  public synchronized void decPrepJob(JobConf conf, JobID id) {
    jobsPreparing.decr();
  }

  @Override
  public synchronized void addRunningJob(JobConf conf, JobID id) {
    jobsRunning.incr();
  }

  @Override
  public synchronized void decRunningJob(JobConf conf, JobID id) {
    jobsRunning.decr();
  }

  @Override
  public synchronized void addRunningMaps(int task) {
    runningMaps.incr(task);
  }

  @Override
  public synchronized void decRunningMaps(int task) {
    runningMaps.decr(task);
  }

  @Override
  public synchronized void addRunningReduces(int task) {
    runningReduces.incr(task);
  }

  @Override
  public synchronized void decRunningReduces(int task) {
    runningReduces.decr(task);
  }

  @Override
  public synchronized void killedMap(TaskAttemptID taskAttemptID) {
    mapsKilled.incr();
  }

  @Override
  public synchronized void killedReduce(TaskAttemptID taskAttemptID) {
    reducesKilled.incr();
  }

  @Override
  public synchronized void addTrackers(int trackers) {
    this.trackers.incr(trackers);
  }

  @Override
  public synchronized void decTrackers(int trackers) {
    this.trackers.decr(trackers);
  }

  @Override
  public synchronized void addBlackListedTrackers(int trackers) {
    trackersBlackListed.incr(trackers);
  }

  @Override
  public synchronized void decBlackListedTrackers(int trackers) {
    trackersBlackListed.decr(trackers);
  }

  @Override
  public synchronized void setDecommissionedTrackers(int trackers) {
    trackersDecommissioned.set(trackers);
  }  

  @Override
  public synchronized void heartbeat() {
    heartbeats.incr();
  }
}
