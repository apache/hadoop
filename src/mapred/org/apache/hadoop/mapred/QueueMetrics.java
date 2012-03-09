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

import java.util.ArrayList;

import org.apache.hadoop.metrics2.MetricsBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterInt;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;
import org.apache.hadoop.metrics2.lib.MetricMutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.source.JvmMetricsSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 *
 */
@SuppressWarnings("deprecation")
class QueueMetrics implements MetricsSource {

  private static final Log LOG =
    LogFactory.getLog(QueueMetrics.class);

  public static final String BUCKET_PROPERTY = 
    "mapred.queue.metrics.runtime.buckets";
  private static final String DEFAULT_BUCKETS = "60,300,1440";

  final MetricsRegistry registry = new MetricsRegistry("Queue");
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
  final MetricMutableCounterInt jobsFailed =
      registry.newCounter("jobs_failed", "", 0);
  final MetricMutableCounterInt jobsKilled =
      registry.newCounter("jobs_killed", "", 0);
  final MetricMutableGaugeInt jobsPreparing =
      registry.newGauge("jobs_preparing", "", 0);
  final MetricMutableGaugeInt jobsRunning =
      registry.newGauge("jobs_running", "", 0);
  final MetricMutableCounterInt mapsKilled =
      registry.newCounter("maps_killed", "", 0);
  final MetricMutableCounterInt redsKilled =
      registry.newCounter("reduces_killed", "", 0);
  final MetricMutableGaugeInt[] runningTime;
  TimeBucketMetrics<JobID> runBuckets;

  final String sessionId;
  private String queueName;

  public QueueMetrics(String queueName, Configuration conf) {
    this.queueName = queueName;
    sessionId = conf.get("session.id", "");
    registry.setContext("mapred").tag("sessionId", "", sessionId);
    registry.tag("Queue", "Metrics by queue", queueName);
    runningTime = buildBuckets(conf);
  }

  public String getQueueName() {
    return this.queueName;
  }

  private static ArrayList<Integer> parseInts(String value) {
    ArrayList<Integer> result = new ArrayList<Integer>();
    for(String word: value.split(",")) {
      result.add(Integer.parseInt(word.trim()));
    }
    return result;
  }

  private MetricMutableGaugeInt[] buildBuckets(Configuration conf) {
    ArrayList<Integer> buckets = 
      parseInts(conf.get(BUCKET_PROPERTY, DEFAULT_BUCKETS));
    MetricMutableGaugeInt[] result = 
      new MetricMutableGaugeInt[buckets.size() + 1];
    result[0] = registry.newGauge("running_0", "", 0);
    long[] cuts = new long[buckets.size()];
    for(int i=0; i < buckets.size(); ++i) {
      result[i+1] = registry.newGauge("running_" + buckets.get(i), "", 0);
      cuts[i] = buckets.get(i) * 1000 * 60; // covert from min to ms
    }
    this.runBuckets = new TimeBucketMetrics<JobID>(cuts);
    return result;
  }

  private void updateRunningTime() {
    int[] counts = runBuckets.getBucketCounts(System.currentTimeMillis());
    for(int i=0; i < counts.length; ++i) {
      runningTime[i].set(counts[i]); 
    }
  }

  public void getMetrics(MetricsBuilder builder, boolean all) {
    updateRunningTime();
    registry.snapshot(builder.addRecord(registry.name()), all);
  }

  public void launchMap(TaskAttemptID taskAttemptID) {
    mapsLaunched.incr();
    decWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  public void completeMap(TaskAttemptID taskAttemptID) {
    mapsCompleted.incr();
  }

  public void failedMap(TaskAttemptID taskAttemptID) {
    mapsFailed.incr();
    addWaitingMaps(taskAttemptID.getJobID(), 1);
  }

  public void launchReduce(TaskAttemptID taskAttemptID) {
    redsLaunched.incr();
    decWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  public void completeReduce(TaskAttemptID taskAttemptID) {
    redsCompleted.incr();
  }

  public void failedReduce(TaskAttemptID taskAttemptID) {
    redsFailed.incr();
    addWaitingReduces(taskAttemptID.getJobID(), 1);
  }

  public void submitJob(JobConf conf, JobID id) {
    jobsSubmitted.incr();
  }

  public void completeJob(JobConf conf, JobID id) {
    jobsCompleted.incr();
  }

  public void addWaitingMaps(JobID id, int task) {
    waitingMaps.incr(task);
  }

  public void decWaitingMaps(JobID id, int task) {
    waitingMaps.decr(task);
  }

  public void addWaitingReduces(JobID id, int task) {
    waitingReds.incr(task);
  }

  public void decWaitingReduces(JobID id, int task){
    waitingReds.decr(task);
  }

  public void addReservedMapSlots(int slots) {
    reservedMapSlots.incr(slots);;
  }

  public void decReservedMapSlots(int slots) {
    reservedMapSlots.decr(slots);
  }

  public void addReservedReduceSlots(int slots) {
    reservedRedSlots.incr(slots);
  }

  public void decReservedReduceSlots(int slots) {
    reservedRedSlots.decr(slots);
  }

  public void failedJob(JobConf conf, JobID id) {
    jobsFailed.incr();
  }

  public void killedJob(JobConf conf, JobID id) {
    jobsKilled.incr();
  }

  public void addPrepJob(JobConf conf, JobID id) {
    jobsPreparing.incr();
  }

  public void decPrepJob(JobConf conf, JobID id) {
    jobsPreparing.decr();
  }

  public void addRunningJob(JobConf conf, JobID id) {
    jobsRunning.incr();
    runBuckets.add(id, System.currentTimeMillis());
  }

  public void decRunningJob(JobConf conf, JobID id) {
    jobsRunning.decr();
    runBuckets.remove(id);
  }

  public void killedMap(TaskAttemptID taskAttemptID) {
    mapsKilled.incr();
  }

  public void killedReduce(TaskAttemptID taskAttemptID) {
    redsKilled.incr();
  }

  static QueueMetrics create(String queueName, Configuration conf) {
    return create(queueName, conf, DefaultMetricsSystem.INSTANCE);
  }

  static QueueMetrics create(String queueName, Configuration conf,
                                     MetricsSystem ms) {
    return ms.register("QueueMetrics,q=" + queueName, "Queue metrics",
                       new QueueMetrics(queueName, conf));
  }

}
