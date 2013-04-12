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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

/**
 * The server-side metrics for a journal from the JournalNode's
 * perspective.
 */
@Metrics(about="Journal metrics", context="dfs")
class JournalMetrics {
  final MetricsRegistry registry = new MetricsRegistry("JournalNode");
  
  @Metric("Number of batches written since startup")
  MutableCounterLong batchesWritten;
  
  @Metric("Number of txns written since startup")
  MutableCounterLong txnsWritten;
  
  @Metric("Number of bytes written since startup")
  MutableCounterLong bytesWritten;
  
  @Metric("Number of batches written where this node was lagging")
  MutableCounterLong batchesWrittenWhileLagging;
  
  private final int[] QUANTILE_INTERVALS = new int[] {
      1*60, // 1m
      5*60, // 5m
      60*60 // 1h
  };
  
  MutableQuantiles[] syncsQuantiles;
  
  private final Journal journal;

  JournalMetrics(Journal journal) {
    this.journal = journal;
    
    syncsQuantiles = new MutableQuantiles[QUANTILE_INTERVALS.length];
    for (int i = 0; i < syncsQuantiles.length; i++) {
      int interval = QUANTILE_INTERVALS[i];
      syncsQuantiles[i] = registry.newQuantiles(
          "syncs" + interval + "s",
          "Journal sync time", "ops", "latencyMicros", interval);
    }
  }
  
  public static JournalMetrics create(Journal j) {
    JournalMetrics m = new JournalMetrics(j);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  String getName() {
    return "Journal-" + journal.getJournalId();
  }

  @Metric("Current writer's epoch")
  public long getLastWriterEpoch() {
    try {
      return journal.getLastWriterEpoch();
    } catch (IOException e) {
      return -1L;
    }
  }
  
  @Metric("Last accepted epoch")
  public long getLastPromisedEpoch() {
    try {
      return journal.getLastPromisedEpoch();
    } catch (IOException e) {
      return -1L;
    }
  }
  
  @Metric("The highest txid stored on this JN")
  public long getLastWrittenTxId() {
    return journal.getHighestWrittenTxId();
  }
  
  @Metric("Number of transactions that this JN is lagging")
  public long getCurrentLagTxns() {
    try {
      return journal.getCurrentLagTxns();
    } catch (IOException e) {
      return -1L;
    }
  }
  
  void addSync(long us) {
    for (MutableQuantiles q : syncsQuantiles) {
      q.add(us);
    }
  }
}
