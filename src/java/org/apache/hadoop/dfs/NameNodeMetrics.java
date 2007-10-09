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
package org.apache.hadoop.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

class NameNodeMetrics implements Updater {
    private final MetricsRecord metricsRecord;
    
    private int numFilesCreated = 0;
    private int numFilesOpened = 0;
    private int numFilesRenamed = 0;
    private int numFilesListed = 0;

    private int numTransactions = 0;
    private int totalTimeTransactionsLogMemory = 0;
    private int numSyncs = 0;
    private int totalTimeSyncs = 0;
      
    NameNodeMetrics(Configuration conf) {
      String sessionId = conf.get("session.id");
      // Initiate Java VM metrics
      JvmMetrics.init("NameNode", sessionId);
      // Create a record for NameNode metrics
      MetricsContext metricsContext = MetricsUtil.getContext("dfs");
      metricsRecord = MetricsUtil.createRecord(metricsContext, "namenode");
      metricsRecord.setTag("sessionId", sessionId);
      metricsContext.registerUpdater(this);
    }
      
    /**
     * Since this object is a registered updater, this method will be called
     * periodically, e.g. every 5 seconds.
     */
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        metricsRecord.incrMetric("files_created", numFilesCreated);
        metricsRecord.incrMetric("files_opened", numFilesOpened);
        metricsRecord.incrMetric("files_renamed", numFilesRenamed);
        metricsRecord.incrMetric("files_listed", numFilesListed);
        metricsRecord.incrMetric("num_transactions", numTransactions);
        metricsRecord.incrMetric("avg_time_transactions_memory", 
                                 getAverageTimeTransaction());
        metricsRecord.incrMetric("num_syncs", numSyncs);
        metricsRecord.incrMetric("avg_time_transactions_sync", 
                                 getAverageTimeSync());
              
        numFilesCreated = 0;
        numFilesOpened = 0;
        numFilesRenamed = 0;
        numFilesListed = 0;
        numTransactions = 0;
        totalTimeTransactionsLogMemory = 0;
        numSyncs = 0;
        totalTimeSyncs = 0;
      }
      metricsRecord.update();
    }
      
    synchronized void createFile() {
      ++numFilesCreated;
    }
      
    synchronized void openFile() {
      ++numFilesOpened;
    }
      
    synchronized void renameFile() {
      ++numFilesRenamed;
    }
      
    synchronized void listFile(int nfiles) {
      numFilesListed += nfiles;
    }

    synchronized void incrNumTransactions(int count, int time) {
      numTransactions += count;
      totalTimeTransactionsLogMemory += time;
    }

    synchronized void incrSyncs(int count, int time) {
      numSyncs += count;
      totalTimeSyncs += time;
    }

    synchronized private int getAverageTimeTransaction() {
      if (numTransactions == 0) {
        return 0;
      }
      return totalTimeTransactionsLogMemory/numTransactions;
    }

    synchronized private int getAverageTimeSync() {
      if (numSyncs == 0) {
        return 0;
      }
      return totalTimeSyncs/numSyncs;
    }
}
