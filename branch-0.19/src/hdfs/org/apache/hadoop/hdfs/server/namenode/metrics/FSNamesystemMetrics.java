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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.util.MetricsIntValue;

/**
 * 
 * This class is for maintaining  the various FSNamesystem status metrics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #filesTotal}.set()
 *
 */
public class FSNamesystemMetrics implements Updater {
  private static Log log = LogFactory.getLog(FSNamesystemMetrics.class);
  private final MetricsRecord metricsRecord;
   
  public MetricsIntValue filesTotal = new MetricsIntValue("FilesTotal");
  public MetricsIntValue blocksTotal = new MetricsIntValue("BlocksTotal");
  public MetricsIntValue capacityTotalGB = new MetricsIntValue("CapacityTotalGB");
  public MetricsIntValue capacityUsedGB = new MetricsIntValue("CapacityUsedGB");
  public MetricsIntValue capacityRemainingGB = new MetricsIntValue("CapacityRemainingGB");
  public MetricsIntValue totalLoad = new MetricsIntValue("TotalLoad");
  public MetricsIntValue pendingReplicationBlocks = new MetricsIntValue("PendingReplicationBlocks");
  public MetricsIntValue underReplicatedBlocks = new MetricsIntValue("UnderReplicatedBlocks");
  public MetricsIntValue scheduledReplicationBlocks = new MetricsIntValue("ScheduledReplicationBlocks");
  public FSNamesystemMetrics(Configuration conf) {
    String sessionId = conf.get("session.id");
     
    // Create a record for FSNamesystem metrics
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "FSNamesystem");
    metricsRecord.setTag("sessionId", sessionId);
    metricsContext.registerUpdater(this);
    log.info("Initializing FSNamesystemMetrics using context object:" +
              metricsContext.getClass().getName());
  }

  private int roundBytesToGBytes(long bytes) {
    return Math.round(((float)bytes/(1024 * 1024 * 1024)));
  }
      
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * We set the metrics value within  this function before pushing it out. 
   * FSNamesystem updates its own local variables which are
   * light weight compared to Metrics counters. 
   *
   * Some of the metrics are explicity casted to int. Few metrics collectors
   * do not handle long values. It is safe to cast to int for now as all these
   * values fit in int value.
   * Metrics related to DFS capacity are stored in bytes which do not fit in 
   * int, so they are rounded to GB
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      FSNamesystem fsNameSystem = FSNamesystem.getFSNamesystem();
      filesTotal.set((int)fsNameSystem.getFilesTotal());
      filesTotal.pushMetric(metricsRecord);

      blocksTotal.set((int)fsNameSystem.getBlocksTotal());
      blocksTotal.pushMetric(metricsRecord);
      
      capacityTotalGB.set(roundBytesToGBytes(fsNameSystem.getCapacityTotal()));
      capacityTotalGB.pushMetric(metricsRecord);
      
      capacityUsedGB.set(roundBytesToGBytes(fsNameSystem.getCapacityUsed()));
      capacityUsedGB.pushMetric(metricsRecord);
      
      capacityRemainingGB.set(roundBytesToGBytes(fsNameSystem.
                                               getCapacityRemaining()));
      capacityRemainingGB.pushMetric(metricsRecord);
      
      totalLoad.set(fsNameSystem.getTotalLoad());
      totalLoad.pushMetric(metricsRecord);
      
      pendingReplicationBlocks.set((int)fsNameSystem.
                                   getPendingReplicationBlocks());
      pendingReplicationBlocks.pushMetric(metricsRecord);

      underReplicatedBlocks.set((int)fsNameSystem.getUnderReplicatedBlocks());
      underReplicatedBlocks.pushMetric(metricsRecord);

      scheduledReplicationBlocks.set((int)fsNameSystem.
                                      getScheduledReplicationBlocks());
      scheduledReplicationBlocks.pushMetric(metricsRecord);
    }
    metricsRecord.update();
  }
}
