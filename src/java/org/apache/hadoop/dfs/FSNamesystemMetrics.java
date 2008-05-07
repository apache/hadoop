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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

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
  private FSNamesystem fsNameSystem;
   
  public MetricsLongValue filesTotal = new MetricsLongValue("FilesTotal");
  public MetricsLongValue blocksTotal = new MetricsLongValue("BlocksTotal");
  public MetricsLongValue capacityTotal = new MetricsLongValue("CapacityTotal");
  public MetricsLongValue capacityUsed = new MetricsLongValue("CapacityUsed");
  public MetricsLongValue capacityRemaining = new MetricsLongValue("CapacityRemaining");
  public MetricsIntValue totalLoad = new MetricsIntValue("TotalLoad");
  public MetricsLongValue pendingReplicationBlocks = new MetricsLongValue("PendingReplicationBlocks");
  public MetricsLongValue underReplicatedBlocks = new MetricsLongValue("UnderReplicatedBlocks");
  public MetricsLongValue scheduledReplicationBlocks = new MetricsLongValue("ScheduledReplicationBlocks");
  FSNamesystemMetrics(Configuration conf, FSNamesystem fsNameSystem) {
    String sessionId = conf.get("session.id");
    this.fsNameSystem = fsNameSystem;
     
    // Create a record for FSNamesystem metrics
    MetricsContext metricsContext = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(metricsContext, "FSNamesystem");
    metricsRecord.setTag("sessionId", sessionId);
    metricsContext.registerUpdater(this);
    log.info("Initializing FSNamesystemMeterics using context object:" +
              metricsContext.getClass().getName());
  }
  public void shutdown() {
    if (fsNameSystem != null) 
      fsNameSystem.shutdown();
  }
      
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * We set the metrics value within  this function before pushing it out. 
   * FSNamesystem updates its own local variables which are
   * light weight compared to Metrics counters. 
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      filesTotal.set(fsNameSystem.getFilesTotal());
      filesTotal.pushMetric(metricsRecord);

      blocksTotal.set(fsNameSystem.getBlocksTotal());
      blocksTotal.pushMetric(metricsRecord);
      
      capacityTotal.set(fsNameSystem.getCapacityTotal());
      capacityTotal.pushMetric(metricsRecord);
      
      capacityUsed.set(fsNameSystem.getCapacityUsed());
      capacityUsed.pushMetric(metricsRecord);
      
      capacityRemaining.set(fsNameSystem.getCapacityRemaining());
      capacityRemaining.pushMetric(metricsRecord);
      
      totalLoad.set(fsNameSystem.getTotalLoad());
      totalLoad.pushMetric(metricsRecord);
      
      pendingReplicationBlocks.set(fsNameSystem.getPendingReplicationBlocks());
      pendingReplicationBlocks.pushMetric(metricsRecord);

      underReplicatedBlocks.set(fsNameSystem.getUnderReplicatedBlocks());
      underReplicatedBlocks.pushMetric(metricsRecord);

      scheduledReplicationBlocks.set(fsNameSystem.
                                      getScheduledReplicationBlocks());
      scheduledReplicationBlocks.pushMetric(metricsRecord);
    }
    metricsRecord.update();
  }
}
