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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;


/**
 * 
 * This class is for maintaining  the various DataNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #blocksRead}.inc()
 *
 */
public class DataNodeMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  private DataNodeStatistics datanodeStats;
  
  
  public MetricsLongValue bytesWritten = 
                      new MetricsLongValue("bytes_written");
  public MetricsLongValue bytesRead = 
                      new MetricsLongValue("bytes_read");
  public MetricsTimeVaryingInt blocksWritten = 
                      new MetricsTimeVaryingInt("blocks_written");
  public MetricsTimeVaryingInt blocksRead = 
                      new MetricsTimeVaryingInt("blocks_read");
  public MetricsTimeVaryingInt blocksReplicated =
                      new MetricsTimeVaryingInt("blocks_replicated");
  public MetricsTimeVaryingInt blocksRemoved =
                       new MetricsTimeVaryingInt("blocks_removed");
  public MetricsTimeVaryingInt blocksVerified = 
                        new MetricsTimeVaryingInt("blocks_verified");
  public MetricsTimeVaryingInt blockVerificationFailures =
                       new MetricsTimeVaryingInt("block_verification_failures");
  
  public MetricsTimeVaryingInt readsFromLocalClient = 
                new MetricsTimeVaryingInt("reads_from_local_client");
  public MetricsTimeVaryingInt readsFromRemoteClient = 
                new MetricsTimeVaryingInt("reads_from_remote_client");
  public MetricsTimeVaryingInt writesFromLocalClient = 
              new MetricsTimeVaryingInt("writes_from_local_client");
  public MetricsTimeVaryingInt writesFromRemoteClient = 
              new MetricsTimeVaryingInt("writes_from_remote_client");
  
  public MetricsTimeVaryingRate readBlockOp = 
                new MetricsTimeVaryingRate("readBlockOp");
  public MetricsTimeVaryingRate writeBlockOp = 
                new MetricsTimeVaryingRate("writeBlockOp");
  public MetricsTimeVaryingRate readMetadataOp = 
                new MetricsTimeVaryingRate("readMetadataOp");
  public MetricsTimeVaryingRate blockChecksumOp = 
                new MetricsTimeVaryingRate("blockChecksumOp");
  public MetricsTimeVaryingRate copyBlockOp = 
                new MetricsTimeVaryingRate("copyBlockOp");
  public MetricsTimeVaryingRate replaceBlockOp = 
                new MetricsTimeVaryingRate("replaceBlockOp");
  public MetricsTimeVaryingRate heartbeats = 
                    new MetricsTimeVaryingRate("heartBeats");
  public MetricsTimeVaryingRate blockReports = 
                    new MetricsTimeVaryingRate("blockReports");

    
  public DataNodeMetrics(Configuration conf, String storageId) {
    String sessionId = conf.get("session.id"); 
    // Initiate reporting of Java VM metrics
    JvmMetrics.init("DataNode", sessionId);
    

    // Now the MBean for the data node
    datanodeStats = new DataNodeStatistics(this, storageId);
    
    // Create record for DataNode metrics
    MetricsContext context = MetricsUtil.getContext("dfs");
    metricsRecord = MetricsUtil.createRecord(context, "datanode");
    metricsRecord.setTag("sessionId", sessionId);
    context.registerUpdater(this);
  }
  
  public void shutdown() {
    if (datanodeStats != null) 
      datanodeStats.shutdown();
  }
    
  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
            
      bytesWritten.pushMetric(metricsRecord);
      bytesRead.pushMetric(metricsRecord);
      blocksWritten.pushMetric(metricsRecord);
      blocksRead.pushMetric(metricsRecord);
      blocksReplicated.pushMetric(metricsRecord);
      blocksRemoved.pushMetric(metricsRecord);
      blocksVerified.pushMetric(metricsRecord);
      blockVerificationFailures.pushMetric(metricsRecord);
      readsFromLocalClient.pushMetric(metricsRecord);
      writesFromLocalClient.pushMetric(metricsRecord);
      readsFromRemoteClient.pushMetric(metricsRecord);
      writesFromRemoteClient.pushMetric(metricsRecord);
      
      readBlockOp.pushMetric(metricsRecord);
      writeBlockOp.pushMetric(metricsRecord);
      readMetadataOp.pushMetric(metricsRecord);
      blockChecksumOp.pushMetric(metricsRecord);
      copyBlockOp.pushMetric(metricsRecord);
      replaceBlockOp.pushMetric(metricsRecord);
      heartbeats.pushMetric(metricsRecord);
      blockReports.pushMetric(metricsRecord);
    }
    metricsRecord.update();
  }
  public void resetAllMinMax() {
    readBlockOp.resetMinMax();
    writeBlockOp.resetMinMax();
    readMetadataOp.resetMinMax();
    blockChecksumOp.resetMinMax();
    copyBlockOp.resetMinMax();
    replaceBlockOp.resetMinMax();
    heartbeats.resetMinMax();
    blockReports.resetMinMax();
  }
}
