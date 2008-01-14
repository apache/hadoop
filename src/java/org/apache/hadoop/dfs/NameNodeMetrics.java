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
import org.apache.hadoop.dfs.namenode.metrics.NameNodeMgt;
import org.apache.hadoop.metrics.*;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * 
 * This class is for maintaining  the various NameNode statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 *  for example:
 *  <p> {@link #syncs}.inc()
 *
 */
public class NameNodeMetrics implements Updater {
    private static Log log = LogFactory.getLog(NameNodeMetrics.class);
    private final MetricsRecord metricsRecord;
    
    public MetricsTimeVaryingInt numFilesCreated = new MetricsTimeVaryingInt("FilesCreated");
    public MetricsTimeVaryingInt numFilesOpened = new MetricsTimeVaryingInt("FilesOpened");
    public MetricsTimeVaryingInt numFilesRenamed = new MetricsTimeVaryingInt("FilesRenamed");
    public MetricsTimeVaryingInt numFilesListed = new MetricsTimeVaryingInt("FilesListed");

    
    public MetricsTimeVaryingRate transactions = new MetricsTimeVaryingRate("Transactions");
    public MetricsTimeVaryingRate syncs = new MetricsTimeVaryingRate("Syncs");
    public MetricsTimeVaryingRate blockReport = new MetricsTimeVaryingRate("blockReport");
    public MetricsIntValue safeModeTime = new MetricsIntValue("SafemodeTime");
    public MetricsIntValue fsImageLoadTime = 
                                        new MetricsIntValue("fsImageLoadTime");

      
    NameNodeMetrics(Configuration conf, NameNode nameNode) {
      String sessionId = conf.get("session.id");
      // Initiate Java VM metrics
      JvmMetrics.init("NameNode", sessionId);

      
      // Now the Mbean for the name node
      new NameNodeMgt(sessionId, this, nameNode);
      
      // Create a record for NameNode metrics
      MetricsContext metricsContext = MetricsUtil.getContext("dfs");
      metricsRecord = MetricsUtil.createRecord(metricsContext, "namenode");
      metricsRecord.setTag("sessionId", sessionId);
      metricsContext.registerUpdater(this);
      log.info("Initializing NameNodeMeterics using context object:" +
                metricsContext.getClass().getName());
    }
      
    /**
     * Since this object is a registered updater, this method will be called
     * periodically, e.g. every 5 seconds.
     */
    public void doUpdates(MetricsContext unused) {
      synchronized (this) {
        numFilesCreated.pushMetric(metricsRecord);
        numFilesOpened.pushMetric(metricsRecord);
        numFilesRenamed.pushMetric(metricsRecord);
        numFilesListed.pushMetric(metricsRecord);


        transactions.pushMetric(metricsRecord);
        syncs.pushMetric(metricsRecord);
        blockReport.pushMetric(metricsRecord);
        safeModeTime.pushMetric(metricsRecord);
        fsImageLoadTime.pushMetric(metricsRecord);
      }
      metricsRecord.update();
    }
      
    void createFile() {
      numFilesCreated.inc();
    }
      
    void openFile() {
      numFilesOpened.inc();
    }
      
    void renameFile() {
      numFilesRenamed.inc();
    }
      
    void listFile(int nfiles) {
      numFilesListed.inc(nfiles);
    }

    void incrNumTransactions(int count, int time) {
      transactions.inc(count, time);
    }

    void incrSyncs(int count, int time) {
      syncs.inc(count, time);
    }
}
