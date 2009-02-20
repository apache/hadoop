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

import javax.management.ObjectName;

import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.metrics.util.MBeanUtil;

/**
 * 
 * This is the implementation of the Name Node JMX MBean
 *
 */
public class NameNodeStatistics implements NameNodeStatisticsMBean {
  private NameNodeMetrics myMetrics;
  private ObjectName mbeanName;

  /**
   * This constructs and registers the NameNodeStatisticsMBean
   * @param nameNodeMetrics - the metrics from which the mbean gets its info
   */
  public NameNodeStatistics(NameNodeMetrics nameNodeMetrics) {
    myMetrics = nameNodeMetrics;
    mbeanName = MBeanUtil.registerMBean("NameNode", "NameNodeStatistics", this);
  }
  
  /**
   * Shuts down the statistics
   *   - unregisters the mbean
   */
  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

  /**
   * @inheritDoc
   */
  public long  getBlockReportAverageTime() {
    return myMetrics.blockReport.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getBlockReportMaxTime() {
    return myMetrics.blockReport.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getBlockReportMinTime() {
    return myMetrics.blockReport.getMinTime();
  }
 
  /**
   * @inheritDoc
   */
  public int getBlockReportNum() {
    return myMetrics.blockReport.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long  getJournalTransactionAverageTime() {
    return myMetrics.transactions.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public int getJournalTransactionNum() {
    return myMetrics.transactions.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getJournalTransactionMaxTime() {
    return myMetrics.transactions.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getJournalTransactionMinTime() {
    return myMetrics.transactions.getMinTime();
  }

  /**
   * @inheritDoc
   */  
  public long getJournalSyncAverageTime() {
    return myMetrics.syncs.getPreviousIntervalAverageTime();
  }
 
  /**
   * @inheritDoc
   */
  public long getJournalSyncMaxTime() {
    return myMetrics.syncs.getMaxTime();
  }

  
  /**
   * @inheritDoc
   */
  public long getJournalSyncMinTime() {
    return myMetrics.syncs.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getJournalSyncNum() {
    return myMetrics.syncs.getPreviousIntervalNumOps();
  }

 
  /**
   * @inheritDoc
   */
  public int getSafemodeTime() {
    return myMetrics.safeModeTime.get();
  }

  /**
   * @inheritDoc
   */
  public int getFSImageLoadTime() {
    return myMetrics.fsImageLoadTime.get();
  }

  /**
   * @inheritDoc
   */
  public void resetAllMinMax() {
    myMetrics.resetAllMinMax();
  }
  
  /**
   * @inheritDoc
   */
  public int getNumFilesCreated() {
    return myMetrics.numFilesCreated.getPreviousIntervalValue();
  }

  /** 
   *@deprecated call getNumGetListingOps() instead
   */
  @Deprecated
  public int getNumFilesListed() {
    return getNumGetListingOps();
  }

  /**
   * @inheritDoc
   */
  public int getNumGetListingOps() {
    return myMetrics.numGetListingOps.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumCreateFileOps() {
    return myMetrics.numCreateFileOps.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumDeleteFileOps() {
    return myMetrics.numDeleteFileOps.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumAddBlockOps() {
    return myMetrics.numAddBlockOps.getPreviousIntervalValue();
  }

  /** @inheritDoc */
  public int getNumGetBlockLocations() {
    return myMetrics.numGetBlockLocations.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumFilesRenamed() {
    return myMetrics.numFilesRenamed.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumFilesAppended() {
    return myMetrics.numFilesAppended.getPreviousIntervalValue();
  }
}
