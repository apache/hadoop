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
package org.apache.hadoop.dfs.namenode.metrics;

import org.apache.hadoop.dfs.NameNode;
import org.apache.hadoop.dfs.NameNodeMetrics;
import org.apache.hadoop.metrics.util.MBeanUtil;

/**
 * 
 * This is the implementation of the Name Node JMX MBean
 *
 */
public class NameNodeMgt implements NameNodeMgtMBean {
  private NameNodeMetrics myMetrics;
  private NameNode myNameNode;

  public NameNodeMgt(String sessionId, NameNodeMetrics nameNodeMetrics, NameNode nameNode) {
    myMetrics = nameNodeMetrics;
    myNameNode = nameNode;
    MBeanUtil.registerMBean("NameNode", "NameNodeStatistics", this);
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
  public String getNameNodeState() {
    return myNameNode.isInSafeMode() ? "safeMode" : "Operational";
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
    myMetrics.syncs.resetMinMax();
    myMetrics.transactions.resetMinMax();
    myMetrics.blockReport.resetMinMax();
  }
  
  /**
   * @inheritDoc
   */
  public int getNumFilesCreated() {
    return myMetrics.numFilesCreated.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumFilesListed() {
    return myMetrics.numFilesListed.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumFilesOpened() {
    return myMetrics.numFilesOpened.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getNumFilesRenamed() {
    return myMetrics.numFilesRenamed.getPreviousIntervalValue();
  }
}
