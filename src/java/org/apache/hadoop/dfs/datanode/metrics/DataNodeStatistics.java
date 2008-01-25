package org.apache.hadoop.dfs.datanode.metrics;

import java.util.Random;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.util.MBeanUtil;

public class DataNodeStatistics implements DataNodeStatisticsMBean {
  private DataNodeMetrics myMetrics;
  private ObjectName mbeanName;
  private Random rand = new Random(); 
  
  

  /**
   * This constructs and registers the DataNodeStatisticsMBean
   * @param dataNodeMetrics - the metrics from which the mbean gets its info
   */
  DataNodeStatistics(DataNodeMetrics dataNodeMetrics, String storageId) {
    myMetrics = dataNodeMetrics;
    String serverName;
    if (storageId.equals("")) {// Temp fix for the uninitialized storage
      serverName = "DataNode-UndefinedStorageId" + rand.nextInt();
    } else {
      serverName = "DataNode-" + storageId;
    }
    mbeanName = MBeanUtil.registerMBean(serverName, "DataNodeStatistics", this);
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
  public void resetAllMinMax() {
    myMetrics.resetAllMinMax();
  }

  /**
   * @inheritDoc
   */
  public int getBlocksRead() {
    return myMetrics.blocksRead.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBlocksRemoved() {
    return myMetrics.blocksRemoved.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBlocksReplicated() {
    return myMetrics.blocksReplicated.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBlocksWritten() {
    return myMetrics.blocksWritten.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBytesRead() {
    return myMetrics.bytesRead.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBlockVerificationFailures() {
    return myMetrics.blockVerificationFailures.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getBlocksVerified() {
    return myMetrics.blocksVerified.getPreviousIntervalValue();
  }
  
  /**
   * @inheritDoc
   */
  public int getReadsFromLocalClient() {
    return myMetrics.readsFromLocalClient.getPreviousIntervalValue();
  }
  
  /**
   * @inheritDoc
   */
  public int getReadsFromRemoteClient() {
    return myMetrics.readsFromRemoteClient.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getWritesFromLocalClient() {
    return myMetrics.writesFromLocalClient.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public int getWritesFromRemoteClient() {
    return myMetrics.writesFromRemoteClient.getPreviousIntervalValue();
  }

  /**
   * @inheritDoc
   */
  public long getReadBlockOpAverageTime() {
    return myMetrics.readBlockOp.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getReadBlockOpMaxTime() {
    return myMetrics.readBlockOp.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getReadBlockOpMinTime() {
    return myMetrics.readBlockOp.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getReadBlockOpNum() {
    return myMetrics.readBlockOp.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getReadMetadataOpAverageTime() {
    return myMetrics.readMetadataOp.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getReadMetadataOpMaxTime() {
    return myMetrics.readMetadataOp.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getReadMetadataOpMinTime() {
    return myMetrics.readMetadataOp.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getReadMetadataOpNum() {
    return myMetrics.readMetadataOp.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getReplaceBlockOpAverageTime() {
    return myMetrics.replaceBlockOp.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getReplaceBlockOpMaxTime() {
    return myMetrics.replaceBlockOp.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getReplaceBlockOpMinTime() {
    return myMetrics.replaceBlockOp.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getReplaceBlockOpNum() {
    return myMetrics.replaceBlockOp.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getWriteBlockOpAverageTime() {
    return myMetrics.writeBlockOp.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getWriteBlockOpMaxTime() {
    return myMetrics.writeBlockOp.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getWriteBlockOpMinTime() {
    return myMetrics.writeBlockOp.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getWriteBlockOpNum() {
    return myMetrics.writeBlockOp.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getCopyBlockOpAverageTime() {
    return myMetrics.copyBlockOp.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getCopyBlockOpMaxTime() {
    return myMetrics.copyBlockOp.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getCopyBlockOpMinTime() {
    return myMetrics.copyBlockOp.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getCopyBlockOpNum() {
    return myMetrics.copyBlockOp.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getBlockReportsAverageTime() {
    return myMetrics.blockReports.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getBlockReportsMaxTime() {
    return myMetrics.blockReports.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getBlockReportsMinTime() {
    return myMetrics.blockReports.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getBlockReportsNum() {
    return myMetrics.blockReports.getPreviousIntervalNumOps();
  }

  /**
   * @inheritDoc
   */
  public long getHeartbeatsAverageTime() {
    return myMetrics.heartbeats.getPreviousIntervalAverageTime();
  }

  /**
   * @inheritDoc
   */
  public long getHeartbeatsMaxTime() {
    return myMetrics.heartbeats.getMaxTime();
  }

  /**
   * @inheritDoc
   */
  public long getHeartbeatsMinTime() {
    return myMetrics.heartbeats.getMinTime();
  }

  /**
   * @inheritDoc
   */
  public int getHeartbeatsNum() {
    return myMetrics.heartbeats.getPreviousIntervalNumOps();
  }
}