package org.apache.hadoop.hdfs.server.protocol;

/**
 * Utilization report for a Datanode storage
 */
public class StorageReport {
  private final String storageID;
  private final boolean failed;
  private final long capacity;
  private final long dfsUsed;
  private final long remaining;
  private final long blockPoolUsed;
  
  public StorageReport(String sid, boolean failed, long capacity, long dfsUsed,
      long remaining, long bpUsed) {
    this.storageID = sid;
    this.failed = failed;
    this.capacity = capacity;
    this.dfsUsed = dfsUsed;
    this.remaining = remaining;
    this.blockPoolUsed = bpUsed;
  }

  public String getStorageID() {
    return storageID;
  }

  public boolean isFailed() {
    return failed;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getDfsUsed() {
    return dfsUsed;
  }

  public long getRemaining() {
    return remaining;
  }

  public long getBlockPoolUsed() {
    return blockPoolUsed;
  }
}
