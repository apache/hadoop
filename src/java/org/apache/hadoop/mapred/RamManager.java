package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;

class RamManager {
  volatile private int numReserved = 0;
  volatile private int size = 0;
  private final int maxSize;
  
  public RamManager(Configuration conf) {
    maxSize = conf.getInt("fs.inmemory.size.mb", 100) * 1024 * 1024;
  }
  
  synchronized boolean reserve(long requestedSize) {
    if (requestedSize > Integer.MAX_VALUE || 
        (size + requestedSize) > Integer.MAX_VALUE) {
      return false;
    }
    
    if ((size + requestedSize) < maxSize) {
      size += requestedSize;
      ++numReserved;
      return true;
    }
    return false;
  }
  
  synchronized void unreserve(int requestedSize) {
    size -= requestedSize;
    --numReserved;
  }
  
  int getUsedMemory() {
    return size;
  }
  
  float getPercentUsed() {
    return (float)size/maxSize;
  }
  
  int getReservedFiles() {
    return numReserved;
  }
  
  int getMemoryLimit() {
    return maxSize;
  }
}
