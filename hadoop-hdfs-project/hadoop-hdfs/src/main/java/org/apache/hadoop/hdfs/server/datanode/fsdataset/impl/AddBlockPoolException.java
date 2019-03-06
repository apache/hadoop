package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

/**
 * This exception collects all IOExceptions thrown when adding block pools and
 * scanning volumes. It keeps the information about which volume is associated
 * with an exception.
 *
 */
public class AddBlockPoolException extends IOException {
  private Map<FsVolumeSpi, IOException> unhealthyDataDirs;
  public AddBlockPoolException(Map<FsVolumeSpi, IOException>
      unhealthyDataDirs) {
    this.unhealthyDataDirs = unhealthyDataDirs;
  }

  public Map<FsVolumeSpi, IOException> getFailingVolumes() {
    return unhealthyDataDirs;
  }
  @Override
  public String toString() {
    return getClass().getName() + ": " + unhealthyDataDirs.toString();
  }
}