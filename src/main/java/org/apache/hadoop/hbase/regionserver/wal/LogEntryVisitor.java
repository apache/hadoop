package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.HRegionInfo;

public interface LogEntryVisitor {

  /**
   *
   * @param info
   * @param logKey
   * @param logEdit
   */
  public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
                                       WALEdit logEdit);
}
