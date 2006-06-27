package org.apache.hadoop.dfs;

/** A report on the status of a DataNode.
 *
 * @see DistributedFileSystem#getDataNodeStats
 * @deprecated Use {@link DatanodeInfo} instead.
 */
public class DataNodeReport extends DatanodeInfo {
  public String toString() {
    return super.getDatanodeReport();
  }
}
