package org.apache.hadoop.dfs;

import java.util.Date;

import org.apache.hadoop.io.UTF8;

/** A report on the status of a DataNode.
 *
 * @see DistributedFileSystem#getDataNodeStats
 */
public class DataNodeReport {
  String name;
  String host;
  long capacity;
  long remaining;
  long lastUpdate;
  
  /** The name of the datanode. */
  public String getName() { return name; }

  /** The hostname of the datanode. */
  public String getHost() { return host; }

  /** The raw capacity. */
  public long getCapacity() { return capacity; }

  /** The raw free space. */
  public long getRemaining() { return remaining; }

  /** The time when this information was accurate. */
  public long getLastUpdate() { return lastUpdate; }

  public String toString() {
    StringBuffer buffer = new StringBuffer();
    long c = getCapacity();
    long r = getRemaining();
    long u = c - r;
    buffer.append("Name: "+name+"\n");
    buffer.append("Total raw bytes: "+c+" ("+DFSShell.byteDesc(c)+")"+"\n");
    buffer.append("Used raw bytes: "+u+" ("+DFSShell.byteDesc(u)+")"+"\n");
    buffer.append("% used: "+DFSShell.limitDecimal(((1.0*u)/c)*100,2)+"%"+"\n");
    buffer.append("Last contact: "+new Date(lastUpdate)+"\n");
    return buffer.toString();
  }

}
