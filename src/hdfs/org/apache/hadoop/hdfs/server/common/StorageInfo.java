package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
public class StorageInfo implements Writable {
  public int   layoutVersion;   // layout version of the storage data
  public int   namespaceID;     // id of the file system
  public long  cTime;           // creation time of the file system state
  
  public StorageInfo () {
    this(0, 0, 0L);
  }
  
  public StorageInfo(int layoutV, int nsID, long cT) {
    layoutVersion = layoutV;
    namespaceID = nsID;
    cTime = cT;
  }
  
  public StorageInfo(StorageInfo from) {
    setStorageInfo(from);
  }

  /**
   * Layout version of the storage data.
   */
  public int    getLayoutVersion(){ return layoutVersion; }

  /**
   * Namespace id of the file system.<p>
   * Assigned to the file system at formatting and never changes after that.
   * Shared by all file system components.
   */
  public int    getNamespaceID()  { return namespaceID; }

  /**
   * Creation time of the file system state.<p>
   * Modified during upgrades.
   */
  public long   getCTime()        { return cTime; }

  public void   setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeInt(getLayoutVersion());
    out.writeInt(getNamespaceID());
    out.writeLong(getCTime());
  }

  public void readFields(DataInput in) throws IOException {
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    cTime = in.readLong();
  }
}
