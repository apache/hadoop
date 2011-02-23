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
package org.apache.hadoop.hdfs.server.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
@InterfaceAudience.Private
public class StorageInfo implements Writable {
  public int   layoutVersion;   // layout version of the storage data
  public int   namespaceID;     // id of the file system
  public String clusterID;      // id of the cluster
  public long  cTime;           // creation time of the file system state
  
  public StorageInfo () {
    this(0, 0, "", 0L);
  }
  
  public StorageInfo(int layoutV, int nsID, String cid, long cT) {
    layoutVersion = layoutV;
    clusterID = cid;
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
   * cluster id of the file system.<p>
   */
  public String    getClusterID()  { return clusterID; }
  
  /**
   * Creation time of the file system state.<p>
   * Modified during upgrades.
   */
  public long   getCTime()        { return cTime; }
  
  public void   setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    clusterID = from.clusterID;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  public void write(DataOutput out) throws IOException {
    out.writeInt(getLayoutVersion());
    out.writeInt(getNamespaceID());
    WritableUtils.writeString(out, clusterID);
    out.writeLong(getCTime());
  }

  public void readFields(DataInput in) throws IOException {
    layoutVersion = in.readInt();
    namespaceID = in.readInt();
    clusterID = WritableUtils.readString(in);
    cTime = in.readLong();
  }
  
  /** validate and set namespaceID */
  protected void setNamespaceID(File storage, int nsId)
      throws InconsistentFSStateException {
    if (namespaceID != 0 && nsId != 0 && namespaceID != nsId) {
      throw new InconsistentFSStateException(storage,
          "namespaceID is incompatible with others.");
    }
    namespaceID = nsId;
  }
  
  /** validate and set layout version */ 
  protected void setLayoutVersion(File storage, int lv)
      throws IncorrectVersionException, IOException {
    if (lv < FSConstants.LAYOUT_VERSION) { // future version
      throw new IncorrectVersionException(lv, "storage directory "
          + storage.getCanonicalPath());
    }
    layoutVersion = lv;
  }
  
  /** validate and set ClusterID */
  protected void setClusterID(File storage, String cid)
      throws InconsistentFSStateException {
    if (!clusterID.equals("") && !cid.equals("") && !clusterID.equals(cid)) {
      throw new InconsistentFSStateException(storage,
          "cluster id is incompatible with others.");
    }
    clusterID = cid;
  }
}
