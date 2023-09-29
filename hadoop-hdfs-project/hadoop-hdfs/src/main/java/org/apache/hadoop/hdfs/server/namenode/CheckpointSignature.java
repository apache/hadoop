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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;

import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
public class CheckpointSignature extends StorageInfo
    implements Comparable<CheckpointSignature> { 

  private static final String FIELD_SEPARATOR = ":";
  private static final int NUM_FIELDS = 7;
  String blockpoolID = "";
  long mostRecentCheckpointTxId;
  long curSegmentTxId;

  CheckpointSignature(FSImage fsImage) {
    super(fsImage.getStorage());
    blockpoolID = fsImage.getBlockPoolID();
    
    mostRecentCheckpointTxId = fsImage.getStorage().getMostRecentCheckpointTxId();
    curSegmentTxId = fsImage.getEditLog().getCurSegmentTxId();
  }

  CheckpointSignature(String str) {
    super(NodeType.NAME_NODE);
    String[] fields = str.split(FIELD_SEPARATOR);
    assert fields.length == NUM_FIELDS :
      "Must be " + NUM_FIELDS + " fields in CheckpointSignature";
    int i = 0;
    layoutVersion = Integer.parseInt(fields[i++]);
    namespaceID = Integer.parseInt(fields[i++]);
    cTime = Long.parseLong(fields[i++]);
    mostRecentCheckpointTxId  = Long.parseLong(fields[i++]);
    curSegmentTxId  = Long.parseLong(fields[i++]);
    clusterID = fields[i++];
    blockpoolID = fields[i];
  }

  public CheckpointSignature(StorageInfo info, String blockpoolID,
      long mostRecentCheckpointTxId, long curSegmentTxId) {
    super(info);
    this.blockpoolID = blockpoolID;
    this.mostRecentCheckpointTxId = mostRecentCheckpointTxId;
    this.curSegmentTxId = curSegmentTxId;
  }

  /**
   * Get the cluster id from CheckpointSignature
   * @return the cluster id
   */
  @Override
  public String getClusterID() {
    return clusterID;
  }

  /**
   * Get the block pool id from CheckpointSignature
   * @return the block pool id
   */
  public String getBlockpoolID() {
    return blockpoolID;
  }

  public long getMostRecentCheckpointTxId() {
    return mostRecentCheckpointTxId;
  }

  public long getCurSegmentTxId() {
    return curSegmentTxId;
  }

  /**
   * Set the block pool id of CheckpointSignature.
   * 
   * @param blockpoolID the new blockpool id
   */
  public void setBlockpoolID(String blockpoolID) {
    this.blockpoolID = blockpoolID;
  }
  
  @Override
  public String toString() {
    return String.valueOf(layoutVersion) + FIELD_SEPARATOR
         + String.valueOf(namespaceID) + FIELD_SEPARATOR
         + String.valueOf(cTime) + FIELD_SEPARATOR
         + String.valueOf(mostRecentCheckpointTxId) + FIELD_SEPARATOR
         + String.valueOf(curSegmentTxId) + FIELD_SEPARATOR
         + clusterID + FIELD_SEPARATOR
         + blockpoolID ;
  }

  boolean storageVersionMatches(StorageInfo si) throws IOException {
    return (layoutVersion == si.layoutVersion) && (cTime == si.cTime);
  }

  boolean isSameCluster(FSImage si) {
    return namespaceID == si.getStorage().namespaceID &&
      clusterID.equals(si.getClusterID()) &&
      blockpoolID.equals(si.getBlockPoolID());
  }

  boolean namespaceIdMatches(FSImage si) {
    return namespaceID == si.getStorage().namespaceID;
  }

  void validateStorageInfo(FSImage si) throws IOException {
    if (!isSameCluster(si)
        || !storageVersionMatches(si.getStorage())) {
      throw new IOException("Inconsistent checkpoint fields.\n"
          + "LV = " + layoutVersion + " namespaceID = " + namespaceID
          + " cTime = " + cTime
          + " ; clusterId = " + clusterID
          + " ; blockpoolId = " + blockpoolID
          + ".\nExpecting respectively: "
          + si.getStorage().layoutVersion + "; " 
          + si.getStorage().namespaceID + "; " + si.getStorage().cTime
          + "; " + si.getClusterID() + "; " 
          + si.getBlockPoolID() + ".");
    }
  }

  //
  // Comparable interface
  //
  @Override
  public int compareTo(CheckpointSignature o) {
    return ComparisonChain.start()
      .compare(layoutVersion, o.layoutVersion)
      .compare(namespaceID, o.namespaceID)
      .compare(cTime, o.cTime)
      .compare(mostRecentCheckpointTxId, o.mostRecentCheckpointTxId)
      .compare(curSegmentTxId, o.curSegmentTxId)
      .compare(clusterID, o.clusterID)
      .compare(blockpoolID, o.blockpoolID)
      .result();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CheckpointSignature)) {
      return false;
    }
    return compareTo((CheckpointSignature)o) == 0;
  }

  @Override
  public int hashCode() {
    return layoutVersion ^ namespaceID ^
            (int)(cTime ^ mostRecentCheckpointTxId ^ curSegmentTxId)
            ^ clusterID.hashCode() ^ blockpoolID.hashCode();
  }
}
