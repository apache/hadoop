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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;

/**
 * A unique signature intended to identify checkpoint transactions.
 */
@InterfaceAudience.Private
public class CheckpointSignatureWritable implements Writable { 
  private String blockpoolID = "";
  private long mostRecentCheckpointTxId;
  private long curSegmentTxId;
  private StorageInfoWritable storageInfo;

  public CheckpointSignatureWritable() {}

  CheckpointSignatureWritable(long mostRecentCheckpointTxId,
      long curSegmentTxId, int layoutVersion, int namespaceID, String bpid,
      String clusterID, long cTime) {
    this.blockpoolID = bpid;
    this.mostRecentCheckpointTxId = mostRecentCheckpointTxId;
    this.curSegmentTxId = curSegmentTxId;
    this.storageInfo = new StorageInfoWritable(layoutVersion, namespaceID,
        clusterID, cTime);
  }

  /////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////
  static {
    WritableFactories.setFactory(CheckpointSignatureWritable.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new CheckpointSignatureWritable();
          }
        });
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    storageInfo.write(out);
    WritableUtils.writeString(out, blockpoolID);
    out.writeLong(mostRecentCheckpointTxId);
    out.writeLong(curSegmentTxId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    storageInfo.readFields(in);
    blockpoolID = WritableUtils.readString(in);
    mostRecentCheckpointTxId = in.readLong();
    curSegmentTxId = in.readLong();
  }

  public static CheckpointSignatureWritable convert(
      CheckpointSignature sig) {
    return new CheckpointSignatureWritable(sig.getMostRecentCheckpointTxId(),
        sig.getCurSegmentTxId(), sig.getLayoutVersion(), sig.getNamespaceID(),
        sig.getBlockpoolID(), sig.getClusterID(), sig.getCTime());
  }

  public CheckpointSignature convert() {
    return new CheckpointSignature(storageInfo.convert(), blockpoolID,
        mostRecentCheckpointTxId, curSegmentTxId);
  }
}
