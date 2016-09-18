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

package org.apache.hadoop.hdfs.server.datanode.extdataset;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.ChunkChecksum;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipeline;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;

public class ExternalReplicaInPipeline implements ReplicaInPipeline {

  @Override
  public void setNumBytes(long bytesReceived) {
  }

  @Override
  public long getBytesAcked() {
    return 0;
  }

  @Override
  public void setBytesAcked(long bytesAcked) {
  }

  @Override
  public void releaseAllBytesReserved() {
  }

  @Override
  public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
  }

  @Override
  public ChunkChecksum getLastChecksumAndDataLen() {
    return new ChunkChecksum(0, null);
  }

  @Override
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException {
    return new ReplicaOutputStreams(null, null, requestedChecksum, false);
  }

  @Override
  public OutputStream createRestartMetaStream() throws IOException {
    return null;
  }

  @Override
  public long getBlockId() {
    return 0;
  }

  @Override
  public long getGenerationStamp() {
    return 0;
  }

  @Override
  public ReplicaState getState() {
    return ReplicaState.FINALIZED;
  }

  @Override
  public long getNumBytes() {
    return 0;
  }

  @Override
  public long getBytesOnDisk() {
    return 0;
  }

  @Override
  public long getVisibleLength() {
    return 0;
  }

  @Override
  public String getStorageUuid() {
    return null;
  }

  @Override
  public boolean isOnTransientStorage() {
    return false;
  }

  @Override
  public ReplicaInfo getReplicaInfo() {
    return null;
  }

  public void setWriter(Thread writer) {
  }

  public void stopWriter(long xceiverStopTimeout)
      throws IOException {
  }

  @Override
  public boolean attemptToSetWriter(Thread prevWriter, Thread newWriter) {
    return false;
  }

  @Override
  public void interruptThread() {
  }
}
