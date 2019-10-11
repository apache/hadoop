/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.protocol;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

/**
 * A BlockSyncTask is an operation that is sent to the datanodes to copy
 * blocks to an external storage endpoint as a part of an orchestrated
 * synchronization across multiple datanodes.
 * BlockSyncTask is intended to be an immutable POJO.
 */
public class BlockSyncTask {
  private final UUID syncTaskId;
  private final URI remoteURI;
  private final List<LocatedBlock> locatedBlocks;
  private String syncMountId;
  private final int partNumber;
  private ByteBuffer uploadHandle;
  private final int offset;
  private final long length;

  public BlockSyncTask(UUID syncTaskId, URI remoteURI,
      List<LocatedBlock> locatedBlocks, Integer partNumber,
      ByteBuffer uploadHandle, int offset, long length, String syncMountId) {
    this.syncTaskId = syncTaskId;
    this.remoteURI = remoteURI;
    this.locatedBlocks = locatedBlocks;
    this.syncMountId = syncMountId;
    this.partNumber = partNumber;
    this.uploadHandle = uploadHandle;
    this.offset = offset;
    this.length = length;
  }

  public int getPartNumber() {
    return partNumber;
  }

  public ByteBuffer getUploadHandle() {
    return uploadHandle;
  }

  public int getOffset() {
    return offset;
  }

  public long getLength() {
    return length;
  }

  public UUID getSyncTaskId() {
    return syncTaskId;
  }

  public URI getRemoteURI() {
    return remoteURI;
  }

  public List<LocatedBlock> getLocatedBlocks() {
    return locatedBlocks;
  }

  public String getSyncMountId() {
    return syncMountId;
  }
}