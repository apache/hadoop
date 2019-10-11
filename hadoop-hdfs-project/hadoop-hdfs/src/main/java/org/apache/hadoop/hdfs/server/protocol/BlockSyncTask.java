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
import org.apache.hadoop.hdfs.protocol.BlockSyncTaskOperation;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import java.net.URI;
import java.util.UUID;

/**
 * A BlockSyncTask is an operation that is sent to the datanodes to copy
 * blocks to an external storage endpoint.
 * BlockSyncTask is intended to be an immutable POJO.
 */
public abstract class BlockSyncTask {
  private final UUID syncTaskId;
  private final BlockSyncTaskOperation operation;
  private final URI remoteURI;
  private final LocatedBlock locatedBlock;
  private String syncMountId;

  public BlockSyncTask(UUID syncTaskId, BlockSyncTaskOperation operation, URI remoteURI,
       LocatedBlock locatedBlock, String syncMountId) {
    this.syncTaskId = syncTaskId;
    this.operation = operation;
    this.remoteURI = remoteURI;
    this.locatedBlock = locatedBlock;
    this.syncMountId = syncMountId;
  }

  public static class MultipartPutPartSyncTask extends BlockSyncTask {
    public final int partNumber;
    public byte[] uploadHandle;

    public MultipartPutPartSyncTask(UUID syncTaskId, URI remoteURI, LocatedBlock locatedBlock,
        Integer partNumber, byte[] uploadHandle, String syncMountId) {
      super(syncTaskId, BlockSyncTaskOperation.MULTIPART_PUT_PART,
          remoteURI, locatedBlock, syncMountId);
      this.partNumber = partNumber;
      this.uploadHandle = uploadHandle;
    }

    public int getPartNumber() {
      return partNumber;
    }

    public byte[] getUploadHandle() {
      return uploadHandle;
    }

  }

  public static class PutFileSyncTask extends BlockSyncTask {
    public PutFileSyncTask(UUID syncTaskId, URI remoteURI, LocatedBlock locatedBlock,
        String syncMountId) {
      super(syncTaskId, BlockSyncTaskOperation.PUT_FILE,
          remoteURI, locatedBlock,  syncMountId);
    }

  }

  public UUID getSyncTaskId() {
    return syncTaskId;
  }

  public BlockSyncTaskOperation getOperation() {
    return operation;
  }

  public URI getRemoteURI() {
    return remoteURI;
  }

  public LocatedBlock getLocatedBlock() {
    return locatedBlock;
  }

  public String getSyncMountId() {
    return syncMountId;
  }
}