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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.TrackableTask;
import org.apache.hadoop.hdfs.server.protocol.SyncTask.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.CREATE_DIRECTORY;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.DELETE_DIRECTORY;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.DELETE_FILE;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.MODIFY_FILE;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.MULTIPART_INIT;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.MULTIPART_COMPLETE;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.RENAME_DIRECTORY;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.RENAME_FILE;
import static org.apache.hadoop.hdfs.protocol.MetadataSyncTaskOperation.TOUCH_FILE;


/**
 * A MetadataSyncTask is an Synchonization operation that is tracked by the Namenode.
 */
public abstract class MetadataSyncTask implements TrackableTask {

  private final UUID syncTaskId;
  private final MetadataSyncTaskOperation operation;
  private final URI uri;
  private String syncMountId;

  public MetadataSyncTask(UUID syncTaskId, MetadataSyncTaskOperation operation,
      URI uri, String syncMountId) {
    this.syncTaskId = syncTaskId;
    this.operation = operation;
    this.uri = uri;
    this.syncMountId = syncMountId;
  }

  @Override
  public UUID getSyncTaskId() {
    return syncTaskId;
  }

  public MetadataSyncTaskOperation getOperation() {
    return operation;
  }

  public URI getUri() {
    return uri;
  }

  public String getSyncMountId() {
    return syncMountId;
  }

  public long getNumberOfBytes() {
    return 0L;
  }

  public static class TouchFileMetadataSyncTask extends MetadataSyncTask {
    public TouchFileMetadataSyncTask(UUID syncTaskId, URI uri, String syncMountId) {
      super(syncTaskId, TOUCH_FILE, uri, syncMountId);
    }
  }

  public static class RenameFileMetadataSyncTask extends MetadataSyncTask {
    public final URI renamedTo;
    public List<Block> blocks;
    public RenameFileMetadataSyncTask(UUID syncTaskId, URI uri, URI renamedTo,
        List<Block> blocks, String syncMountId) {
      super(syncTaskId, RENAME_FILE, uri, syncMountId);
      this.renamedTo = renamedTo;
      this.blocks = blocks;
    }
  }

  public static class DeleteFileMetadataSyncTask extends MetadataSyncTask {
    List<Block> blocks;
    public DeleteFileMetadataSyncTask(UUID syncTaskId, URI uri,
        List<Block> blocks, String syncMountId) {
      super(syncTaskId, DELETE_FILE, uri, syncMountId);
      this.blocks = blocks;
    }

    public List<Block> getBlocks() {
      return blocks;
    }
  }

  public static class DeleteDirectoryMetadataSyncTask extends MetadataSyncTask {
    public DeleteDirectoryMetadataSyncTask(UUID syncTaskId, URI uri, String syncMountId) {
      super(syncTaskId, DELETE_DIRECTORY, uri, syncMountId);
    }
  }

  public static class MultipartInitMetadataSyncTask extends MetadataSyncTask {
    public MultipartInitMetadataSyncTask(UUID syncTaskId, URI uri, String syncMountId) {
      super(syncTaskId, MULTIPART_INIT, uri, syncMountId);
    }
  }

  public static class MultipartCompleteMetadataSyncTask extends MetadataSyncTask {
    public List<ExtendedBlock> blocks;
    public ByteBuffer uploadHandle;
    public List<ByteBuffer> partHandles;
    private final long blockCollectionId;

    public MultipartCompleteMetadataSyncTask(UUID syncTaskId, URI uri,
        List<ExtendedBlock> blocks, ByteBuffer uploadHandle,
        List<ByteBuffer> partHandles, String syncMountId,
        long blockCollectionId) {
      super(syncTaskId, MULTIPART_COMPLETE, uri, syncMountId);
      this.blocks = blocks;
      this.uploadHandle = uploadHandle;
      this.partHandles = partHandles;
      this.blockCollectionId = blockCollectionId;
    }

    public void update(SyncTaskExecutionResult syncTaskExecutionResult,
        List<SyncTaskExecutionResult> parallelSyncTaskExecutionResults) {
      uploadHandle = syncTaskExecutionResult.getResult();
      partHandles = parallelSyncTaskExecutionResults.stream()
          .map(handle -> handle.getResult())
          .collect(Collectors.toList());
    }

    public long getBlockCollectionId() {
      return blockCollectionId;
    }
  }

  public static class CreateDirectoryMetadataSyncTask extends MetadataSyncTask {
    public CreateDirectoryMetadataSyncTask(UUID syncTaskId, URI uri, String syncMountId) {
      super(syncTaskId, CREATE_DIRECTORY, uri, syncMountId);
    }
  }

  public static class RenameDirectoryMetadataSyncTask extends MetadataSyncTask {
    public final URI renamedTo;

    public RenameDirectoryMetadataSyncTask(UUID syncTaskId, URI uri, URI renamedTo, String syncMountId) {
      super(syncTaskId, RENAME_DIRECTORY, uri, syncMountId);
      this.renamedTo = renamedTo;
    }
  }

  public static MetadataSyncTask touchFile(TouchFileSyncTask syncTask) {
    return new TouchFileMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getSyncMountId());
  }

  public static MetadataSyncTask touchFile(UUID syncTaskId, URI uri, String syncMountId) {
    return new TouchFileMetadataSyncTask(syncTaskId, uri, syncMountId);
  }

  public static MetadataSyncTask createFileMultipartInit(URI uri, String syncMountId) {
    return new MultipartInitMetadataSyncTask(UUID.randomUUID(), uri, syncMountId);
  }

  public static BiFunction<ByteBuffer, List<ByteBuffer>, MultipartCompleteMetadataSyncTask>
    multipartComplete(URI uri, List<ExtendedBlock> blocks, String syncMountId,
      long blockCollectionId) {
    return (uploadHandle, partHandles) ->
        new MultipartCompleteMetadataSyncTask(
            UUID.randomUUID(),
            uri,
            blocks,
            uploadHandle,
            partHandles,
            syncMountId,
            blockCollectionId);
  }

  public static MetadataSyncTask deleteFile(DeleteFileSyncTask syncTask) {
    return new DeleteFileMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getBlocks(), syncTask.getSyncMountId());
  }

  public static MetadataSyncTask deleteFile(UUID syncTaskId, URI uri,
      List<Block> blocks, String syncMountId) {
    return new DeleteFileMetadataSyncTask(syncTaskId, uri, blocks,
        syncMountId);
  }

  public static MetadataSyncTask renameFile(RenameFileSyncTask syncTask) {
    return new RenameFileMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getRenamedTo(), syncTask.blocks,
        syncTask.getSyncMountId());
  }

  public static MetadataSyncTask renameFile(UUID syncTaskId, URI uri,
      URI renamedTo, List<Block> blocks, String syncMountId) {
    return new RenameFileMetadataSyncTask(syncTaskId, uri, renamedTo,
        blocks, syncMountId);
  }

  public static CreateDirectoryMetadataSyncTask createDirectory(
      CreateDirectorySyncTask syncTask) {
    return new CreateDirectoryMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getSyncMountId());
  }

  public static MetadataSyncTask deleteDirectory(
      DeleteDirectorySyncTask syncTask) {
    return new DeleteDirectoryMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getSyncMountId());
  }

  public static MetadataSyncTask deleteDirectory(UUID syncTaskId, URI uri,
      String syncMountId) {
    return new DeleteDirectoryMetadataSyncTask(syncTaskId, uri, syncMountId);
  }

  public static MetadataSyncTask renameDirectory(
      RenameDirectorySyncTask syncTask) {
    return new RenameDirectoryMetadataSyncTask(syncTask.getSyncTaskId(),
        syncTask.getUri(), syncTask.getRenamedTo(), syncTask.getSyncMountId());
  }

  public static MetadataSyncTask renameDirectory(UUID syncTaskId, URI uri,
      URI renamedTo, String syncMountId) {
    return new RenameDirectoryMetadataSyncTask(syncTaskId, uri, renamedTo, syncMountId);
  }

  @Override
  public String toString() {
    return "MetadataSyncTask{" +
        "syncTaskId=" + syncTaskId +
        ", operation=" + operation +
        ", uri=" + uri +
        ", syncMountId='" + syncMountId + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MetadataSyncTask that = (MetadataSyncTask) o;

    if (!syncTaskId.equals(that.syncTaskId)) return false;
    if (operation != that.operation) return false;
    if (!uri.equals(that.uri)) return false;
    return syncMountId.equals(that.syncMountId);
  }

  @Override
  public int hashCode() {
    int result = syncTaskId.hashCode();
    result = 31 * result + operation.hashCode();
    result = 31 * result + uri.hashCode();
    result = 31 * result + syncMountId.hashCode();
    return result;
  }
}
