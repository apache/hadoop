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
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.syncservice.updatetracker.TrackableTask;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.CREATE_DIRECTORY;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.CREATE_FILE;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.DELETE_DIRECTORY;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.DELETE_FILE;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.MODIFY_FILE;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.RENAME_DIRECTORY;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.RENAME_FILE;
import static org.apache.hadoop.hdfs.server.protocol.SyncTaskOperation.TOUCH_FILE;

/**
 * A SyncTask is an Synchronization operation that is tracked by the Namenode.
 */
public abstract class SyncTask implements TrackableTask {

  private final UUID syncTaskId;
  private final SyncTaskOperation operation;
  private final URI uri;
  private String syncMountId;

  public SyncTask(
      UUID syncTaskId,
      SyncTaskOperation operation,
      URI uri,
      String syncMountId) {
    this.syncTaskId = syncTaskId;
    this.operation = operation;
    this.uri = uri;
    this.syncMountId = syncMountId;
  }

  @Override
  public UUID getSyncTaskId() {
    return syncTaskId;
  }

  public SyncTaskOperation getOperation() {
    return operation;
  }

  public URI getUri() {
    return uri;
  }

  /**
   * Get the final uri on remote storage.
   */
  public URI getFinalUri() {
    return getUri();
  }

  public String getSyncMountId() {
    return syncMountId;
  }

  public long getNumberOfBytes() {
    return 0L;
  }

  /**
   * Sync task to touch file.
   */
  public static class TouchFileSyncTask extends SyncTask {
    private final long blockCollectionId;

    public TouchFileSyncTask(UUID syncTaskId, URI uri, String syncMountId,
        long blockCollectionId) {
      super(syncTaskId, TOUCH_FILE, uri, syncMountId);
      this.blockCollectionId = blockCollectionId;
    }

    public long getBlockCollectionId() {
      return blockCollectionId;
    }
  }

  /**
   * Sync task to create file.
   */
  public static class CreateFileSyncTask extends SyncTask {
    private final long blockCollectionId;
    private List<LocatedBlock> locatedBlocks;

    public CreateFileSyncTask(UUID syncTaskId, URI uri, String syncMountId,
        List<LocatedBlock> locatedBlocks, long blockCollectionId) {
      super(syncTaskId, CREATE_FILE, uri, syncMountId);
      this.locatedBlocks = locatedBlocks;
      this.blockCollectionId = blockCollectionId;
    }

    public List<LocatedBlock> getLocatedBlocks() {
      return locatedBlocks;
    }

    public long getBlockCollectionId() {
      return blockCollectionId;
    }
  }

  /**
   * Sync task to modify file.
   */
  public static class ModifyFileSyncTask extends SyncTask {
    private List<LocatedBlock> locatedBlocks;

    public ModifyFileSyncTask(UUID syncTaskId, URI uri, String syncMountId,
        List<LocatedBlock> locatedBlocks) {
      super(syncTaskId, MODIFY_FILE, uri, syncMountId);
      this.locatedBlocks = locatedBlocks;
    }

    public List<LocatedBlock> getLocatedBlocks() {
      return locatedBlocks;
    }
  }

  /**
   * Sync task to rename file.
   */
  public static class RenameFileSyncTask extends SyncTask {
    private final URI renamedTo;
    private List<Block> blocks;
    public RenameFileSyncTask(UUID syncTaskId, URI uri, URI renamedTo,
        List<Block> blocks, String syncMountId) {
      super(syncTaskId, RENAME_FILE, uri, syncMountId);
      this.renamedTo = renamedTo;
      this.blocks = blocks;
    }

    public URI getRenamedTo() {
      return renamedTo;
    }

    /**
     * Get the final uri on remote storage. For rename task,
     * the final uri is the uri after rename.
     */
    public URI getFinalUri() {
      return getRenamedTo();
    }

    public List<Block> getBlocks() {
      return blocks;
    }
  }

  /**
   * Sync task to delete file.
   */
  public static class DeleteFileSyncTask extends SyncTask {
    private List<Block> blocks;
    public DeleteFileSyncTask(UUID syncTaskId, URI uri,
        List<Block> blocks, String syncMountId) {
      super(syncTaskId, DELETE_FILE, uri, syncMountId);
      this.blocks = blocks;
    }

    public List<Block> getBlocks() {
      return blocks;
    }
  }

  /**
   * Sync task to delete directory.
   */
  public static class DeleteDirectorySyncTask extends SyncTask {
    public DeleteDirectorySyncTask(UUID syncTaskId, URI uri,
        String syncMountId) {
      super(syncTaskId, DELETE_DIRECTORY, uri, syncMountId);
    }
  }

  /**
   * Sync task to create directory.
   */
  public static class CreateDirectorySyncTask extends SyncTask {
    public CreateDirectorySyncTask(UUID syncTaskId, URI uri,
        String syncMountId) {
      super(syncTaskId, CREATE_DIRECTORY, uri, syncMountId);
    }
  }

  /**
   * Sync task to rename directory.
   */
  public static class RenameDirectorySyncTask extends SyncTask {
    private final URI renamedTo;

    public RenameDirectorySyncTask(UUID syncTaskId, URI uri, URI renamedTo,
        String syncMountId) {
      super(syncTaskId, RENAME_DIRECTORY, uri, syncMountId);
      this.renamedTo = renamedTo;
    }

    public URI getRenamedTo() {
      return renamedTo;
    }
  }

  public static SyncTask createFile(URI uri,
      String syncMountId,
      List<LocatedBlock> locatedBlocks,
      long blockCollectionId) {
    return
        new CreateFileSyncTask(UUID.randomUUID(),
            uri,
            syncMountId,
            locatedBlocks,
            blockCollectionId);
  }

  public static SyncTask touchFile(URI uri, String syncMountId,
      long blockCollectionId) {
    return new TouchFileSyncTask(UUID.randomUUID(), uri, syncMountId,
        blockCollectionId);
  }


  public static SyncTask deleteFile(URI uri,
      List<Block> blocks, String syncMountId) {
    return new DeleteFileSyncTask(UUID.randomUUID(), uri, blocks,
        syncMountId);
  }

  public static SyncTask renameFile(URI uri, URI renamedTo,
      List<Block> blocks, String syncMountId) {
    return new RenameFileSyncTask(UUID.randomUUID(), uri, renamedTo,
        blocks, syncMountId);
  }

  public static CreateDirectorySyncTask createDirectory(URI uri,
      String syncMountId) {
    return new CreateDirectorySyncTask(UUID.randomUUID(), uri, syncMountId);
  }

  public static DeleteDirectorySyncTask deleteDirectory(URI uri,
      String syncMountId) {
    return new DeleteDirectorySyncTask(UUID.randomUUID(), uri, syncMountId);
  }

  public static RenameDirectorySyncTask renameDirectory(URI uri, URI renamedTo,
      String syncMountId) {
    return new RenameDirectorySyncTask(UUID.randomUUID(), uri, renamedTo,
        syncMountId);
  }

  @Override
  public String toString() {
    return "SyncTask{" +
        "syncTaskId=" + syncTaskId +
        ", operation=" + operation +
        ", uri=" + uri +
        ", syncMountId='" + syncMountId + '\'' +
        '}';
  }
}
