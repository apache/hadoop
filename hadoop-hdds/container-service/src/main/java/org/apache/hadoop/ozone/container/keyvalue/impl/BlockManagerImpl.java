/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;

import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_BLOCK;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.UNKNOWN_BCSID;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.BCSID_MISMATCH;
/**
 * This class is for performing block related operations on the KeyValue
 * Container.
 */
public class BlockManagerImpl implements BlockManager {

  static final Logger LOG = LoggerFactory.getLogger(BlockManagerImpl.class);
  private static byte[] blockCommitSequenceIdKey =
          DFSUtil.string2Bytes(OzoneConsts.BLOCK_COMMIT_SEQUENCE_ID_PREFIX);

  private Configuration config;

  /**
   * Constructs a Block Manager.
   *
   * @param conf - Ozone configuration
   */
  public BlockManagerImpl(Configuration conf) {
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.config = conf;
  }

  /**
   * Puts or overwrites a block.
   *
   * @param container - Container for which block need to be added.
   * @param data     - BlockData.
   * @return length of the block.
   * @throws IOException
   */
  public long putBlock(Container container, BlockData data) throws IOException {
    Preconditions.checkNotNull(data, "BlockData cannot be null for put " +
        "operation.");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id " +
        "cannot be negative");
    // We are not locking the key manager since LevelDb serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    MetadataStore db = BlockUtils.getDB((KeyValueContainerData) container
        .getContainerData(), config);

    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");

    long bcsId = data.getBlockCommitSequenceId();
    long containerBCSId = ((KeyValueContainerData) container.getContainerData())
        .getBlockCommitSequenceId();

    // default blockCommitSequenceId for any block is 0. It the putBlock
    // request is not coming via Ratis(for test scenarios), it will be 0.
    // In such cases, we should overwrite the block as well
    if (bcsId != 0) {
      if (bcsId <= containerBCSId) {
        // Since the blockCommitSequenceId stored in the db is greater than
        // equal to blockCommitSequenceId to be updated, it means the putBlock
        // transaction is reapplied in the ContainerStateMachine on restart.
        // It also implies that the given block must already exist in the db.
        // just log and return
        LOG.warn("blockCommitSequenceId " + containerBCSId
            + " in the Container Db is greater than" + " the supplied value "
            + bcsId + " .Ignoring it");
        return data.getSize();
      }
    }
    // update the blockData as well as BlockCommitSequenceId here
    BatchOperation batch = new BatchOperation();
    batch.put(Longs.toByteArray(data.getLocalID()),
        data.getProtoBufMessage().toByteArray());
    batch.put(blockCommitSequenceIdKey,
        Longs.toByteArray(bcsId));
    db.writeBatch(batch);
    container.updateBlockCommitSequenceId(bcsId);
    // Increment keycount here
    container.getContainerData().incrKeyCount();
    LOG.debug(
        "Block " + data.getBlockID() + " successfully committed with bcsId "
            + bcsId + " chunk size " + data.getChunks().size());
    return data.getSize();
  }

  /**
   * Gets an existing block.
   *
   * @param container - Container from which block need to be fetched.
   * @param blockID - BlockID of the block.
   * @return Key Data.
   * @throws IOException
   */
  @Override
  public BlockData getBlock(Container container, BlockID blockID)
      throws IOException {
    long bcsId = blockID.getBlockCommitSequenceId();
    Preconditions.checkNotNull(blockID,
        "BlockID cannot be null in GetBlock request");
    Preconditions.checkNotNull(container,
        "Container cannot be null");

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = BlockUtils.getDB(containerData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");

    long containerBCSId = containerData.getBlockCommitSequenceId();
    if (containerBCSId < bcsId) {
      throw new StorageContainerException(
          "Unable to find the block with bcsID " + bcsId + " .Container "
              + container.getContainerData().getContainerID() + " bcsId is "
              + containerBCSId + ".", UNKNOWN_BCSID);
    }
    byte[] kData = db.get(Longs.toByteArray(blockID.getLocalID()));
    if (kData == null) {
      throw new StorageContainerException("Unable to find the block." + blockID,
          NO_SUCH_BLOCK);
    }
    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.parseFrom(kData);
    long id = blockData.getBlockID().getBlockCommitSequenceId();
    if (id < bcsId) {
      throw new StorageContainerException(
          "bcsId " + bcsId + " mismatches with existing block Id "
              + id + " for block " + blockID + ".", BCSID_MISMATCH);
    }
    return BlockData.getFromProtoBuf(blockData);
  }

  /**
   * Returns the length of the committed block.
   *
   * @param container - Container from which block need to be fetched.
   * @param blockID - BlockID of the block.
   * @return length of the block.
   * @throws IOException in case, the block key does not exist in db.
   */
  @Override
  public long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = BlockUtils.getDB(containerData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    byte[] kData = db.get(Longs.toByteArray(blockID.getLocalID()));
    if (kData == null) {
      throw new StorageContainerException("Unable to find the block.",
          NO_SUCH_BLOCK);
    }
    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.parseFrom(kData);
    return blockData.getSize();
  }

  /**
   * Deletes an existing block.
   *
   * @param container - Container from which block need to be deleted.
   * @param blockID - ID of the block.
   * @throws StorageContainerException
   */
  public void deleteBlock(Container container, BlockID blockID) throws
      IOException {
    Preconditions.checkNotNull(blockID, "block ID cannot be null.");
    Preconditions.checkState(blockID.getContainerID() >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(blockID.getLocalID() >= 0,
        "Local ID cannot be negative.");

    KeyValueContainerData cData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = BlockUtils.getDB(cData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    // Note : There is a race condition here, since get and delete
    // are not atomic. Leaving it here since the impact is refusing
    // to delete a Block which might have just gotten inserted after
    // the get check.
    byte[] kKey = Longs.toByteArray(blockID.getLocalID());
    byte[] kData = db.get(kKey);
    if (kData == null) {
      throw new StorageContainerException("Unable to find the block.",
          NO_SUCH_BLOCK);
    }
    db.delete(kKey);

    // Decrement blockcount here
    container.getContainerData().decrKeyCount();
  }

  /**
   * List blocks in a container.
   *
   * @param container - Container from which blocks need to be listed.
   * @param startLocalID  - Key to start from, 0 to begin.
   * @param count    - Number of blocks to return.
   * @return List of Blocks that match the criteria.
   */
  @Override
  public List<BlockData> listBlock(Container container, long startLocalID, int
      count) throws IOException {
    Preconditions.checkNotNull(container, "container cannot be null");
    Preconditions.checkState(startLocalID >= 0, "startLocal ID cannot be " +
        "negative");
    Preconditions.checkArgument(count > 0,
        "Count must be a positive number.");
    container.readLock();
    List<BlockData> result = null;
    KeyValueContainerData cData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = BlockUtils.getDB(cData, config);
    result = new ArrayList<>();
    byte[] startKeyInBytes = Longs.toByteArray(startLocalID);
    List<Map.Entry<byte[], byte[]>> range =
        db.getSequentialRangeKVs(startKeyInBytes, count,
            MetadataKeyFilters.getNormalKeyFilter());
    for (Map.Entry<byte[], byte[]> entry : range) {
      BlockData value = BlockUtils.getBlockData(entry.getValue());
      BlockData data = new BlockData(value.getBlockID());
      result.add(data);
    }
    return result;
  }

  /**
   * Shutdown KeyValueContainerManager.
   */
  public void shutdown() {
    BlockUtils.shutdownCache(ContainerCache.getInstance(config));
  }

}
