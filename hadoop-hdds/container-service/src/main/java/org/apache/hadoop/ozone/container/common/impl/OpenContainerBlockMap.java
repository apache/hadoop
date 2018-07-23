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

package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class will maintain list of open keys per container when closeContainer
 * command comes, it should autocommit all open keys of a open container before
 * marking the container as closed.
 */
public class OpenContainerBlockMap {

  /**
   * TODO : We may construct the openBlockMap by reading the Block Layout
   * for each block inside a container listing all chunk files and reading the
   * sizes. This will help to recreate the openKeys Map once the DataNode
   * restarts.
   *
   * For now, we will track all open blocks of a container in the blockMap.
   */
  private final ConcurrentHashMap<Long, HashMap<Long, KeyData>>
      openContainerBlockMap;

  /**
   * Constructs OpenContainerBlockMap.
   */
  public OpenContainerBlockMap() {
     openContainerBlockMap = new ConcurrentHashMap<>();
  }
  /**
   * Removes the Container matching with specified containerId.
   * @param containerId containerId
   */
  public void removeContainer(long containerId) {
    Preconditions
        .checkState(containerId >= 0, "Container Id cannot be negative.");
    openContainerBlockMap.computeIfPresent(containerId, (k, v) -> null);
  }

  /**
   * updates the chunkInfoList in case chunk is added or deleted
   * @param blockID id of the block.
   * @param info - Chunk Info
   * @param remove if true, deletes the chunkInfo list otherwise appends to the
   *               chunkInfo List
   * @throws IOException
   */
  public synchronized void updateOpenKeyMap(BlockID blockID,
      ContainerProtos.ChunkInfo info, boolean remove) throws IOException {
    if (remove) {
      deleteChunkFromMap(blockID, info);
    } else {
      addChunkToMap(blockID, info);
    }
  }

  private KeyData getKeyData(ContainerProtos.ChunkInfo info, BlockID blockID)
      throws IOException {
    KeyData keyData = new KeyData(blockID);
    keyData.addMetadata("TYPE", "KEY");
    keyData.addChunk(info);
    return keyData;
  }

  private void addChunkToMap(BlockID blockID, ContainerProtos.ChunkInfo info)
      throws IOException {
    Preconditions.checkNotNull(info);
    long containerId = blockID.getContainerID();
    long localID = blockID.getLocalID();

    KeyData keyData = openContainerBlockMap.computeIfAbsent(containerId,
        emptyMap -> new LinkedHashMap<Long, KeyData>())
        .putIfAbsent(localID, getKeyData(info, blockID));
    // KeyData != null means the block already exist
    if (keyData != null) {
      HashMap<Long, KeyData> keyDataSet =
          openContainerBlockMap.get(containerId);
      keyDataSet.putIfAbsent(blockID.getLocalID(), getKeyData(info, blockID));
      keyDataSet.computeIfPresent(blockID.getLocalID(), (key, value) -> {
        value.addChunk(info);
        return value;
      });
    }
  }

  /**
   * removes the chunks from the chunkInfo list for the given block.
   * @param blockID id of the block
   * @param chunkInfo chunk info.
   */
  private synchronized void deleteChunkFromMap(BlockID blockID,
      ContainerProtos.ChunkInfo chunkInfo) {
    Preconditions.checkNotNull(chunkInfo);
    Preconditions.checkNotNull(blockID);
    HashMap<Long, KeyData> keyDataMap =
        openContainerBlockMap.get(blockID.getContainerID());
    if (keyDataMap != null) {
      long localId = blockID.getLocalID();
      KeyData keyData = keyDataMap.get(localId);
      if (keyData != null) {
        keyData.removeChunk(chunkInfo);
      }
    }
  }

  /**
   * returns the list of open to the openContainerBlockMap
   * @param containerId container id
   * @return List of open Keys(blocks)
   */
  public List<KeyData> getOpenKeys(long containerId) {
    HashMap<Long, KeyData> keyDataHashMap =
        openContainerBlockMap.get(containerId);
    return keyDataHashMap == null ? null :
        keyDataHashMap.values().stream().collect(Collectors.toList());
  }

  /**
   * removes the block from the block map.
   * @param blockID
   */
  public synchronized void removeFromKeyMap(BlockID blockID) {
    Preconditions.checkNotNull(blockID);
    HashMap<Long, KeyData> keyDataMap =
        openContainerBlockMap.get(blockID.getContainerID());
    if (keyDataMap != null) {
      keyDataMap.remove(blockID.getLocalID());
      if (keyDataMap.size() == 0) {
        removeContainer(blockID.getContainerID());
      }
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<Long,
      HashMap<Long, KeyData>> getContainerOpenKeyMap() {
    return openContainerBlockMap;
  }
}
