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
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.client.BlockID;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

/**
 * Helper class to convert Protobuf to Java classes.
 */
public class BlockData {
  private final BlockID blockID;
  private final Map<String, String> metadata;

  /**
   * Represent a list of chunks.
   * In order to reduce memory usage, chunkList is declared as an
   * {@link Object}.
   * When #elements == 0, chunkList is null.
   * When #elements == 1, chunkList refers to the only element.
   * When #elements > 1, chunkList refers to the list.
   *
   * Please note : when we are working with blocks, we don't care what they
   * point to. So we We don't read chunkinfo nor validate them. It is
   * responsibility of higher layer like ozone. We just read and write data
   * from network.
   */
  private Object chunkList;

  /**
   * total size of the key.
   */
  private long size;

  /**
   * Constructs a BlockData Object.
   *
   * @param blockID
   */
  public BlockData(BlockID blockID) {
    this.blockID = blockID;
    this.metadata = new TreeMap<>();
    this.size = 0;
  }

  public long getBlockCommitSequenceId() {
    return blockID.getBlockCommitSequenceId();
  }

  public void setBlockCommitSequenceId(long blockCommitSequenceId) {
    this.blockID.setBlockCommitSequenceId(blockCommitSequenceId);
  }

  /**
   * Returns a blockData object from the protobuf data.
   *
   * @param data - Protobuf data.
   * @return - BlockData
   * @throws IOException
   */
  public static BlockData getFromProtoBuf(ContainerProtos.BlockData data) throws
      IOException {
    BlockData blockData = new BlockData(
        BlockID.getFromProtobuf(data.getBlockID()));
    for (int x = 0; x < data.getMetadataCount(); x++) {
      blockData.addMetadata(data.getMetadata(x).getKey(),
          data.getMetadata(x).getValue());
    }
    blockData.setChunks(data.getChunksList());
    if (data.hasSize()) {
      Preconditions.checkArgument(data.getSize() == blockData.getSize());
    }
    return blockData;
  }

  /**
   * Returns a Protobuf message from BlockData.
   * @return Proto Buf Message.
   */
  public ContainerProtos.BlockData getProtoBufMessage() {
    ContainerProtos.BlockData.Builder builder =
        ContainerProtos.BlockData.newBuilder();
    builder.setBlockID(this.blockID.getDatanodeBlockIDProtobuf());
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }
    builder.addAllChunks(getChunks());
    builder.setSize(size);
    return builder.build();
  }

  /**
   * Adds metadata.
   *
   * @param key   - Key
   * @param value - Value
   * @throws IOException
   */
  public synchronized void addMetadata(String key, String value) throws
      IOException {
    if (this.metadata.containsKey(key)) {
      throw new IOException("This key already exists. Key " + key);
    }
    metadata.put(key, value);
  }

  public synchronized Map<String, String> getMetadata() {
    return Collections.unmodifiableMap(this.metadata);
  }

  /**
   * Returns value of a key.
   */
  public synchronized String getValue(String key) {
    return metadata.get(key);
  }

  /**
   * Deletes a metadata entry from the map.
   *
   * @param key - Key
   */
  public synchronized void deleteKey(String key) {
    metadata.remove(key);
  }

  @SuppressWarnings("unchecked")
  private List<ContainerProtos.ChunkInfo> castChunkList() {
    return (List<ContainerProtos.ChunkInfo>)chunkList;
  }

  /**
   * Returns chunks list.
   *
   * @return list of chunkinfo.
   */
  public List<ContainerProtos.ChunkInfo> getChunks() {
    return chunkList == null? Collections.emptyList()
        : chunkList instanceof ContainerProtos.ChunkInfo?
            Collections.singletonList((ContainerProtos.ChunkInfo)chunkList)
        : Collections.unmodifiableList(castChunkList());
  }

  /**
   * Adds chinkInfo to the list.
   */
  public void addChunk(ContainerProtos.ChunkInfo chunkInfo) {
    if (chunkList == null) {
      chunkList = chunkInfo;
    } else {
      final List<ContainerProtos.ChunkInfo> list;
      if (chunkList instanceof ContainerProtos.ChunkInfo) {
        list = new ArrayList<>(2);
        list.add((ContainerProtos.ChunkInfo)chunkList);
        chunkList = list;
      } else {
        list = castChunkList();
      }
      list.add(chunkInfo);
    }
    size += chunkInfo.getLen();
  }

  /**
   * removes the chunk.
   */
  public boolean removeChunk(ContainerProtos.ChunkInfo chunkInfo) {
    final boolean removed;
    if (chunkList instanceof List) {
      final List<ContainerProtos.ChunkInfo> list = castChunkList();
      removed = list.remove(chunkInfo);
      if (list.size() == 1) {
        chunkList = list.get(0);
      }
    } else if (chunkInfo.equals(chunkList)) {
      chunkList = null;
      removed = true;
    } else {
      removed = false;
    }

    if (removed) {
      size -= chunkInfo.getLen();
    }
    return removed;
  }

  /**
   * Returns container ID.
   *
   * @return long.
   */
  public long getContainerID() {
    return blockID.getContainerID();
  }

  /**
   * Returns LocalID.
   * @return long.
   */
  public long getLocalID() {
    return blockID.getLocalID();
  }

  /**
   * Return Block ID.
   * @return BlockID.
   */
  public BlockID getBlockID() {
    return blockID;
  }

  /**
   * Sets Chunk list.
   *
   * @param chunks - List of chunks.
   */
  public void setChunks(List<ContainerProtos.ChunkInfo> chunks) {
    if (chunks == null) {
      chunkList = null;
      size = 0L;
    } else {
      final int n = chunks.size();
      chunkList = n == 0? null: n == 1? chunks.get(0): chunks;
      size = chunks.parallelStream().mapToLong(
          ContainerProtos.ChunkInfo::getLen).sum();
    }
  }

  /**
   * Get the total size of chunks allocated for the key.
   * @return total size of the key.
   */
  public long getSize() {
    return size;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("blockId", blockID.toString())
        .append("size", this.size)
        .toString();
  }
}
