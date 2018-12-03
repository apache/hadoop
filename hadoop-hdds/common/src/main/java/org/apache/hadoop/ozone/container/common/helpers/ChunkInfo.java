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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;

/**
 * Java class that represents ChunkInfo ProtoBuf class. This helper class allows
 * us to convert to and from protobuf to normal java.
 */
public class ChunkInfo {
  private final String chunkName;
  private final long offset;
  private final long len;
  private ChecksumData checksumData;
  private final Map<String, String> metadata;


  /**
   * Constructs a ChunkInfo.
   *
   * @param chunkName - File Name where chunk lives.
   * @param offset    - offset where Chunk Starts.
   * @param len       - Length of the Chunk.
   */
  public ChunkInfo(String chunkName, long offset, long len) {
    this.chunkName = chunkName;
    this.offset = offset;
    this.len = len;
    this.metadata = new TreeMap<>();
  }

  /**
   * Adds metadata.
   *
   * @param key   - Key Name.
   * @param value - Value.
   * @throws IOException
   */
  public void addMetadata(String key, String value) throws IOException {
    synchronized (this.metadata) {
      if (this.metadata.containsKey(key)) {
        throw new IOException("This key already exists. Key " + key);
      }
      metadata.put(key, value);
    }
  }

  /**
   * Gets a Chunkinfo class from the protobuf definitions.
   *
   * @param info - Protobuf class
   * @return ChunkInfo
   * @throws IOException
   */
  public static ChunkInfo getFromProtoBuf(ContainerProtos.ChunkInfo info)
      throws IOException {
    Preconditions.checkNotNull(info);

    ChunkInfo chunkInfo = new ChunkInfo(info.getChunkName(), info.getOffset(),
        info.getLen());

    for (int x = 0; x < info.getMetadataCount(); x++) {
      chunkInfo.addMetadata(info.getMetadata(x).getKey(),
          info.getMetadata(x).getValue());
    }

    chunkInfo.setChecksumData(
        ChecksumData.getFromProtoBuf(info.getChecksumData()));

    return chunkInfo;
  }

  /**
   * Returns a ProtoBuf Message from ChunkInfo.
   *
   * @return Protocol Buffer Message
   */
  public ContainerProtos.ChunkInfo getProtoBufMessage() {
    ContainerProtos.ChunkInfo.Builder builder = ContainerProtos
        .ChunkInfo.newBuilder();

    builder.setChunkName(this.getChunkName());
    builder.setOffset(this.getOffset());
    builder.setLen(this.getLen());
    if (checksumData == null) {
      // ChecksumData cannot be null while computing the protobufMessage.
      // Set it to NONE type (equivalent to non checksum).
      builder.setChecksumData(Checksum.getNoChecksumDataProto());
    } else {
      builder.setChecksumData(this.checksumData.getProtoBufMessage());
    }

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    return builder.build();
  }

  /**
   * Returns the chunkName.
   *
   * @return - String
   */
  public String getChunkName() {
    return chunkName;
  }

  /**
   * Gets the start offset of the given chunk in physical file.
   *
   * @return - long
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Returns the length of the Chunk.
   *
   * @return long
   */
  public long getLen() {
    return len;
  }

  /**
   * Returns the checksumData of this chunk.
   */
  public ChecksumData getChecksumData() {
    return checksumData;
  }

  /**
   * Sets the checksums of this chunk.
   */
  public void setChecksumData(ChecksumData cData) {
    this.checksumData = cData;
  }

  /**
   * Returns Metadata associated with this Chunk.
   *
   * @return - Map of Key,values.
   */
  public Map<String, String> getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "ChunkInfo{" +
        "chunkName='" + chunkName +
        ", offset=" + offset +
        ", len=" + len +
        '}';
  }
}
