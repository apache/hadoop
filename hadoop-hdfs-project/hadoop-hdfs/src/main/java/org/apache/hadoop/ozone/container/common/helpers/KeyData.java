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

import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Helper class to convert Protobuf to Java classes.
 */
public class KeyData {
  private final String containerName;
  private final String keyName;
  private final Map<String, String> metadata;

  /**
   * Please note : when we are working with keys, we don't care what they point
   * to. So we We don't read chunkinfo nor validate them. It is responsibility
   * of higher layer like ozone. We just read and write data from network.
   */
  private List<ContainerProtos.ChunkInfo> chunks;

  /**
   * Constructs a KeyData Object.
   *
   * @param containerName
   * @param keyName
   */
  public KeyData(String containerName, String keyName) {
    this.containerName = containerName;
    this.keyName = keyName;
    this.metadata = new TreeMap<>();
  }

  /**
   * Returns a keyData object from the protobuf data.
   *
   * @param data - Protobuf data.
   * @return - KeyData
   * @throws IOException
   */
  public static KeyData getFromProtoBuf(ContainerProtos.KeyData data) throws
      IOException {
    KeyData keyData = new KeyData(data.getContainerName(), data.getName());
    for (int x = 0; x < data.getMetadataCount(); x++) {
      keyData.addMetadata(data.getMetadata(x).getKey(),
          data.getMetadata(x).getValue());
    }
    keyData.setChunks(data.getChunksList());
    return keyData;
  }

  /**
   * Returns a Protobuf message from KeyData.
   * @return Proto Buf Message.
   */
  public ContainerProtos.KeyData getProtoBufMessage() {
    ContainerProtos.KeyData.Builder builder =
        ContainerProtos.KeyData.newBuilder();
    builder.setContainerName(this.containerName);
    builder.setName(this.getKeyName());
    builder.addAllChunks(this.chunks);
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      OzoneProtos.KeyValue.Builder keyValBuilder =
          OzoneProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }
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

  /**
   * Returns chunks list.
   *
   * @return list of chunkinfo.
   */
  public List<ContainerProtos.ChunkInfo> getChunks() {
    return chunks;
  }

  /**
   * Returns container Name.
   * @return String.
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Returns KeyName.
   * @return String.
   */
  public String getKeyName() {
    return keyName;
  }

  /**
   * Sets Chunk list.
   *
   * @param chunks - List of chunks.
   */
  public void setChunks(List<ContainerProtos.ChunkInfo> chunks) {
    this.chunks = chunks;
  }

  /**
   * Get the total size of chunks allocated for the key.
   * @return total size of the key.
   */
  public long getSize() {
    return chunks.parallelStream().mapToLong(e->e.getLen()).sum();
  }

}
