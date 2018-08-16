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

package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.yaml.snakeyaml.nodes.Tag;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.CHUNKS_PATH;
import static org.apache.hadoop.ozone.OzoneConsts.CONTAINER_DB_TYPE;
import static org.apache.hadoop.ozone.OzoneConsts.METADATA_PATH;

/**
 * This class represents the KeyValueContainer metadata, which is the
 * in-memory representation of container metadata and is represented on disk
 * by the .container file.
 */
public class KeyValueContainerData extends ContainerData {

  // Yaml Tag used for KeyValueContainerData.
  public static final Tag KEYVALUE_YAML_TAG = new Tag("KeyValueContainerData");

  // Fields need to be stored in .container file.
  private static final List<String> KV_YAML_FIELDS;

  // Path to Container metadata Level DB/RocksDB Store and .container file.
  private String metadataPath;

  // Path to Physical file system where chunks are stored.
  private String chunksPath;

  //Type of DB used to store key to chunks mapping
  private String containerDBType;

  private File dbFile = null;

  static {
    // Initialize YAML fields
    KV_YAML_FIELDS = Lists.newArrayList();
    KV_YAML_FIELDS.addAll(YAML_FIELDS);
    KV_YAML_FIELDS.add(METADATA_PATH);
    KV_YAML_FIELDS.add(CHUNKS_PATH);
    KV_YAML_FIELDS.add(CONTAINER_DB_TYPE);
  }

  /**
   * Constructs KeyValueContainerData object.
   * @param id - ContainerId
   * @param size - maximum size of the container
   */
  public KeyValueContainerData(long id, int size) {
    super(ContainerProtos.ContainerType.KeyValueContainer, id, size);
  }

  /**
   * Constructs KeyValueContainerData object.
   * @param id - ContainerId
   * @param layOutVersion
   * @param size - maximum size of the container
   */
  public KeyValueContainerData(long id, int layOutVersion, int size) {
    super(ContainerProtos.ContainerType.KeyValueContainer, id, layOutVersion,
        size);
  }


  /**
   * Sets Container dbFile. This should be called only during creation of
   * KeyValue container.
   * @param containerDbFile
   */
  public void setDbFile(File containerDbFile) {
    dbFile = containerDbFile;
  }

  /**
   * Returns container DB file.
   * @return dbFile
   */
  public File getDbFile() {
    return dbFile;
  }

  /**
   * Returns container metadata path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getMetadataPath() {
    return metadataPath;
  }

  /**
   * Sets container metadata path.
   *
   * @param path - String.
   */
  public void setMetadataPath(String path) {
    this.metadataPath = path;
  }

  /**
   * Returns the path to base dir of the container.
   * @return Path to base dir
   */
  public String getContainerPath() {
    if (metadataPath == null) {
      return null;
    }
    return new File(metadataPath).getParent();
  }

  /**
   * Get chunks path.
   * @return - Path where chunks are stored
   */
  public String getChunksPath() {
    return chunksPath;
  }

  /**
   * Set chunks Path.
   * @param chunkPath - File path.
   */
  public void setChunksPath(String chunkPath) {
    this.chunksPath = chunkPath;
  }

  /**
   * Returns the DBType used for the container.
   * @return containerDBType
   */
  public String getContainerDBType() {
    return containerDBType;
  }

  /**
   * Sets the DBType used for the container.
   * @param containerDBType
   */
  public void setContainerDBType(String containerDBType) {
    this.containerDBType = containerDBType;
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public ContainerProtos.ContainerData getProtoBufMessage() {
    ContainerProtos.ContainerData.Builder builder = ContainerProtos
        .ContainerData.newBuilder();
    builder.setContainerID(this.getContainerID());
    builder.setContainerPath(this.getMetadataPath());
    builder.setState(this.getState());

    for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
      ContainerProtos.KeyValue.Builder keyValBuilder =
          ContainerProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if(this.getContainerType() != null) {
      builder.setContainerType(ContainerProtos.ContainerType.KeyValueContainer);
    }

    return builder.build();
  }

  public static List<String> getYamlFields() {
    return Collections.unmodifiableList(KV_YAML_FIELDS);
  }

  /**
   * Constructs a KeyValueContainerData object from ProtoBuf classes.
   *
   * @param protoData - ProtoBuf Message
   * @throws IOException
   */
  @VisibleForTesting
  public static KeyValueContainerData getFromProtoBuf(
      ContainerProtos.ContainerData protoData) throws IOException {
    // TODO: Add containerMaxSize to ContainerProtos.ContainerData
    KeyValueContainerData data = new KeyValueContainerData(
        protoData.getContainerID(),
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT);
    for (int x = 0; x < protoData.getMetadataCount(); x++) {
      data.addMetadata(protoData.getMetadata(x).getKey(),
          protoData.getMetadata(x).getValue());
    }

    if (protoData.hasContainerPath()) {
      String metadataPath = protoData.getContainerPath()+ File.separator +
          OzoneConsts.CONTAINER_META_PATH;
      data.setMetadataPath(metadataPath);
    }

    if (protoData.hasState()) {
      data.setState(protoData.getState());
    }

    if (protoData.hasBytesUsed()) {
      data.setBytesUsed(protoData.getBytesUsed());
    }

    return data;
  }
}
