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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneProtos;
import org.apache.hadoop.scm.ScmConfigKeys;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class maintains the information about a container in the ozone world.
 * <p>
 * A container is a name, along with metadata- which is a set of key value
 * pair.
 */
public class ContainerData {

  private final String containerName;
  private final Map<String, String> metadata;
  private String dbPath;  // Path to Level DB Store.
  // Path to Physical file system where container and checksum are stored.
  private String containerFilePath;
  private boolean open;
  private String hash;
  private AtomicLong bytesUsed;
  private long maxSize;

  /**
   * Constructs a  ContainerData Object.
   *
   * @param containerName - Name
   */
  public ContainerData(String containerName, Configuration conf) {
    this.metadata = new TreeMap<>();
    this.containerName = containerName;
    this.open = true;
    this.maxSize = conf.getLong(ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_KEY,
        ScmConfigKeys.SCM_CONTAINER_CLIENT_MAX_SIZE_DEFAULT) * OzoneConsts.GB;
    this.bytesUsed =  new AtomicLong(0L);

  }

  /**
   * Constructs a ContainerData object from ProtoBuf classes.
   *
   * @param protoData - ProtoBuf Message
   * @throws IOException
   */
  public static ContainerData getFromProtBuf(
      ContainerProtos.ContainerData protoData, Configuration conf)
      throws IOException {
    ContainerData data = new ContainerData(protoData.getName(), conf);
    for (int x = 0; x < protoData.getMetadataCount(); x++) {
      data.addMetadata(protoData.getMetadata(x).getKey(),
          protoData.getMetadata(x).getValue());
    }

    if (protoData.hasContainerPath()) {
      data.setContainerPath(protoData.getContainerPath());
    }

    if (protoData.hasDbPath()) {
      data.setDBPath(protoData.getDbPath());
    }

    if (protoData.hasOpen()) {
      data.setOpen(protoData.getOpen());
    } else {
      data.setOpen(true);
    }

    if(protoData.hasHash()) {
      data.setHash(protoData.getHash());
    }

    if (protoData.hasBytesUsed()) {
      data.setBytesUsed(protoData.getBytesUsed());
    }

    if (protoData.hasSize()) {
      data.setMaxSize(protoData.getSize());
    }
    return data;
  }

  /**
   * Returns a ProtoBuf Message from ContainerData.
   *
   * @return Protocol Buffer Message
   */
  public ContainerProtos.ContainerData getProtoBufMessage() {
    ContainerProtos.ContainerData.Builder builder = ContainerProtos
        .ContainerData.newBuilder();
    builder.setName(this.getContainerName());

    if (this.getDBPath() != null) {
      builder.setDbPath(this.getDBPath());
    }

    if (this.getHash() != null) {
      builder.setHash(this.getHash());
    }

    if (this.getContainerPath() != null) {
      builder.setContainerPath(this.getContainerPath());
    }

    builder.setOpen(this.isOpen());

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      OzoneProtos.KeyValue.Builder keyValBuilder =
          OzoneProtos.KeyValue.newBuilder();
      builder.addMetadata(keyValBuilder.setKey(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    if (this.getBytesUsed() >= 0) {
      builder.setBytesUsed(this.getBytesUsed());
    }

    if (this.getMaxSize() >= 0) {
      builder.setSize(this.getMaxSize());
    }

    return builder.build();
  }

  /**
   * Returns the name of the container.
   *
   * @return - name
   */
  public String getContainerName() {
    return containerName;
  }

  /**
   * Adds metadata.
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
   * Returns all metadata.
   */
  public Map<String, String> getAllMetadata() {
    synchronized (this.metadata) {
      return Collections.unmodifiableMap(this.metadata);
    }
  }

  /**
   * Returns value of a key.
   */
  public String getValue(String key) {
    synchronized (this.metadata) {
      return metadata.get(key);
    }
  }

  /**
   * Deletes a metadata entry from the map.
   *
   * @param key - Key
   */
  public void deleteKey(String key) {
    synchronized (this.metadata) {
      metadata.remove(key);
    }
  }

  /**
   * Returns path.
   *
   * @return - path
   */
  public String getDBPath() {
    return dbPath;
  }

  /**
   * Sets path.
   *
   * @param path - String.
   */
  public void setDBPath(String path) {
    this.dbPath = path;
  }

  /**
   * This function serves as the generic key for ContainerCache class. Both
   * ContainerData and ContainerKeyData overrides this function to appropriately
   * return the right name that can  be used in ContainerCache.
   *
   * @return String Name.
   */
  public String getName() {
    return getContainerName();
  }

  /**
   * Get container file path.
   * @return - Physical path where container file and checksum is stored.
   */
  public String getContainerPath() {
    return containerFilePath;
  }

  /**
   * Set container Path.
   * @param containerPath - File path.
   */
  public void setContainerPath(String containerPath) {
    this.containerFilePath = containerPath;
  }

  /**
   * checks if the container is open.
   * @return - boolean
   */
  public synchronized  boolean isOpen() {
    return open;
  }

  /**
   * Marks this container as closed.
   */
  public synchronized void closeContainer() {
    setOpen(false);

    // Some thing brain dead for now. name + Time stamp of when we get the close
    // container message.
    setHash(DigestUtils.sha256Hex(this.getContainerName() +
        Long.toString(Time.monotonicNow())));
  }

  /**
   * Final hash for this container.
   * @return - Hash
   */
  public String getHash() {
    return hash;
  }

  public void setHash(String hash) {
    this.hash = hash;
  }

  /**
   * Sets the open or closed values.
   * @param open
   */
  public synchronized void setOpen(boolean open) {
    this.open = open;
  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public long getMaxSize() {
    return maxSize;
  }

  public long getKeyCount() {
    return metadata.size();
  }

  public void setBytesUsed(long used) {
    this.bytesUsed.set(used);
  }

  public long addBytesUsed(long delta) {
    return this.bytesUsed.addAndGet(delta);
  }

  public long getBytesUsed() {
    return bytesUsed.get();
  }
}
