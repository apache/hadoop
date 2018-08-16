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

import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Result.NO_SUCH_KEY;

/**
 * This class is for performing key related operations on the KeyValue
 * Container.
 */
public class KeyManagerImpl implements KeyManager {

  static final Logger LOG = LoggerFactory.getLogger(KeyManagerImpl.class);

  private Configuration config;

  /**
   * Constructs a key Manager.
   *
   * @param conf - Ozone configuration
   */
  public KeyManagerImpl(Configuration conf) {
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.config = conf;
  }

  /**
   * Puts or overwrites a key.
   *
   * @param container - Container for which key need to be added.
   * @param data     - Key Data.
   * @return length of the key.
   * @throws IOException
   */
  public long putKey(Container container, KeyData data) throws IOException {
    Preconditions.checkNotNull(data, "KeyData cannot be null for put " +
        "operation.");
    Preconditions.checkState(data.getContainerID() >= 0, "Container Id " +
        "cannot be negative");
    // We are not locking the key manager since LevelDb serializes all actions
    // against a single DB. We rely on DB level locking to avoid conflicts.
    MetadataStore db = KeyUtils.getDB((KeyValueContainerData) container
        .getContainerData(), config);

    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    db.put(Longs.toByteArray(data.getLocalID()), data.getProtoBufMessage()
        .toByteArray());

    // Increment keycount here
    container.getContainerData().incrKeyCount();
    return data.getSize();
  }

  /**
   * Gets an existing key.
   *
   * @param container - Container from which key need to be get.
   * @param blockID - BlockID of the key.
   * @return Key Data.
   * @throws IOException
   */
  public KeyData getKey(Container container, BlockID blockID)
      throws IOException {
    Preconditions.checkNotNull(blockID,
        "BlockID cannot be null in GetKet request");
    Preconditions.checkNotNull(blockID.getContainerID(),
        "Container name cannot be null");

    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = KeyUtils.getDB(containerData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    byte[] kData = db.get(Longs.toByteArray(blockID.getLocalID()));
    if (kData == null) {
      throw new StorageContainerException("Unable to find the key.",
          NO_SUCH_KEY);
    }
    ContainerProtos.KeyData keyData = ContainerProtos.KeyData.parseFrom(kData);
    return KeyData.getFromProtoBuf(keyData);
  }

  /**
   * Returns the length of the committed block.
   *
   * @param container - Container from which key need to be get.
   * @param blockID - BlockID of the key.
   * @return length of the block.
   * @throws IOException in case, the block key does not exist in db.
   */
  @Override
  public long getCommittedBlockLength(Container container, BlockID blockID)
      throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = KeyUtils.getDB(containerData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    byte[] kData = db.get(Longs.toByteArray(blockID.getLocalID()));
    if (kData == null) {
      throw new StorageContainerException("Unable to find the key.",
          NO_SUCH_KEY);
    }
    ContainerProtos.KeyData keyData = ContainerProtos.KeyData.parseFrom(kData);
    return keyData.getSize();
  }

  /**
   * Deletes an existing Key.
   *
   * @param container - Container from which key need to be deleted.
   * @param blockID - ID of the block.
   * @throws StorageContainerException
   */
  public void deleteKey(Container container, BlockID blockID) throws
      IOException {
    Preconditions.checkNotNull(blockID, "block ID cannot be null.");
    Preconditions.checkState(blockID.getContainerID() >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(blockID.getLocalID() >= 0,
        "Local ID cannot be negative.");

    KeyValueContainerData cData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = KeyUtils.getDB(cData, config);
    // This is a post condition that acts as a hint to the user.
    // Should never fail.
    Preconditions.checkNotNull(db, "DB cannot be null here");
    // Note : There is a race condition here, since get and delete
    // are not atomic. Leaving it here since the impact is refusing
    // to delete a key which might have just gotten inserted after
    // the get check.
    byte[] kKey = Longs.toByteArray(blockID.getLocalID());
    byte[] kData = db.get(kKey);
    if (kData == null) {
      throw new StorageContainerException("Unable to find the key.",
          NO_SUCH_KEY);
    }
    db.delete(kKey);

    // Decrement keycount here
    container.getContainerData().decrKeyCount();
  }

  /**
   * List keys in a container.
   *
   * @param container - Container from which keys need to be listed.
   * @param startLocalID  - Key to start from, 0 to begin.
   * @param count    - Number of keys to return.
   * @return List of Keys that match the criteria.
   */
  @Override
  public List<KeyData> listKey(Container container, long startLocalID, int
      count) throws IOException {
    Preconditions.checkNotNull(container, "container cannot be null");
    Preconditions.checkState(startLocalID >= 0, "startLocal ID cannot be " +
        "negative");
    Preconditions.checkArgument(count > 0,
        "Count must be a positive number.");
    container.readLock();
    List<KeyData> result = null;
    KeyValueContainerData cData = (KeyValueContainerData) container
        .getContainerData();
    MetadataStore db = KeyUtils.getDB(cData, config);
    result = new ArrayList<>();
    byte[] startKeyInBytes = Longs.toByteArray(startLocalID);
    List<Map.Entry<byte[], byte[]>> range = db.getSequentialRangeKVs(
        startKeyInBytes, count, null);
    for (Map.Entry<byte[], byte[]> entry : range) {
      KeyData value = KeyUtils.getKeyData(entry.getValue());
      KeyData data = new KeyData(value.getBlockID());
      result.add(data);
    }
    return result;
  }

  /**
   * Shutdown KeyValueContainerManager.
   */
  public void shutdown() {
    KeyUtils.shutdownCache(ContainerCache.getInstance(config));
  }

}
