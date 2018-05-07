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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.hdds.protocol.proto.ContainerProtos;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.ContainerProtos.Result
    .NO_SUCH_KEY;

/**
 * Key Manager impl.
 */
public class KeyManagerImpl implements KeyManager {
  static final Logger LOG =
      LoggerFactory.getLogger(KeyManagerImpl.class);

  private static final float LOAD_FACTOR = 0.75f;
  private final ContainerManager containerManager;
  private final Configuration conf;

  /**
   * Constructs a key Manager.
   *
   * @param containerManager - Container Manager.
   */
  public KeyManagerImpl(ContainerManager containerManager, Configuration conf) {
    Preconditions.checkNotNull(containerManager, "Container manager cannot be" +
        " null");
    Preconditions.checkNotNull(conf, "Config cannot be null");
    this.containerManager = containerManager;
    this.conf = conf;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putKey(KeyData data) throws IOException {
    Preconditions.checkNotNull(data, "KeyData cannot be null for put operation.");
    Preconditions.checkState(data.getContainerID() >= 0, "Container ID cannot be negative");
    containerManager.readLock();
    try {
      // We are not locking the key manager since LevelDb serializes all actions
      // against a single DB. We rely on DB level locking to avoid conflicts.
      ContainerData cData = containerManager.readContainer(
          data.getContainerID());
      MetadataStore db = KeyUtils.getDB(cData, conf);

      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, "DB cannot be null here");
      db.put(Longs.toByteArray(data.getLocalID()), data
          .getProtoBufMessage().toByteArray());
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KeyData getKey(KeyData data) throws IOException {
    containerManager.readLock();
    try {
      Preconditions.checkNotNull(data, "Key data cannot be null");
      Preconditions.checkNotNull(data.getContainerID(),
          "Container name cannot be null");
      ContainerData cData = containerManager.readContainer(data
          .getContainerID());
      MetadataStore db = KeyUtils.getDB(cData, conf);

      // This is a post condition that acts as a hint to the user.
      // Should never fail.
      Preconditions.checkNotNull(db, "DB cannot be null here");

      byte[] kData = db.get(Longs.toByteArray(data.getLocalID()));
      if (kData == null) {
        throw new StorageContainerException("Unable to find the key.",
            NO_SUCH_KEY);
      }
      ContainerProtos.KeyData keyData =
          ContainerProtos.KeyData.parseFrom(kData);
      return KeyData.getFromProtoBuf(keyData);
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deleteKey(BlockID blockID)
      throws IOException {
    Preconditions.checkNotNull(blockID, "block ID cannot be null.");
    Preconditions.checkState(blockID.getContainerID() >= 0,
        "Container ID cannot be negative.");
    Preconditions.checkState(blockID.getLocalID() >= 0,
        "Local ID cannot be negative.");

    containerManager.readLock();
    try {

      ContainerData cData = containerManager
          .readContainer(blockID.getContainerID());
      MetadataStore db = KeyUtils.getDB(cData, conf);

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
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<KeyData> listKey(
      long containerID, long startLocalID, int count)
      throws IOException {
    Preconditions.checkState(containerID >= 0, "Container ID cannot be negative");
    Preconditions.checkState(startLocalID >= 0, "startLocal ID cannot be negative");
    Preconditions.checkArgument(count > 0,
        "Count must be a positive number.");
    ContainerData cData = containerManager.readContainer(containerID);
    MetadataStore db = KeyUtils.getDB(cData, conf);

    List<KeyData> result = new ArrayList<>();
    byte[] startKeyInBytes = Longs.toByteArray(startLocalID);
    List<Map.Entry<byte[], byte[]>> range =
        db.getSequentialRangeKVs(startKeyInBytes, count, null);
    for (Map.Entry<byte[], byte[]> entry : range) {
      KeyData value = KeyUtils.getKeyData(entry.getValue());
      KeyData data = new KeyData(value.getBlockID());
      result.add(data);
    }
    return result;
  }

  /**
   * Shutdown keyManager.
   */
  @Override
  public void shutdown() {
    Preconditions.checkState(this.containerManager.hasWriteLock(), "asserts " +
        "that we are holding the container manager lock when shutting down.");
    KeyUtils.shutdownCache(ContainerCache.getInstance(conf));
  }
}
