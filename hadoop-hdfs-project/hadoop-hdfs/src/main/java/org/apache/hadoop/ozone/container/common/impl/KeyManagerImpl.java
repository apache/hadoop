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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.ozone.protocol.proto.ContainerProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.helpers.ContainerData;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.helpers.KeyUtils;
import org.apache.hadoop.ozone.container.common.helpers.Pipeline;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerManager;
import org.apache.hadoop.ozone.container.common.interfaces.KeyManager;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.LevelDBStore;

import java.io.IOException;
import java.util.List;

/**
 * Key Manager impl.
 */
public class KeyManagerImpl implements KeyManager {
  private static final float LOAD_FACTOR = 0.75f;
  private final ContainerManager containerManager;
  private final ContainerCache containerCache;

  /**
   * Constructs a key Manager.
   *
   * @param containerManager - Container Manager.
   */
  public KeyManagerImpl(ContainerManager containerManager, Configuration conf) {
    Preconditions.checkNotNull(containerManager);
    Preconditions.checkNotNull(conf);
    int cacheSize = conf.getInt(OzoneConfigKeys.DFS_OZONE_KEY_CACHE,
        OzoneConfigKeys.DFS_OZONE_KEY_CACHE_DEFAULT);
    this.containerManager = containerManager;
    containerCache = new ContainerCache(cacheSize, LOAD_FACTOR, true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putKey(Pipeline pipeline, KeyData data) throws IOException {
    containerManager.readLock();
    try {
      // We are not locking the key manager since LevelDb serializes all actions
      // against a single DB. We rely on DB level locking to avoid conflicts.
      Preconditions.checkNotNull(pipeline);
      Preconditions.checkNotNull(pipeline.getContainerName());
      ContainerData cData = containerManager.readContainer(
          pipeline.getContainerName());
      LevelDBStore db = KeyUtils.getDB(cData, containerCache);
      Preconditions.checkNotNull(db);
      db.put(data.getKeyName().getBytes(KeyUtils.ENCODING), data
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
      Preconditions.checkNotNull(data);
      Preconditions.checkNotNull(data.getContainerName());
      ContainerData cData = containerManager.readContainer(data
          .getContainerName());
      LevelDBStore db = KeyUtils.getDB(cData, containerCache);
      Preconditions.checkNotNull(db);
      byte[] kData = db.get(data.getKeyName().getBytes(KeyUtils.ENCODING));
      if(kData == null) {
        throw new IOException("Unable to find the key.");
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
  public void deleteKey(Pipeline pipeline, String keyName) throws IOException {
    containerManager.readLock();
    try {
      Preconditions.checkNotNull(pipeline);
      Preconditions.checkNotNull(pipeline.getContainerName());
      ContainerData cData = containerManager.readContainer(pipeline
          .getContainerName());
      LevelDBStore db = KeyUtils.getDB(cData, containerCache);
      Preconditions.checkNotNull(db);

      // Note : There is a race condition here, since get and delete
      // are not atomic. Leaving it here since the impact is refusing
      // to delete a key which might have just gotten inserted after
      // the get check.

      byte[] kData = db.get(keyName.getBytes(KeyUtils.ENCODING));
      if(kData == null) {
        throw new IOException("Unable to find the key.");
      }
      db.delete(keyName.getBytes(KeyUtils.ENCODING));
    } finally {
      containerManager.readUnlock();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<KeyData> listKey(Pipeline pipeline, String prefix, String
      prevKey, int count) {
    // TODO :
    return null;
  }
}
