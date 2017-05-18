/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.ozone.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.apache.hadoop.utils.LevelDBStore;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.ozone.OzoneConsts.KSM_DB_NAME;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys
    .OZONE_KSM_DB_CACHE_SIZE_MB;

/**
 * KSM metadata manager interface.
 */
public class MetadataManagerImpl implements  MetadataManager {

  private final LevelDBStore store;
  private final ReadWriteLock lock;


  public MetadataManagerImpl(OzoneConfiguration conf) throws IOException {
    File metaDir = OzoneUtils.getScmMetadirPath(conf);
    final int cacheSize = conf.getInt(OZONE_KSM_DB_CACHE_SIZE_MB,
        OZONE_KSM_DB_CACHE_SIZE_DEFAULT);
    Options options = new Options();
    options.cacheSize(cacheSize * OzoneConsts.MB);
    File ksmDBFile = new File(metaDir.getPath(), KSM_DB_NAME);
    this.store = new LevelDBStore(ksmDBFile, options);
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Start metadata manager.
   */
  @Override
  public void start() {

  }

  /**
   * Stop metadata manager.
   */
  @Override
  public void stop() throws IOException {
    if (store != null) {
      store.close();
    }
  }

  /**
   * Returns the read lock used on Metadata DB.
   * @return readLock
   */
  @Override
  public Lock readLock() {
    return lock.readLock();
  }

  /**
   * Returns the write lock used on Metadata DB.
   * @return writeLock
   */
  @Override
  public Lock writeLock() {
    return lock.writeLock();
  }

  /**
   * Returns the value associated with this key.
   * @param key - key
   * @return value
   */
  @Override
  public byte[] get(byte[] key) {
    return store.get(key);
  }

  /**
   * Puts a Key into Metadata DB.
   * @param key   - key
   * @param value - value
   */
  @Override
  public void put(byte[] key, byte[] value) {
    store.put(key, value);
  }

  /**
   * Performs a batch Put to Metadata DB.
   * Can be used to do multiple puts atomically.
   * @param list - list of Map.Entry
   */
  @Override
  public void batchPut(List<Map.Entry<byte[], byte[]>> list)
      throws IOException {
    WriteBatch batch = store.createWriteBatch();
    list.forEach(entry -> batch.put(entry.getKey(), entry.getValue()));
    try {
      store.commitWriteBatch(batch);
    } finally {
      store.closeWriteBatch(batch);
    }
  }

}
