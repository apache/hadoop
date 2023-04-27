/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.LeveldbConfigurationStore;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;

/**
 *  Generic wrapper for LevelDB which abstracts common logic required to interact with levelDB
 *  TODO
 *    1. Refactor LeveldbRMStateStore and LeveldbConfigurationStore to use this
 *    2. Evaluate moving to https://github.com/dain/leveldb which has an iterator API with prefix key support
 */
public class
LevelDbStore implements KVStore {

  public static final Log LOG =
      LogFactory.getLog(LeveldbRMStateStore.class);

  private DB db;
  private final String dbPath;
  private final Options options;

  public LevelDbStore(String dbPath, Options options) {
    this.dbPath = dbPath;
    this.options = options;

    // LevelDB database can only be accessed through the comparator that was used to create it
    // Conf Store uses a custom comparator and thus the same comparator needs to be used to access it again
    if (dbPath.contains(LeveldbConfigurationStore.DB_NAME)) {
      this.options.comparator(LeveldbConfigurationStore.getDBComparator());
    }
  }

  /**
   * Initialises the level db database
   * Constructs the database if it doesn't exist with the necessary permissions
   */
  @Override
  public void init() throws IOException {
    File dbfile = new File(dbPath);
    try {
      db = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      LOG.error("Unable to open database with error : " + e.getMessage());
      throw e;
    }
  }

  @Override
  public byte[] get(byte[] key) throws IOException {
    try {
      return db.get(key);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void set(byte[] key, byte[] value) throws IOException {
    try {
      db.put(key, value);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void del(byte[] key) throws IOException {
    try {
      db.delete(key);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (db != null) {
      db.close();
      db = null;
    }
  }
}
