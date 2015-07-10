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

package org.apache.hadoop.ozone.web.localstorage;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

/**
 * OzoneLevelDBStore is used by the local
 * OzoneStore which is used in testing.
 */
class OzoneLevelDBStore {
  private DB db;

  /**
   * Opens a DB file.
   *
   * @param dbPath - DB File path
   * @param createIfMissing - Create if missing
   *
   * @throws IOException
   */
  OzoneLevelDBStore(File dbPath, boolean createIfMissing) throws IOException {
    Options options = new Options();
    options.createIfMissing(createIfMissing);
    db = JniDBFactory.factory.open(dbPath, options);
    if (db == null) {
      throw new IOException("Db is null");
    }
  }

  /**
   * Puts a Key into file.
   *
   * @param key - key
   * @param value - value
   */
  public void put(byte[] key, byte[] value) {
    db.put(key, value);
  }

  /**
   * Get Key.
   *
   * @param key key
   *
   * @return value
   */
  public byte[] get(byte[] key) {
    return db.get(key);
  }

  /**
   * Delete Key.
   *
   * @param key - Key
   */
  public void delete(byte[] key) {
    db.delete(key);
  }

  /**
   * Closes the DB.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    db.close();
  }
}
