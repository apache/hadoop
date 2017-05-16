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

package org.apache.hadoop.utils;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * LevelDB interface.
 */
public class LevelDBStore implements Closeable {
  private DB db;
  private final File dbFile;
  private final Options dbOptions;
  private final WriteOptions writeOptions;

  /**
   * Opens a DB file.
   *
   * @param dbPath          - DB File path
   * @param createIfMissing - Create if missing
   * @throws IOException
   */
  public LevelDBStore(File dbPath, boolean createIfMissing) throws
      IOException {
    dbOptions = new Options();
    dbOptions.createIfMissing(createIfMissing);
    db = JniDBFactory.factory.open(dbPath, dbOptions);
    if (db == null) {
      throw new IOException("Db is null");
    }
    this.dbFile = dbPath;
    this.writeOptions = new WriteOptions().sync(true);
  }

  /**
   * Opens a DB file.
   *
   * @param dbPath          - DB File path
   * @throws IOException
   */
  public LevelDBStore(File dbPath, Options options)
      throws IOException {
    dbOptions = options;
    db = JniDBFactory.factory.open(dbPath, options);
    if (db == null) {
      throw new IOException("Db is null");
    }
    this.dbFile = dbPath;
    this.writeOptions = new WriteOptions().sync(true);
  }


  /**
   * Puts a Key into file.
   *
   * @param key   - key
   * @param value - value
   */
  public void put(byte[] key, byte[] value) {
    db.put(key, value, writeOptions);
  }

  /**
   * Get Key.
   *
   * @param key key
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
  @Override
  public void close() throws IOException {
    db.close();
  }

  /**
   * Returns true if the DB is empty.
   *
   * @return boolean
   * @throws IOException
   */
  public boolean isEmpty() throws IOException {
    DBIterator iter = db.iterator();
    try {
      iter.seekToFirst();
      return !iter.hasNext();
    } finally {
      iter.close();
    }
  }

  /**
   * Returns Java File Object that points to the DB.
   * @return File
   */
  public File getDbFile() {
    return dbFile;
  }

  /**
   * Returns the actual levelDB object.
   * @return DB handle.
   */
  public DB getDB() {
    return db;
  }

  /**
   * Returns an iterator on all the key-value pairs in the DB.
   * @return an iterator on DB entries.
   */
  public DBIterator getIterator() {
    return db.iterator();
  }


  public void destroy() throws IOException {
    JniDBFactory.factory.destroy(dbFile, dbOptions);
  }

  /**
   * Returns a write batch for write multiple key-value pairs atomically.
   * @return write batch that can be commit atomically.
   */
  public WriteBatch createWriteBatch() {
    return db.createWriteBatch();
  }

  /**
   * Commit multiple writes of key-value pairs atomically.
   * @param wb
   */
  public void commitWriteBatch(WriteBatch wb) {
    db.write(wb, writeOptions);
  }

  /**
   * Close a write batch of multiple writes to key-value pairs.
   * @param wb - write batch.
   * @throws IOException
   */
  public void closeWriteBatch(WriteBatch wb) throws IOException {
    wb.close();
  }

  /**
   * Compacts the DB by removing deleted keys etc.
   * @throws IOException if there is an error.
   */
  public void compactDB() throws IOException {
    if(db != null) {
      // From LevelDB docs : begin == null and end == null means the whole DB.
      db.compactRange(null, null);
    }
  }
}
