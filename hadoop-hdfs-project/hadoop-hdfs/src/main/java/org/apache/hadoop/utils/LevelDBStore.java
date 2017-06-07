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

import org.apache.hadoop.utils.LevelDBKeyFilters.LevelDBKeyFilter;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteOptions;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Snapshot;
import org.iq80.leveldb.ReadOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;

/**
 * LevelDB interface.
 */
public class LevelDBStore implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(LevelDBStore.class);

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

  /**
   * Returns a certain range of key value pairs as a list based on a startKey
   * or count.
   *
   * @param keyPrefix start key.
   * @param count number of entries to return.
   * @return a range of entries or an empty list if nothing found.
   * @throws IOException
   *
   * @see #getRangeKVs(byte[], int, LevelDBKeyFilter...)
   */
  public List<Entry<byte[], byte[]>> getRangeKVs(byte[] keyPrefix, int count)
      throws IOException {
    LevelDBKeyFilter emptyFilter = (preKey, currentKey, nextKey) -> true;
    return getRangeKVs(keyPrefix, count, emptyFilter);
  }

  /**
   * Returns a certain range of key value pairs as a list based on a
   * startKey or count. Further a {@link LevelDBKeyFilter} can be added to
   * filter keys if necessary. To prevent race conditions while listing
   * entries, this implementation takes a snapshot and lists the entries from
   * the snapshot. This may, on the other hand, cause the range result slight
   * different with actual data if data is updating concurrently.
   * <p>
   * If the startKey is specified and found in levelDB, this key and the keys
   * after this key will be included in the result. If the startKey is null
   * all entries will be included as long as other conditions are satisfied.
   * If the given startKey doesn't exist, an IOException will be thrown.
   * <p>
   * The count argument is to limit number of total entries to return,
   * the value for count must be an integer greater than 0.
   * <p>
   * This method allows to specify one or more {@link LevelDBKeyFilter}
   * to filter keys by certain condition. Once given, only the entries
   * whose key passes all the filters will be included in the result.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param filters customized one or more {@link LevelDBKeyFilter}.
   * @return a list of entries found in the database.
   * @throws IOException if an invalid startKey is given or other I/O errors.
   * @throws IllegalArgumentException if count is less than 0.
   */
  public List<Entry<byte[], byte[]>> getRangeKVs(byte[] startKey,
      int count, LevelDBKeyFilter... filters) throws IOException {
    List<Entry<byte[], byte[]>> result = new ArrayList<>();
    long start = System.currentTimeMillis();
    if (count < 0) {
      throw new IllegalArgumentException(
          "Invalid count given " + count + ", count must be greater than 0");
    }
    Snapshot snapShot = null;
    DBIterator dbIter = null;
    try {
      snapShot = db.getSnapshot();
      ReadOptions readOptions = new ReadOptions().snapshot(snapShot);
      dbIter = db.iterator(readOptions);
      dbIter.seekToFirst();
      if (startKey == null) {
        dbIter.seekToFirst();
      } else {
        if (db.get(startKey) == null) {
          throw new IOException("Invalid start key, not found in current db.");
        }
        dbIter.seek(startKey);
      }
      while (dbIter.hasNext() && result.size() < count) {
        byte[] preKey = dbIter.hasPrev() ? dbIter.peekPrev().getKey() : null;
        byte[] nextKey = dbIter.hasNext() ? dbIter.peekNext().getKey() : null;
        Entry<byte[], byte[]> current = dbIter.next();
        if (filters == null || Arrays.asList(filters).stream()
            .allMatch(entry -> entry.filterKey(preKey,
                current.getKey(), nextKey))) {
          result.add(current);
        }
      }
    } finally {
      if (snapShot != null) {
        snapShot.close();
      }
      if (dbIter != null) {
        dbIter.close();
      }
      long end = System.currentTimeMillis();
      long timeConsumed = end - start;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Time consumed for getRangeKVs() is {},"
                + " result length is {}.",
            timeConsumed, result.size());
      }
    }
    return result;
  }
}
