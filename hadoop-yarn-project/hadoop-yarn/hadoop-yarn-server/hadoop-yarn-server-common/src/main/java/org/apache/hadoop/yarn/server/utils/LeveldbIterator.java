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

package org.apache.hadoop.yarn.server.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

/**
 * A wrapper for a DBIterator to translate the raw RuntimeExceptions that
 * can be thrown into DBExceptions.
 */
@Public
@Evolving
public class LeveldbIterator implements Iterator<Map.Entry<byte[], byte[]>>,
                                        Closeable {
  private DBIterator iter;

  /**
   * Create an iterator for the specified database
   */
  public LeveldbIterator(DB db) {
    iter = db.iterator();
  }

  /**
   * Create an iterator for the specified database
   */
  public LeveldbIterator(DB db, ReadOptions options) {
    iter = db.iterator(options);
  }

  /**
   * Create an iterator using the specified underlying DBIterator
   */
  public LeveldbIterator(DBIterator iter) {
    this.iter = iter;
  }

  /**
   * Repositions the iterator so the key of the next BlockElement
   * returned greater than or equal to the specified targetKey.
   */
  public void seek(byte[] key) throws DBException {
    try {
      iter.seek(key);
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Repositions the iterator so is is at the beginning of the Database.
   */
  public void seekToFirst() throws DBException {
    try {
      iter.seekToFirst();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Repositions the iterator so it is at the end of of the Database.
   */
  public void seekToLast() throws DBException {
    try {
      iter.seekToLast();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Returns <tt>true</tt> if the iteration has more elements.
   */
  public boolean hasNext() throws DBException {
    try {
      return iter.hasNext();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Returns the next element in the iteration.
   */
  @Override
  public Map.Entry<byte[], byte[]> next() throws DBException {
    try {
      return iter.next();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Returns the next element in the iteration, without advancing the
   * iteration.
   */
  public Map.Entry<byte[], byte[]> peekNext() throws DBException {
    try {
      return iter.peekNext();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * @return true if there is a previous entry in the iteration.
   */
  public boolean hasPrev() throws DBException {
    try {
      return iter.hasPrev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * @return the previous element in the iteration and rewinds the iteration.
   */
  public Map.Entry<byte[], byte[]> prev() throws DBException {
    try {
      return iter.prev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * @return the previous element in the iteration, without rewinding the
   * iteration.
   */
  public Map.Entry<byte[], byte[]> peekPrev() throws DBException {
    try {
      return iter.peekPrev();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Removes from the database the last element returned by the iterator.
   */
  @Override
  public void remove() throws DBException {
    try {
      iter.remove();
    } catch (DBException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DBException(e.getMessage(), e);
    }
  }

  /**
   * Closes the iterator.
   */
  @Override
  public void close() throws IOException {
    try {
      iter.close();
    } catch (RuntimeException e) {
      throw new IOException(e.getMessage(), e);
    }
  }
}
