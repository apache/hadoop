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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.utils.MetadataKeyFilters.MetadataKeyFilter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface for key-value store that stores ozone metadata.
 * Ozone metadata is stored as key value pairs, both key and value
 * are arbitrary byte arrays.
 */
@InterfaceStability.Evolving
public interface MetadataStore extends Closeable{

  /**
   * Puts a key-value pair into the store.
   *
   * @param key metadata key
   * @param value metadata value
   */
  void put(byte[] key, byte[] value) throws IOException;

  /**
   * @return true if the metadata store is empty.
   *
   * @throws IOException
   */
  boolean isEmpty() throws IOException;

  /**
   * Returns the value mapped to the given key in byte array.
   *
   * @param key metadata key
   * @return value in byte array
   * @throws IOException
   */
  byte[] get(byte[] key) throws IOException;

  /**
   * Deletes a key from the metadata store.
   *
   * @param key metadata key
   * @throws IOException
   */
  void delete(byte[] key) throws IOException;

  /**
   * Returns a certain range of key value pairs as a list based on a
   * startKey or count. Further a {@link MetadataKeyFilter} can be added to
   * filter keys if necessary. To prevent race conditions while listing
   * entries, this implementation takes a snapshot and lists the entries from
   * the snapshot. This may, on the other hand, cause the range result slight
   * different with actual data if data is updating concurrently.
   * <p>
   * If the startKey is specified and found in levelDB, this key and the keys
   * after this key will be included in the result. If the startKey is null
   * all entries will be included as long as other conditions are satisfied.
   * If the given startKey doesn't exist and empty list will be returned.
   * <p>
   * The count argument is to limit number of total entries to return,
   * the value for count must be an integer greater than 0.
   * <p>
   * This method allows to specify one or more {@link MetadataKeyFilter}
   * to filter keys by certain condition. Once given, only the entries
   * whose key passes all the filters will be included in the result.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param filters customized one or more {@link MetadataKeyFilter}.
   * @return a list of entries found in the database or an empty list if the
   * startKey is invalid.
   * @throws IOException if there are I/O errors.
   * @throws IllegalArgumentException if count is less than 0.
   */
  List<Map.Entry<byte[], byte[]>> getRangeKVs(byte[] startKey,
      int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException;

  /**
   * This method is very similar to {@link #getRangeKVs}, the only
   * different is this method is supposed to return a sequential range
   * of elements based on the filters. While iterating the elements,
   * if it met any entry that cannot pass the filter, the iterator will stop
   * from this point without looking for next match. If no filter is given,
   * this method behaves just like {@link #getRangeKVs}.
   *
   * @param startKey a start key.
   * @param count max number of entries to return.
   * @param filters customized one or more {@link MetadataKeyFilter}.
   * @return a list of entries found in the database.
   * @throws IOException
   * @throws IllegalArgumentException
   */
  List<Map.Entry<byte[], byte[]>> getSequentialRangeKVs(byte[] startKey,
      int count, MetadataKeyFilter... filters)
      throws IOException, IllegalArgumentException;

  /**
   * A batch of PUT, DELETE operations handled as a single atomic write.
   *
   * @throws IOException write fails
   */
  void writeBatch(BatchOperation operation) throws IOException;

  /**
   * Compact the entire database.
   * @throws IOException
   */
  void compactDB() throws IOException;

  /**
   * Destroy the content of the specified database,
   * a destroyed database will not be able to load again.
   * Be very careful with this method.
   *
   * @throws IOException if I/O error happens
   */
  void destroy() throws IOException;

  /**
   * Seek the database to a certain key, returns the key-value
   * pairs around this key based on the given offset. Note, this method
   * can only support offset -1 (left), 0 (current) and 1 (right),
   * any other offset given will cause a {@link IllegalArgumentException}.
   *
   * @param offset offset to the key
   * @param from from which key
   * @return a key-value pair
   * @throws IOException
   */
  ImmutablePair<byte[], byte[]> peekAround(int offset, byte[] from)
      throws IOException, IllegalArgumentException;

  /**
   * Iterates entries in the database from a certain key.
   * Applies the given {@link EntryConsumer} to the key and value of
   * each entry, the function produces a boolean result which is used
   * as the criteria to exit from iteration.
   *
   * @param from the start key
   * @param consumer
   *   a {@link EntryConsumer} applied to each key and value. If the consumer
   *   returns true, continues the iteration to next entry; otherwise exits
   *   the iteration.
   * @throws IOException
   */
  void iterate(byte[] from, EntryConsumer consumer)
      throws IOException;

  /**
   * Returns the iterator for this metadata store.
   * @return MetaStoreIterator
   */
  MetaStoreIterator<KeyValue> iterator();

  /**
   * Class used to represent the key and value pair of a db entry.
   */
  class KeyValue {

    private final byte[] key;
    private final byte[] value;

    /**
     * KeyValue Constructor, used to represent a key and value of a db entry.
     * @param key
     * @param value
     */
    private KeyValue(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Return key.
     * @return byte[]
     */
    public byte[] getKey() {
      byte[] result = new byte[key.length];
      System.arraycopy(key, 0, result, 0, key.length);
      return result;
    }

    /**
     * Return value.
     * @return byte[]
     */
    public byte[] getValue() {
      byte[] result = new byte[value.length];
      System.arraycopy(value, 0, result, 0, value.length);
      return result;
    }

    /**
     * Create a KeyValue pair.
     * @param key
     * @param value
     * @return KeyValue object.
     */
    public static KeyValue create(byte[] key, byte[] value) {
      return new KeyValue(key, value);
    }
  }
}
