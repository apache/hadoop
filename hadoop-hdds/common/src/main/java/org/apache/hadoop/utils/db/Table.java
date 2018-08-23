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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.utils.db;

import org.apache.hadoop.classification.InterfaceStability;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.WriteBatch;

import java.io.IOException;

/**
 * Interface for key-value store that stores ozone metadata. Ozone metadata is
 * stored as key value pairs, both key and value are arbitrary byte arrays. Each
 * Table Stores a certain kind of keys and values. This allows a DB to have
 * different kind of tables.
 */
@InterfaceStability.Evolving
public interface Table extends AutoCloseable {

  /**
   * Puts a key-value pair into the store.
   *
   * @param key metadata key
   * @param value metadata value
   */
  void put(byte[] key, byte[] value) throws IOException;

  /**
   * @return true if the metadata store is empty.
   * @throws IOException on Failure
   */
  boolean isEmpty() throws IOException;

  /**
   * Returns the value mapped to the given key in byte array or returns null
   * if the key is not found.
   *
   * @param key metadata key
   * @return value in byte array or null if the key is not found.
   * @throws IOException on Failure
   */
  byte[] get(byte[] key) throws IOException;

  /**
   * Deletes a key from the metadata store.
   *
   * @param key metadata key
   * @throws IOException on Failure
   */
  void delete(byte[] key) throws IOException;

  /**
   * Return the Column Family handle. TODO: This leaks an RockDB abstraction
   * into Ozone code, cleanup later.
   *
   * @return ColumnFamilyHandle
   */
  ColumnFamilyHandle getHandle();

  /**
   * A batch of PUT, DELETE operations handled as a single atomic write.
   *
   * @throws IOException write fails
   */
  void writeBatch(WriteBatch operation) throws IOException;

  /**
   * Returns the iterator for this metadata store.
   *
   * @return MetaStoreIterator
   */
  TableIterator<KeyValue> iterator();

  /**
   * Returns the Name of this Table.
   * @return - Table Name.
   * @throws IOException on failure.
   */
  String getName() throws IOException;

  /**
   * Class used to represent the key and value pair of a db entry.
   */
  class KeyValue {

    private final byte[] key;
    private final byte[] value;

    /**
     * KeyValue Constructor, used to represent a key and value of a db entry.
     *
     * @param key - Key Bytes
     * @param value - Value bytes
     */
    private KeyValue(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }

    /**
     * Create a KeyValue pair.
     *
     * @param key - Key Bytes
     * @param value - Value bytes
     * @return KeyValue object.
     */
    public static KeyValue create(byte[] key, byte[] value) {
      return new KeyValue(key, value);
    }

    /**
     * Return key.
     *
     * @return byte[]
     */
    public byte[] getKey() {
      byte[] result = new byte[key.length];
      System.arraycopy(key, 0, result, 0, key.length);
      return result;
    }

    /**
     * Return value.
     *
     * @return byte[]
     */
    public byte[] getValue() {
      byte[] result = new byte[value.length];
      System.arraycopy(value, 0, result, 0, value.length);
      return result;
    }
  }
}
