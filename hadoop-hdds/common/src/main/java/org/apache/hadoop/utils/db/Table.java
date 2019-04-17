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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface for key-value store that stores ozone metadata. Ozone metadata is
 * stored as key value pairs, both key and value are arbitrary byte arrays. Each
 * Table Stores a certain kind of keys and values. This allows a DB to have
 * different kind of tables.
 */
@InterfaceStability.Evolving
public interface Table<KEY, VALUE> extends AutoCloseable {

  /**
   * Puts a key-value pair into the store.
   *
   * @param key metadata key
   * @param value metadata value
   */
  void put(KEY key, VALUE value) throws IOException;

  /**
   * Puts a key-value pair into the store as part of a bath operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   * @param value metadata value
   */
  void putWithBatch(BatchOperation batch, KEY key, VALUE value)
      throws IOException;

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
  VALUE get(KEY key) throws IOException;

  /**
   * Deletes a key from the metadata store.
   *
   * @param key metadata key
   * @throws IOException on Failure
   */
  void delete(KEY key) throws IOException;

  /**
   * Deletes a key from the metadata store as part of a batch operation.
   *
   * @param batch the batch operation
   * @param key metadata key
   * @throws IOException on Failure
   */
  void deleteWithBatch(BatchOperation batch, KEY key) throws IOException;

  /**
   * Returns the iterator for this metadata store.
   *
   * @return MetaStoreIterator
   */
  TableIterator<KEY, ? extends KeyValue<KEY, VALUE>> iterator();

  /**
   * Returns the Name of this Table.
   * @return - Table Name.
   * @throws IOException on failure.
   */
  String getName() throws IOException;

  /**
   * Class used to represent the key and value pair of a db entry.
   */
  interface KeyValue<KEY, VALUE> {

    KEY getKey() throws IOException;

    VALUE getValue() throws IOException;
  }
}
