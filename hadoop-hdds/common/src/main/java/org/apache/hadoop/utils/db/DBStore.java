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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceStability;

/**
 * The DBStore interface provides the ability to create Tables, which store
 * a specific type of Key-Value pair. Some DB interfaces like LevelDB will not
 * be able to do this. In those case a Table creation will map to a default
 * store.
 *
 */
@InterfaceStability.Evolving
public interface DBStore extends AutoCloseable {

  /**
   * Gets an existing TableStore.
   *
   * @param name - Name of the TableStore to get
   * @return - TableStore.
   * @throws IOException on Failure
   */
  Table<byte[], byte[]> getTable(String name) throws IOException;

  /**
   * Gets an existing TableStore with implicit key/value conversion.
   *
   * @param name - Name of the TableStore to get
   * @param keyType
   * @param valueType
   * @return - TableStore.
   * @throws IOException on Failure
   */
  <KEY, VALUE> Table<KEY, VALUE> getTable(String name,
      Class<KEY> keyType, Class<VALUE> valueType) throws IOException;

  /**
   * Lists the Known list of Tables in a DB.
   *
   * @return List of Tables, in case of Rocks DB and LevelDB we will return at
   * least one entry called DEFAULT.
   * @throws IOException on Failure
   */
  ArrayList<Table> listTables() throws IOException;

  /**
   * Flush the DB buffer onto persistent storage.
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Compact the entire database.
   *
   * @throws IOException on Failure
   */
  void compactDB() throws IOException;

  /**
   * Moves a key from the Source Table to the destination Table.
   *
   * @param key - Key to move.
   * @param source - Source Table.
   * @param dest - Destination Table.
   * @throws IOException on Failure
   */
  <KEY, VALUE> void move(KEY key, Table<KEY, VALUE> source,
                         Table<KEY, VALUE> dest) throws IOException;

  /**
   * Moves a key from the Source Table to the destination Table and updates the
   * destination to the new value.
   *
   * @param key - Key to move.
   * @param value - new value to write to the destination table.
   * @param source - Source Table.
   * @param dest - Destination Table.
   * @throws IOException on Failure
   */
  <KEY, VALUE> void move(KEY key, VALUE value, Table<KEY, VALUE> source,
                         Table<KEY, VALUE> dest)
      throws IOException;

  /**
   * Moves a key from the Source Table to the destination Table and updates the
   * destination with the new key name and value.
   * This is similar to deleting an entry in one table and adding an entry in
   * another table, here it is done atomically.
   *
   * @param sourceKey - Key to move.
   * @param destKey - Destination key name.
   * @param value - new value to write to the destination table.
   * @param source - Source Table.
   * @param dest - Destination Table.
   * @throws IOException on Failure
   */
  <KEY, VALUE> void move(KEY sourceKey, KEY destKey, VALUE value,
                         Table<KEY, VALUE> source, Table<KEY, VALUE> dest)
      throws IOException;

  /**
   * Returns an estimated count of keys in this DB.
   *
   * @return long, estimate of keys in the DB.
   */
  long getEstimatedKeyCount() throws IOException;

  /**
   * Initialize an atomic batch operation which can hold multiple PUT/DELETE
   * operations and committed later in one step.
   *
   * @return BatchOperation holder which can be used to add or commit batch
   * operations.
   */
  BatchOperation initBatchOperation();

  /**
   * Commit the batch operations.
   *
   * @param operation which contains all the required batch operation.
   * @throws IOException on Failure.
   */
  void commitBatchOperation(BatchOperation operation) throws IOException;

  /**
   * Get current snapshot of OM DB store as an artifact stored on
   * the local filesystem.
   * @return An object that encapsulates the checkpoint information along with
   * location.
   */
  DBCheckpoint getCheckpoint(boolean flush) throws IOException;

  /**
   * Get DB Store location.
   * @return DB file location.
   */
  File getDbLocation();
}
