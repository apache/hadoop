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
package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * Used to represent a partially qualified column, where the actual column name
 * will be composed of a prefix and the remainder of the column qualifier. The
 * prefix can be null, in which case the column qualifier will be completely
 * determined when the values are stored.
 */
public interface ColumnPrefix<T> {

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey identifying the row to write. Nothing gets written when null.
   * @param tableMutator used to modify the underlying HBase table. Caller is
   *          responsible to pass a mutator for the table that actually has this
   *          column.
   * @param qualifier column qualifier. Nothing gets written when null.
   * @param timestamp version timestamp. When null the server timestamp will be
   *          used.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException
   */
  public void store(byte[] rowKey, TypedBufferedMutator<T> tableMutator,
      String qualifier, Long timestamp, Object inputValue) throws IOException;

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link Cell}.
   *
   * @param result Cannot be null
   * @param qualifier column qualifier. Nothing gets read when null.
   * @return result object (can be cast to whatever object was written to) or
   *         null when specified column qualifier for this prefix doesn't exist
   *         in the result.
   * @throws IOException
   */
  public Object readResult(Result result, String qualifier) throws IOException;

  /**
   * @param result from which to read columns
   * @return the latest values of columns in the column family with this prefix
   *         (or all of them if the prefix value is null).
   * @throws IOException
   */
  public Map<String, Object> readResults(Result result) throws IOException;

  /**
   * @param result from which to reads data with timestamps
   * @param <V> the type of the values. The values will be cast into that type.
   * @return the cell values at each respective time in for form
   *         {idA={timestamp1->value1}, idA={timestamp2->value2},
   *         idB={timestamp3->value3}, idC={timestamp1->value4}}
   * @throws IOException
   */
  public <V> NavigableMap<String, NavigableMap<Long, V>>
      readResultsWithTimestamps(Result result) throws IOException;
}