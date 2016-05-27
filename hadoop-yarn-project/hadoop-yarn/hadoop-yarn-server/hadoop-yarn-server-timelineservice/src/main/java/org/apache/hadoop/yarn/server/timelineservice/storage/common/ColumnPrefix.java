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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.Attribute;

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
   * @param attributes attributes for the mutation that are used by the
   *          coprocessor to set/read the cell tags.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException if there is any exception encountered while doing
   *     store operation(sending mutation to the table).
   */
  void store(byte[] rowKey, TypedBufferedMutator<T> tableMutator,
      byte[] qualifier, Long timestamp, Object inputValue,
      Attribute... attributes) throws IOException;

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
   * @param attributes attributes for the mutation that are used by the
   *          coprocessor to set/read the cell tags.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException if there is any exception encountered while doing
   *     store operation(sending mutation to the table).
   */
  void store(byte[] rowKey, TypedBufferedMutator<T> tableMutator,
      String qualifier, Long timestamp, Object inputValue,
      Attribute... attributes) throws IOException;

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link org.apache.hadoop.hbase.Cell Cell}.
   *
   * @param result Cannot be null
   * @param qualifier column qualifier. Nothing gets read when null.
   * @return result object (can be cast to whatever object was written to) or
   *         null when specified column qualifier for this prefix doesn't exist
   *         in the result.
   * @throws IOException if there is any exception encountered while reading
   *     result.
   */
  Object readResult(Result result, String qualifier) throws IOException;

  /**
   *
   * @param <K> identifies the type of key converter.
   * @param result from which to read columns.
   * @param keyConverter used to convert column bytes to the appropriate key
   *          type
   * @return the latest values of columns in the column family with this prefix
   *         (or all of them if the prefix value is null).
   * @throws IOException if there is any exception encountered while reading
   *           results.
   */
  <K> Map<K, Object> readResults(Result result, KeyConverter<K> keyConverter)
      throws IOException;

  /**
   * @param result from which to reads data with timestamps.
   * @param <K> identifies the type of key converter.
   * @param <V> the type of the values. The values will be cast into that type.
   * @param keyConverter used to convert column bytes to the appropriate key
   *     type.
   * @return the cell values at each respective time in for form
   *         {@literal {idA={timestamp1->value1}, idA={timestamp2->value2},
   *         idB={timestamp3->value3}, idC={timestamp1->value4}}}
   * @throws IOException if there is any exception encountered while reading
   *     result.
   */
  <K, V> NavigableMap<K, NavigableMap<Long, V>> readResultsWithTimestamps(
      Result result, KeyConverter<K> keyConverter) throws IOException;

  /**
   * @param qualifierPrefix Column qualifier or prefix of qualifier.
   * @return a byte array encoding column prefix and qualifier/prefix passed.
   */
  byte[] getColumnPrefixBytes(String qualifierPrefix);

  /**
   * @param qualifierPrefix Column qualifier or prefix of qualifier.
   * @return a byte array encoding column prefix and qualifier/prefix passed.
   */
  byte[] getColumnPrefixBytes(byte[] qualifierPrefix);

  /**
   * Returns column family name(as bytes) associated with this column prefix.
   * @return a byte array encoding column family for this prefix.
   */
  byte[] getColumnFamilyBytes();

  /**
   * Returns value converter implementation associated with this column prefix.
   * @return a {@link ValueConverter} implementation.
   */
  ValueConverter getValueConverter();
}