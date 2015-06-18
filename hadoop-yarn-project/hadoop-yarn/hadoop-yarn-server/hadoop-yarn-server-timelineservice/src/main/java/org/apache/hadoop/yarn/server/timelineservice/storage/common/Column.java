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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

/**
 * A Column represents the way to store a fully qualified column in a specific
 * table.
 */
public interface Column<T> {

  /**
   * Sends a Mutation to the table. The mutations will be buffered and sent over
   * the wire as part of a batch.
   *
   * @param rowKey identifying the row to write. Nothing gets written when null.
   * @param tableMutator used to modify the underlying HBase table. Caller is
   *          responsible to pass a mutator for the table that actually has this
   *          column.
   * @param timestamp version timestamp. When null the server timestamp will be
   *          used.
   * @param inputValue the value to write to the rowKey and column qualifier.
   *          Nothing gets written when null.
   * @throws IOException
   */
  public void store(byte[] rowKey, TypedBufferedMutator<T> tableMutator,
      Long timestamp, Object inputValue) throws IOException;

  /**
   * Get the latest version of this specified column. Note: this call clones the
   * value content of the hosting {@link Cell}.
   *
   * @param result Cannot be null
   * @return result object (can be cast to whatever object was written to), or
   *         null when result doesn't contain this column.
   * @throws IOException
   */
  public Object readResult(Result result) throws IOException;

}