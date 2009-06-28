/**
 * Copyright 2008 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.client.tableindexed;

import java.util.Map;

import org.apache.hadoop.io.Writable;

/**
 * Interface for generating an index-row-key from a row in the base table.
 */
public interface IndexKeyGenerator extends Writable {

  /** Create an index key from a base row.
   * 
   * @param rowKey the row key of the base row
   * @param columns the columns in the base row
   * @return the row key in the indexed row.
   */
  byte[] createIndexKey(byte[] rowKey, Map<byte[], byte[]> columns);
}
