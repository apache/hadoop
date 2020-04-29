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
package org.apache.hadoop.hdfs.server.federation.store.records;

import java.util.List;

/**
 * Encapsulates a state store query result that includes a set of records and a
 * time stamp for the result.
 */
public class QueryResult<T extends BaseRecord> {

  /** Data result. */
  private final List<T> records;
  /** Time stamp of the data results. */
  private final long timestamp;

  public QueryResult(final List<T> recs, final long time) {
    this.records = recs;
    this.timestamp = time;
  }

  /**
   * Get the result of the query.
   *
   * @return List of records.
   */
  public List<T> getRecords() {
    return this.records;
  }

  /**
   * The timetamp in driver time of this query.
   *
   * @return Timestamp in driver time.
   */
  public long getTimestamp() {
    return this.timestamp;
  }
}