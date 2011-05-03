/*
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.monitoring.MonitoredTask;

/**
 * A package protected interface for a store flushing.
 * A store flusher carries the state required to prepare/flush/commit the
 * store's cache.
 */
interface StoreFlusher {

  /**
   * Prepare for a store flush (create snapshot)
   *
   * Requires pausing writes.
   *
   * A very short operation.
   */
  void prepare();

  /**
   * Flush the cache (create the new store file)
   *
   * A length operation which doesn't require locking out any function
   * of the store.
   *
   * @throws IOException in case the flush fails
   */
  void flushCache(MonitoredTask status) throws IOException;

  /**
   * Commit the flush - add the store file to the store and clear the
   * memstore snapshot.
   *
   * Requires pausing scans.
   *
   * A very short operation
   *
   * @return
   * @throws IOException
   */
  boolean commit() throws IOException;
}
