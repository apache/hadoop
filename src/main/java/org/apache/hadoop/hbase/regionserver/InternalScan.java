/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Special internal-only scanner, currently used for increment operations to
 * allow additional server-side arguments for Scan operations.
 * <p>
 * Rather than adding new options/parameters to the public Scan API, this new
 * class has been created.
 * <p>
 * Supports adding an option to only read from the MemStore with
 * {@link #checkOnlyMemStore()} or to only read from StoreFiles with
 * {@link #checkOnlyStoreFiles()}.
 */
class InternalScan extends Scan {
  private boolean memOnly = false;
  private boolean filesOnly = false;

  /**
   * @param get get to model scan after
   */
  public InternalScan(Get get) {
    super(get);
  }

  /**
   * StoreFiles will not be scanned. Only MemStore will be scanned.
   */
  public void checkOnlyMemStore() {
    memOnly = true;
    filesOnly = false;
  }

  /**
   * MemStore will not be scanned. Only StoreFiles will be scanned.
   */
  public void checkOnlyStoreFiles() {
    memOnly = false;
    filesOnly = true;
  }

  /**
   * Returns true if only the MemStore should be checked.  False if not.
   * @return true to only check MemStore
   */
  public boolean isCheckOnlyMemStore() {
    return (memOnly);
  }

  /**
   * Returns true if only StoreFiles should be checked.  False if not.
   * @return true if only check StoreFiles
   */
  public boolean isCheckOnlyStoreFiles() {
    return (filesOnly);
  }
}