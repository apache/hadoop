/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.transactional;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Cleans up committed transactions when they are no longer needed to verify
 * pending transactions.
 */
class CleanOldTransactionsChore extends Chore {

  private static final String SLEEP_CONF = "hbase.transaction.clean.sleep";
  private static final int DEFAULT_SLEEP = 60 * 1000;

  private final TransactionalRegionServer regionServer;

  /**
   * @param regionServer
   * @param stopRequest
   */
  public CleanOldTransactionsChore(
      final TransactionalRegionServer regionServer,
      final AtomicBoolean stopRequest) {
    super(regionServer.getConfiguration().getInt(SLEEP_CONF, DEFAULT_SLEEP),
        stopRequest);
    this.regionServer = regionServer;
  }

  @Override
  protected void chore() {
    for (HRegion region : regionServer.getOnlineRegions()) {
      ((TransactionalRegion) region).removeUnNeededCommitedTransactions();
    }
  }
}
