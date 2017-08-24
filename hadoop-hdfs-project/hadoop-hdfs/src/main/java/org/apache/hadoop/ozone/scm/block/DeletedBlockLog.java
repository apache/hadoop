/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.scm.block;


import org.apache.hadoop.ozone.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The DeletedBlockLog is a persisted log in SCM to keep tracking
 * container blocks which are under deletion. It maintains info
 * about under-deletion container blocks that notified by KSM,
 * and the state how it is processed.
 */
public interface DeletedBlockLog extends Closeable {

  /**
   *  A limit size list of transactions. Note count is the max number
   *  of TXs to return, we might not be able to always return this
   *  number. and the processCount of those transactions
   *  should be [0, MAX_RETRY).
   *
   * @param count - number of transactions.
   * @return a list of BlockDeletionTransaction.
   */
  List<DeletedBlocksTransaction> getTransactions(int count)
      throws IOException;

  /**
   * Increments count for given list of transactions by 1.
   * The log maintains a valid range of counts for each transaction
   * [0, MAX_RETRY]. If exceed this range, resets it to -1 to indicate
   * the transaction is no longer valid.
   *
   * @param txIDs - transaction ID.
   */
  void incrementCount(List<Long> txIDs)
      throws IOException;

  /**
   * Commits a transaction means to delete all footprints of a transaction
   * from the log. This method doesn't guarantee all transactions can be
   * successfully deleted, it tolerate failures and tries best efforts to.
   *
   * @param txIDs - transaction IDs.
   */
  void commitTransactions(List<Long> txIDs) throws IOException;

  /**
   * Creates a block deletion transaction and adds that into the log.
   *
   * @param containerName - container name.
   * @param blocks - blocks that belong to the same container.
   *
   * @throws IOException
   */
  void addTransaction(String containerName, List<String> blocks)
      throws IOException;
}
