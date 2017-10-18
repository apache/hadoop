/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.scm.block;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.scm.container.Mapping;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.apache.hadoop.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTaskResult.EmptyTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A background service running in SCM to delete blocks. This service scans
 * block deletion log in certain interval and caches block deletion commands
 * in {@link org.apache.hadoop.ozone.scm.node.CommandQueue}, asynchronously
 * SCM HB thread polls cached commands and sends them to datanode for physical
 * processing.
 */
public class SCMBlockDeletingService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockDeletingService.class);

  // ThreadPoolSize=2, 1 for scheduler and the other for the scanner.
  private final static int BLOCK_DELETING_SERVICE_CORE_POOL_SIZE = 2;
  private final DeletedBlockLog deletedBlockLog;
  private final Mapping mappingService;
  private final NodeManager nodeManager;

  // Default container size is 5G and block size is 256MB, a full container
  // at most contains 20 blocks. At most each TX contains 20 blocks.
  // When SCM sends block deletion TXs to datanode, each command we allow
  // at most 50 containers so that will limit number of to be deleted blocks
  // less than 1000.
  // TODO - a better throttle algorithm
  // Note, this is not an accurate limit of blocks. When we scan
  // the log, worst case we may get 50 TX for 50 different datanodes,
  // that will cause the deletion message sent by SCM extremely small.
  // As a result, the deletion will be slow. An improvement is to scan
  // log multiple times until we get enough TXs for each datanode, or
  // the entire log is scanned.
  private static final int BLOCK_DELETE_TX_PER_REQUEST_LIMIT = 50;

  public SCMBlockDeletingService(DeletedBlockLog deletedBlockLog,
      Mapping mapper, NodeManager nodeManager,
      int interval, long serviceTimeout) {
    super("SCMBlockDeletingService", interval, TimeUnit.MILLISECONDS,
        BLOCK_DELETING_SERVICE_CORE_POOL_SIZE, serviceTimeout);
    this.deletedBlockLog = deletedBlockLog;
    this.mappingService = mapper;
    this.nodeManager = nodeManager;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DeletedBlockTransactionScanner());
    return queue;
  }

  private class DeletedBlockTransactionScanner
      implements BackgroundTask<EmptyTaskResult> {

    @Override
    public int getPriority() {
      return 1;
    }

    @Override
    public EmptyTaskResult call() throws Exception {
      int dnTxCount = 0;
      long startTime = Time.monotonicNow();
      // Scan SCM DB in HB interval and collect a throttled list of
      // to delete blocks.
      LOG.debug("Running DeletedBlockTransactionScanner");
      DatanodeDeletedBlockTransactions transactions =
          getToDeleteContainerBlocks();
      if (transactions != null && !transactions.isEmpty()) {
        for (DatanodeID datanodeID : transactions.getDatanodes()) {
          List<DeletedBlocksTransaction> dnTXs = transactions
              .getDatanodeTransactions(datanodeID);
          dnTxCount += dnTXs.size();
          // TODO commandQueue needs a cap.
          // We should stop caching new commands if num of un-processed
          // command is bigger than a limit, e.g 50. In case datanode goes
          // offline for sometime, the cached commands be flooded.
          nodeManager.addDatanodeCommand(datanodeID,
              new DeleteBlocksCommand(dnTXs));
          LOG.debug(
              "Added delete block command for datanode {} in the queue,"
                  + " number of delete block transactions: {}, TxID list: {}",
              datanodeID, dnTXs.size(),
              String.join(",", transactions.getTransactionIDList(datanodeID)));
        }
      }

      if (dnTxCount > 0) {
        LOG.info("Totally added {} delete blocks command for"
            + " {} datanodes, task elapsed time: {}ms",
            dnTxCount, transactions.getDatanodes().size(),
            Time.monotonicNow() - startTime);
      }

      return EmptyTaskResult.newResult();
    }

    // Scan deleteBlocks.db to get a number of to-delete blocks.
    // this is going to be properly throttled.
    private DatanodeDeletedBlockTransactions getToDeleteContainerBlocks() {
      DatanodeDeletedBlockTransactions dnTXs =
          new DatanodeDeletedBlockTransactions();
      List<DeletedBlocksTransaction> txs = null;
      try {
        // Get a limited number of TXs to send via HB at a time.
        txs = deletedBlockLog
            .getTransactions(BLOCK_DELETE_TX_PER_REQUEST_LIMIT);
        LOG.debug("Scanned deleted blocks log and got {} delTX to process",
            txs.size());
      } catch (IOException e) {
        // We may tolerant a number of failures for sometime
        // but if it continues to fail, at some point we need to raise
        // an exception and probably fail the SCM ? At present, it simply
        // continues to retry the scanning.
        LOG.error("Failed to get block deletion transactions from delTX log",
            e);
      }

      if (txs != null) {
        for (DeletedBlocksTransaction tx : txs) {
          try {
            ContainerInfo info = mappingService
                .getContainer(tx.getContainerName());
            // Find out the datanode where this TX is supposed to send to.
            info.getPipeline().getMachines()
                .forEach(entry -> dnTXs.addTransaction(entry, tx));
          } catch (IOException e) {
            LOG.warn("Container {} not found, continue to process next",
                tx.getContainerName(), e);
          }
        }
      }
      return dnTXs;
    }
  }

  /**
   * A wrapper class to hold info about datanode and all deleted block
   * transactions that will be sent to this datanode.
   */
  private static class DatanodeDeletedBlockTransactions {

    // A list of TXs mapped to a certain datanode ID.
    private final Map<DatanodeID, List<DeletedBlocksTransaction>> transactions;

    DatanodeDeletedBlockTransactions() {
      this.transactions = Maps.newHashMap();
    }

    void addTransaction(DatanodeID dnID, DeletedBlocksTransaction tx) {
      if (transactions.containsKey(dnID)) {
        transactions.get(dnID).add(tx);
      } else {
        List<DeletedBlocksTransaction> first = Lists.newArrayList();
        first.add(tx);
        transactions.put(dnID, first);
      }
      LOG.debug("Transaction added: {} <- TX({})", dnID, tx.getTxID());
    }

    Set<DatanodeID> getDatanodes() {
      return transactions.keySet();
    }

    boolean isEmpty() {
      return transactions.isEmpty();
    }

    boolean hasTransactions(DatanodeID dnID) {
      return transactions.containsKey(dnID) &&
          !transactions.get(dnID).isEmpty();
    }

    List<DeletedBlocksTransaction> getDatanodeTransactions(DatanodeID dnID) {
      return transactions.get(dnID);
    }

    List<String> getTransactionIDList(DatanodeID dnID) {
      if (hasTransactions(dnID)) {
        return transactions.get(dnID).stream()
            .map(DeletedBlocksTransaction::getTxID)
            .map(String::valueOf)
            .collect(Collectors.toList());
      } else {
        return Collections.emptyList();
      }
    }
  }
}
