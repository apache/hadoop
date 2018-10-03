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
package org.apache.hadoop.hdds.scm.block;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTaskResult.EmptyTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys
    .OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT;

/**
 * A background service running in SCM to delete blocks. This service scans
 * block deletion log in certain interval and caches block deletion commands
 * in {@link org.apache.hadoop.hdds.scm.node.CommandQueue}, asynchronously
 * SCM HB thread polls cached commands and sends them to datanode for physical
 * processing.
 */
public class SCMBlockDeletingService extends BackgroundService {

  public static final Logger LOG =
      LoggerFactory.getLogger(SCMBlockDeletingService.class);

  // ThreadPoolSize=2, 1 for scheduler and the other for the scanner.
  private final static int BLOCK_DELETING_SERVICE_CORE_POOL_SIZE = 2;
  private final DeletedBlockLog deletedBlockLog;
  private final ContainerManager containerManager;
  private final NodeManager nodeManager;
  private final EventPublisher eventPublisher;

  // Block delete limit size is dynamically calculated based on container
  // delete limit size (ozone.block.deleting.container.limit.per.interval)
  // that configured for datanode. To ensure DN not wait for
  // delete commands, we use this value multiply by a factor 2 as the final
  // limit TX size for each node.
  // Currently we implement a throttle algorithm that throttling delete blocks
  // for each datanode. Each node is limited by the calculation size. Firstly
  // current node info is fetched from nodemanager, then scan entire delLog
  // from the beginning to end. If one node reaches maximum value, its records
  // will be skipped. If not, keep scanning until it reaches maximum value.
  // Once all node are full, the scan behavior will stop.
  private int blockDeleteLimitSize;

  public SCMBlockDeletingService(DeletedBlockLog deletedBlockLog,
      ContainerManager containerManager, NodeManager nodeManager,
      EventPublisher eventPublisher, long interval, long serviceTimeout,
      Configuration conf) {
    super("SCMBlockDeletingService", interval, TimeUnit.MILLISECONDS,
        BLOCK_DELETING_SERVICE_CORE_POOL_SIZE, serviceTimeout);
    this.deletedBlockLog = deletedBlockLog;
    this.containerManager = containerManager;
    this.nodeManager = nodeManager;
    this.eventPublisher = eventPublisher;

    int containerLimit = conf.getInt(
        OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL,
        OZONE_BLOCK_DELETING_CONTAINER_LIMIT_PER_INTERVAL_DEFAULT);
    Preconditions.checkArgument(containerLimit > 0,
        "Container limit size should be " + "positive.");
    // Use container limit value multiply by a factor 2 to ensure DN
    // not wait for orders.
    this.blockDeleteLimitSize = containerLimit * 2;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new DeletedBlockTransactionScanner());
    return queue;
  }

  public void handlePendingDeletes(PendingDeleteStatusList deletionStatusList) {
    DatanodeDetails dnDetails = deletionStatusList.getDatanodeDetails();
    for (PendingDeleteStatusList.PendingDeleteStatus deletionStatus :
        deletionStatusList.getPendingDeleteStatuses()) {
      LOG.info(
          "Block deletion txnID mismatch in datanode {} for containerID {}."
              + " Datanode delete txnID: {}, SCM txnID: {}",
          dnDetails.getUuid(), deletionStatus.getContainerId(),
          deletionStatus.getDnDeleteTransactionId(),
          deletionStatus.getScmDeleteTransactionId());
    }
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
      DatanodeDeletedBlockTransactions transactions = null;
      List<DatanodeDetails> datanodes = nodeManager.getNodes(NodeState.HEALTHY);
      Map<Long, Long> transactionMap = null;
      if (datanodes != null) {
        transactions = new DatanodeDeletedBlockTransactions(containerManager,
            blockDeleteLimitSize, datanodes.size());
        try {
          transactionMap = deletedBlockLog.getTransactions(transactions);
        } catch (IOException e) {
          // We may tolerant a number of failures for sometime
          // but if it continues to fail, at some point we need to raise
          // an exception and probably fail the SCM ? At present, it simply
          // continues to retry the scanning.
          LOG.error("Failed to get block deletion transactions from delTX log",
              e);
        }
        LOG.debug("Scanned deleted blocks log and got {} delTX to process.",
            transactions.getTXNum());
      }

      if (transactions != null && !transactions.isEmpty()) {
        for (UUID dnId : transactions.getDatanodeIDs()) {
          List<DeletedBlocksTransaction> dnTXs = transactions
              .getDatanodeTransactions(dnId);
          if (dnTXs != null && !dnTXs.isEmpty()) {
            dnTxCount += dnTXs.size();
            // TODO commandQueue needs a cap.
            // We should stop caching new commands if num of un-processed
            // command is bigger than a limit, e.g 50. In case datanode goes
            // offline for sometime, the cached commands be flooded.
            eventPublisher.fireEvent(SCMEvents.RETRIABLE_DATANODE_COMMAND,
                new CommandForDatanode<>(dnId, new DeleteBlocksCommand(dnTXs)));
            LOG.debug(
                "Added delete block command for datanode {} in the queue,"
                    + " number of delete block transactions: {}, TxID list: {}",
                dnId, dnTXs.size(), String.join(",",
                    transactions.getTransactionIDList(dnId)));
          }
        }
        containerManager.updateDeleteTransactionId(transactionMap);
      }

      if (dnTxCount > 0) {
        LOG.info(
            "Totally added {} delete blocks command for"
                + " {} datanodes, task elapsed time: {}ms",
            dnTxCount, transactions.getDatanodeIDs().size(),
            Time.monotonicNow() - startTime);
      }

      return EmptyTaskResult.newResult();
    }
  }

  @VisibleForTesting
  public void setBlockDeleteTXNum(int numTXs) {
    blockDeleteLimitSize = numTXs;
  }
}
