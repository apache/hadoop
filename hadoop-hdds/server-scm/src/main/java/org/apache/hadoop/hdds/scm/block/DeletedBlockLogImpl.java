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
package org.apache.hadoop.hdds.scm.block;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.command
    .CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.utils.db.BatchOperation;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;

/**
 * A implement class of {@link DeletedBlockLog}, and it uses
 * K/V db to maintain block deletion transactions between scm and datanode.
 * This is a very basic implementation, it simply scans the log and
 * memorize the position that scanned by last time, and uses this to
 * determine where the next scan starts. It has no notion about weight
 * of each transaction so as long as transaction is still valid, they get
 * equally same chance to be retrieved which only depends on the nature
 * order of the transaction ID.
 */
public class DeletedBlockLogImpl
    implements DeletedBlockLog, EventHandler<DeleteBlockStatus> {

  public static final Logger LOG =
      LoggerFactory.getLogger(DeletedBlockLogImpl.class);

  private final int maxRetry;
  private final ContainerManager containerManager;
  private final SCMMetadataStore scmMetadataStore;
  private final Lock lock;
  // Maps txId to set of DNs which are successful in committing the transaction
  private Map<Long, Set<UUID>> transactionToDNsCommitMap;

  public DeletedBlockLogImpl(Configuration conf,
                             ContainerManager containerManager,
                             SCMMetadataStore scmMetadataStore) {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);
    this.containerManager = containerManager;
    this.scmMetadataStore = scmMetadataStore;
    this.lock = new ReentrantLock();

    // transactionToDNsCommitMap is updated only when
    // transaction is added to the log and when it is removed.

    // maps transaction to dns which have committed it.
    transactionToDNsCommitMap = new ConcurrentHashMap<>();
  }


  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions()
      throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               scmMetadataStore.getDeletedBlocksTXTable().iterator()) {
        while (iter.hasNext()) {
          DeletedBlocksTransaction delTX = iter.next().getValue();
          if (delTX.getCount() == -1) {
            failedTXs.add(delTX);
          }
        }
      }
      return failedTXs;
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param txIDs - transaction ID.
   * @throws IOException
   */
  @Override
  public void incrementCount(List<Long> txIDs) throws IOException {
    for (Long txID : txIDs) {
      lock.lock();
      try {
        DeletedBlocksTransaction block =
            scmMetadataStore.getDeletedBlocksTXTable().get(txID);
        if (block == null) {
          // Should we make this an error ? How can we not find the deleted
          // TXID?
          LOG.warn("Deleted TXID not found.");
          continue;
        }
        DeletedBlocksTransaction.Builder builder = block.toBuilder();
        int currentCount = block.getCount();
        if (currentCount > -1) {
          builder.setCount(++currentCount);
        }
        // if the retry time exceeds the maxRetry value
        // then set the retry value to -1, stop retrying, admins can
        // analyze those blocks and purge them manually by SCMCli.
        if (currentCount > maxRetry) {
          builder.setCount(-1);
        }
        scmMetadataStore.getDeletedBlocksTXTable().put(txID,
            builder.build());
      } catch (IOException ex) {
        LOG.warn("Cannot increase count for txID " + txID, ex);
        // We do not throw error here, since we don't want to abort the loop.
        // Just log and continue processing the rest of txids.
      } finally {
        lock.unlock();
      }
    }
  }


  private DeletedBlocksTransaction constructNewTransaction(long txID,
                                                           long containerID,
                                                           List<Long> blocks) {
    return DeletedBlocksTransaction.newBuilder()
        .setTxID(txID)
        .setContainerID(containerID)
        .addAllLocalID(blocks)
        .setCount(0)
        .build();
  }

  /**
   * {@inheritDoc}
   *
   * @param transactionResults - transaction IDs.
   * @param dnID               - Id of Datanode which has acknowledged
   *                           a delete block command.
   * @throws IOException
   */
  @Override
  public void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults, UUID dnID) {
    lock.lock();
    try {
      Set<UUID> dnsWithCommittedTxn;
      for (DeleteBlockTransactionResult transactionResult :
          transactionResults) {
        if (isTransactionFailed(transactionResult)) {
          continue;
        }
        try {
          long txID = transactionResult.getTxID();
          // set of dns which have successfully committed transaction txId.
          dnsWithCommittedTxn = transactionToDNsCommitMap.get(txID);
          final ContainerID containerId = ContainerID.valueof(
              transactionResult.getContainerID());
          if (dnsWithCommittedTxn == null) {
            LOG.warn("Transaction txId={} commit by dnId={} for containerID={} "
                    + "failed. Corresponding entry not found.", txID, dnID,
                containerId);
            return;
          }

          dnsWithCommittedTxn.add(dnID);
          final ContainerInfo container =
              containerManager.getContainer(containerId);
          final Set<ContainerReplica> replicas =
              containerManager.getContainerReplicas(containerId);
          // The delete entry can be safely removed from the log if all the
          // corresponding nodes commit the txn. It is required to check that
          // the nodes returned in the pipeline match the replication factor.
          if (min(replicas.size(), dnsWithCommittedTxn.size())
              >= container.getReplicationFactor().getNumber()) {
            List<UUID> containerDns = replicas.stream()
                .map(ContainerReplica::getDatanodeDetails)
                .map(DatanodeDetails::getUuid)
                .collect(Collectors.toList());
            if (dnsWithCommittedTxn.containsAll(containerDns)) {
              transactionToDNsCommitMap.remove(txID);
              LOG.debug("Purging txId={} from block deletion log", txID);
              scmMetadataStore.getDeletedBlocksTXTable().delete(txID);
            }
          }
          LOG.debug("Datanode txId={} containerId={} committed by dnId={}",
              txID, containerId, dnID);
        } catch (IOException e) {
          LOG.warn("Could not commit delete block transaction: " +
              transactionResult.getTxID(), e);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean isTransactionFailed(DeleteBlockTransactionResult result) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Got block deletion ACK from datanode, TXIDs={}, " + "success={}",
          result.getTxID(), result.getSuccess());
    }
    if (!result.getSuccess()) {
      LOG.warn("Got failed ACK for TXID={}, prepare to resend the "
          + "TX in next interval", result.getTxID());
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @param containerID - container ID.
   * @param blocks      - blocks that belong to the same container.
   * @throws IOException
   */
  @Override
  public void addTransaction(long containerID, List<Long> blocks)
      throws IOException {
    lock.lock();
    try {
      Long nextTXID = scmMetadataStore.getNextDeleteBlockTXID();
      DeletedBlocksTransaction tx =
          constructNewTransaction(nextTXID, containerID, blocks);
      scmMetadataStore.getDeletedBlocksTXTable().put(nextTXID, tx);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               scmMetadataStore.getDeletedBlocksTXTable().iterator()) {
        while (iter.hasNext()) {
          DeletedBlocksTransaction delTX = iter.next().getValue();
          if (delTX.getCount() > -1) {
            num.incrementAndGet();
          }
        }
      }
      return num.get();
    } finally {
      lock.unlock();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @param containerBlocksMap a map of containerBlocks.
   * @throws IOException
   */
  @Override
  public void addTransactions(Map<Long, List<Long>> containerBlocksMap)
      throws IOException {
    lock.lock();
    try {
      BatchOperation batch = scmMetadataStore.getStore().initBatchOperation();
      for (Map.Entry<Long, List<Long>> entry : containerBlocksMap.entrySet()) {
        long nextTXID = scmMetadataStore.getNextDeleteBlockTXID();
        DeletedBlocksTransaction tx = constructNewTransaction(nextTXID,
            entry.getKey(), entry.getValue());
        scmMetadataStore.getDeletedBlocksTXTable().putWithBatch(batch,
            nextTXID, tx);
      }
      scmMetadataStore.getStore().commitBatchOperation(batch);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Map<Long, Long> getTransactions(
      DatanodeDeletedBlockTransactions transactions) throws IOException {
    lock.lock();
    try {
      Map<Long, Long> deleteTransactionMap = new HashMap<>();
      try (TableIterator<Long,
          ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
               scmMetadataStore.getDeletedBlocksTXTable().iterator()) {
        while (iter.hasNext()) {
          Table.KeyValue<Long, DeletedBlocksTransaction> keyValue =
              iter.next();
          DeletedBlocksTransaction block = keyValue.getValue();
          if (block.getCount() > -1 && block.getCount() <= maxRetry) {
            if (transactions.addTransaction(block,
                transactionToDNsCommitMap.get(block.getTxID()))) {
              deleteTransactionMap.put(block.getContainerID(),
                  block.getTxID());
              transactionToDNsCommitMap
                  .putIfAbsent(block.getTxID(), new ConcurrentHashSet<>());
            }
          }
        }
      }
      return deleteTransactionMap;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onMessage(DeleteBlockStatus deleteBlockStatus,
                        EventPublisher publisher) {
    ContainerBlocksDeletionACKProto ackProto =
        deleteBlockStatus.getCmdStatus().getBlockDeletionAck();
    commitTransactions(ackProto.getResultsList(),
        UUID.fromString(ackProto.getDnId()));
  }
}
