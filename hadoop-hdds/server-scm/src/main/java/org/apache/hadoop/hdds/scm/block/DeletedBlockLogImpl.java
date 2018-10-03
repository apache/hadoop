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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;
import org.apache.hadoop.hdds.scm.command
    .CommandStatusReportHandler.DeleteBlockStatus;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.utils.BatchOperation;
import org.apache.hadoop.utils.MetadataStore;
import org.apache.hadoop.utils.MetadataStoreBuilder;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
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

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_DB_CACHE_SIZE_MB;
import static org.apache.hadoop.hdds.server.ServerUtils.getOzoneMetaDirPath;
import static org.apache.hadoop.ozone.OzoneConsts.DELETED_BLOCK_DB;

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

  private static final byte[] LATEST_TXID =
      DFSUtil.string2Bytes("#LATEST_TXID#");

  private final int maxRetry;
  private final MetadataStore deletedStore;
  private final ContainerManager containerManager;
  private final Lock lock;
  // The latest id of deleted blocks in the db.
  private long lastTxID;
  // Maps txId to set of DNs which are successful in committing the transaction
  private Map<Long, Set<UUID>> transactionToDNsCommitMap;

  public DeletedBlockLogImpl(Configuration conf,
     ContainerManager containerManager) throws IOException {
    maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY,
        OZONE_SCM_BLOCK_DELETION_MAX_RETRY_DEFAULT);

    File metaDir = getOzoneMetaDirPath(conf);
    String scmMetaDataDir = metaDir.getPath();
    File deletedLogDbPath = new File(scmMetaDataDir, DELETED_BLOCK_DB);
    int cacheSize = conf.getInt(OZONE_SCM_DB_CACHE_SIZE_MB,
        OZONE_SCM_DB_CACHE_SIZE_DEFAULT);
    // Load store of all transactions.
    deletedStore = MetadataStoreBuilder.newBuilder()
        .setCreateIfMissing(true)
        .setConf(conf)
        .setDbFile(deletedLogDbPath)
        .setCacheSize(cacheSize * OzoneConsts.MB)
        .build();
    this.containerManager = containerManager;

    this.lock = new ReentrantLock();
    // start from the head of deleted store.
    lastTxID = findLatestTxIDInStore();

    // transactionToDNsCommitMap is updated only when
    // transaction is added to the log and when it is removed.

    // maps transaction to dns which have committed it.
    transactionToDNsCommitMap = new ConcurrentHashMap<>();
  }

  @VisibleForTesting
  public MetadataStore getDeletedStore() {
    return deletedStore;
  }

  /**
   * There is no need to lock before reading because
   * it's only used in construct method.
   *
   * @return latest txid.
   * @throws IOException
   */
  private long findLatestTxIDInStore() throws IOException {
    long txid = 0;
    byte[] value = deletedStore.get(LATEST_TXID);
    if (value != null) {
      txid = Longs.fromByteArray(value);
    }
    return txid;
  }

  @Override
  public List<DeletedBlocksTransaction> getFailedTransactions()
      throws IOException {
    lock.lock();
    try {
      final List<DeletedBlocksTransaction> failedTXs = Lists.newArrayList();
      deletedStore.iterate(null, (key, value) -> {
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction delTX =
              DeletedBlocksTransaction.parseFrom(value);
          if (delTX.getCount() == -1) {
            failedTXs.add(delTX);
          }
        }
        return true;
      });
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
    BatchOperation batch = new BatchOperation();
    lock.lock();
    try {
      for(Long txID : txIDs) {
        try {
          byte[] deleteBlockBytes =
              deletedStore.get(Longs.toByteArray(txID));
          if (deleteBlockBytes == null) {
            LOG.warn("Delete txID {} not found", txID);
            continue;
          }
          DeletedBlocksTransaction block = DeletedBlocksTransaction
              .parseFrom(deleteBlockBytes);
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
          deletedStore.put(Longs.toByteArray(txID),
              builder.build().toByteArray());
        } catch (IOException ex) {
          LOG.warn("Cannot increase count for txID " + txID, ex);
        }
      }
      deletedStore.writeBatch(batch);
    } finally {
      lock.unlock();
    }
  }

  private DeletedBlocksTransaction constructNewTransaction(long txID,
      long containerID, List<Long> blocks) {
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
   * @param dnID - Id of Datanode which has acknowledged a delete block command.
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
          Long containerId = transactionResult.getContainerID();
          if (dnsWithCommittedTxn == null) {
            LOG.warn("Transaction txId={} commit by dnId={} for containerID={} "
                    + "failed. Corresponding entry not found.", txID, dnID,
                containerId);
            return;
          }

          dnsWithCommittedTxn.add(dnID);
          Pipeline pipeline =
              containerManager.getContainerWithPipeline(containerId)
                  .getPipeline();
          Collection<DatanodeDetails> containerDnsDetails =
              pipeline.getDatanodes().values();
          // The delete entry can be safely removed from the log if all the
          // corresponding nodes commit the txn. It is required to check that
          // the nodes returned in the pipeline match the replication factor.
          if (min(containerDnsDetails.size(), dnsWithCommittedTxn.size())
              >= pipeline.getFactor().getNumber()) {
            List<UUID> containerDns = containerDnsDetails.stream()
                .map(DatanodeDetails::getUuid)
                .collect(Collectors.toList());
            if (dnsWithCommittedTxn.containsAll(containerDns)) {
              transactionToDNsCommitMap.remove(txID);
              LOG.debug("Purging txId={} from block deletion log", txID);
              deletedStore.delete(Longs.toByteArray(txID));
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
   * @param blocks - blocks that belong to the same container.
   * @throws IOException
   */
  @Override
  public void addTransaction(long containerID, List<Long> blocks)
      throws IOException {
    BatchOperation batch = new BatchOperation();
    lock.lock();
    try {
      DeletedBlocksTransaction tx = constructNewTransaction(lastTxID + 1,
          containerID, blocks);
      byte[] key = Longs.toByteArray(lastTxID + 1);

      batch.put(key, tx.toByteArray());
      batch.put(LATEST_TXID, Longs.toByteArray(lastTxID + 1));

      deletedStore.writeBatch(batch);
      lastTxID += 1;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int getNumOfValidTransactions() throws IOException {
    lock.lock();
    try {
      final AtomicInteger num = new AtomicInteger(0);
      deletedStore.iterate(null, (key, value) -> {
        // Exclude latest txid record
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction delTX =
              DeletedBlocksTransaction.parseFrom(value);
          if (delTX.getCount() > -1) {
            num.incrementAndGet();
          }
        }
        return true;
      });
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
  public void addTransactions(
      Map<Long, List<Long>> containerBlocksMap)
      throws IOException {
    BatchOperation batch = new BatchOperation();
    lock.lock();
    try {
      long currentLatestID = lastTxID;
      for (Map.Entry<Long, List<Long>> entry :
          containerBlocksMap.entrySet()) {
        currentLatestID += 1;
        byte[] key = Longs.toByteArray(currentLatestID);
        DeletedBlocksTransaction tx = constructNewTransaction(currentLatestID,
            entry.getKey(), entry.getValue());
        batch.put(key, tx.toByteArray());
      }
      lastTxID = currentLatestID;
      batch.put(LATEST_TXID, Longs.toByteArray(lastTxID));
      deletedStore.writeBatch(batch);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (deletedStore != null) {
      deletedStore.close();
    }
  }

  @Override
  public Map<Long, Long> getTransactions(
      DatanodeDeletedBlockTransactions transactions) throws IOException {
    lock.lock();
    try {
      Map<Long, Long> deleteTransactionMap = new HashMap<>();
      deletedStore.iterate(null, (key, value) -> {
        if (!Arrays.equals(LATEST_TXID, key)) {
          DeletedBlocksTransaction block = DeletedBlocksTransaction
              .parseFrom(value);

          if (block.getCount() > -1 && block.getCount() <= maxRetry) {
            if (transactions.addTransaction(block,
                transactionToDNsCommitMap.get(block.getTxID()))) {
              deleteTransactionMap.put(block.getContainerID(), block.getTxID());
              transactionToDNsCommitMap
                  .putIfAbsent(block.getTxID(), new ConcurrentHashSet<>());
            }
          }
          return !transactions.isFull();
        }
        return true;
      });
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
