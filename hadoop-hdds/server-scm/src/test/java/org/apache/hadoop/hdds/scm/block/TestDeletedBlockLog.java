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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.SCMContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerBlocksDeletionACKProto
    .DeleteBlockTransactionResult;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.utils.MetadataKeyFilters;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys
    .OZONE_SCM_BLOCK_DELETION_MAX_RETRY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ENABLED;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

/**
 * Tests for DeletedBlockLog.
 */
public class TestDeletedBlockLog {

  private static DeletedBlockLogImpl deletedBlockLog;
  private OzoneConfiguration conf;
  private File testDir;
  private ContainerManager containerManager;
  private StorageContainerManager scm;
  private List<DatanodeDetails> dnList;

  @Before
  public void setup() throws Exception {
    testDir = GenericTestUtils.getTestDir(
        TestDeletedBlockLog.class.getSimpleName());
    conf = new OzoneConfiguration();
    conf.set(OZONE_ENABLED, "true");
    conf.setInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    scm = TestUtils.getScm(conf);
    containerManager = Mockito.mock(SCMContainerManager.class);
    deletedBlockLog = new DeletedBlockLogImpl(conf, containerManager,
        scm.getScmMetadataStore());
    dnList = new ArrayList<>(3);
    setupContainerManager();
  }

  private void setupContainerManager() throws IOException {
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID().toString())
            .build());
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID().toString())
            .build());
    dnList.add(
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID().toString())
            .build());

    final ContainerInfo container =
        new ContainerInfo.Builder().setContainerID(1)
            .setReplicationFactor(ReplicationFactor.THREE)
            .setState(HddsProtos.LifeCycleState.CLOSED)
            .build();
    final Set<ContainerReplica> replicaSet = dnList.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(container.containerID())
            .setContainerState(ContainerReplicaProto.State.OPEN)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());

    when(containerManager.getContainerReplicas(anyObject()))
        .thenReturn(replicaSet);
    when(containerManager.getContainer(anyObject()))
        .thenReturn(container);
  }

  @After
  public void tearDown() throws Exception {
    deletedBlockLog.close();
    scm.stop();
    scm.join();
    FileUtils.deleteDirectory(testDir);
  }

  private Map<Long, List<Long>> generateData(int dataSize) {
    Map<Long, List<Long>> blockMap = new HashMap<>();
    Random random = new Random(1);
    int continerIDBase = random.nextInt(100);
    int localIDBase = random.nextInt(1000);
    for (int i = 0; i < dataSize; i++) {
      long containerID = continerIDBase + i;
      List<Long> blocks = new ArrayList<>();
      int blockSize = random.nextInt(30) + 1;
      for (int j = 0; j < blockSize; j++)  {
        long localID = localIDBase + j;
        blocks.add(localID);
      }
      blockMap.put(containerID, blocks);
    }
    return blockMap;
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults,
      DatanodeDetails... dns) {
    for (DatanodeDetails dnDetails : dns) {
      deletedBlockLog
          .commitTransactions(transactionResults, dnDetails.getUuid());
    }
  }

  private void commitTransactions(
      List<DeleteBlockTransactionResult> transactionResults) {
    commitTransactions(transactionResults,
        dnList.toArray(new DatanodeDetails[3]));
  }

  private void commitTransactions(
      Collection<DeletedBlocksTransaction> deletedBlocksTransactions,
      DatanodeDetails... dns) {
    commitTransactions(deletedBlocksTransactions.stream()
        .map(this::createDeleteBlockTransactionResult)
        .collect(Collectors.toList()), dns);
  }

  private void commitTransactions(
      Collection<DeletedBlocksTransaction> deletedBlocksTransactions) {
    commitTransactions(deletedBlocksTransactions.stream()
        .map(this::createDeleteBlockTransactionResult)
        .collect(Collectors.toList()));
  }

  private DeleteBlockTransactionResult createDeleteBlockTransactionResult(
      DeletedBlocksTransaction transaction) {
    return DeleteBlockTransactionResult.newBuilder()
        .setContainerID(transaction.getContainerID()).setSuccess(true)
        .setTxID(transaction.getTxID()).build();
  }

  private List<DeletedBlocksTransaction> getTransactions(
      int maximumAllowedTXNum) throws IOException {
    DatanodeDeletedBlockTransactions transactions =
        new DatanodeDeletedBlockTransactions(containerManager,
            maximumAllowedTXNum, 3);
    deletedBlockLog.getTransactions(transactions);
    return transactions.getDatanodeTransactions(dnList.get(0).getUuid());
  }

  @Test
  public void testIncrementCount() throws Exception {
    int maxRetry = conf.getInt(OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 20);

    // Create 30 TXs in the log.
    for (Map.Entry<Long, List<Long>> entry : generateData(30).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }

    // This will return all TXs, total num 30.
    List<DeletedBlocksTransaction> blocks =
        getTransactions(40);
    List<Long> txIDs = blocks.stream().map(DeletedBlocksTransaction::getTxID)
        .collect(Collectors.toList());

    for (int i = 0; i < maxRetry; i++) {
      deletedBlockLog.incrementCount(txIDs);
    }

    // Increment another time so it exceed the maxRetry.
    // On this call, count will be set to -1 which means TX eventually fails.
    deletedBlockLog.incrementCount(txIDs);
    blocks = getTransactions(40);
    for (DeletedBlocksTransaction block : blocks) {
      Assert.assertEquals(-1, block.getCount());
    }

    // If all TXs are failed, getTransactions call will always return nothing.
    blocks = getTransactions(40);
    Assert.assertEquals(blocks.size(), 0);
  }

  @Test
  public void testCommitTransactions() throws Exception {
    for (Map.Entry<Long, List<Long>> entry : generateData(50).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }
    List<DeletedBlocksTransaction> blocks =
        getTransactions(20);
    // Add an invalid txn.
    blocks.add(
        DeletedBlocksTransaction.newBuilder().setContainerID(1).setTxID(70)
            .setCount(0).addLocalID(0).build());
    commitTransactions(blocks);
    blocks.remove(blocks.size() - 1);

    blocks = getTransactions(50);
    Assert.assertEquals(30, blocks.size());
    commitTransactions(blocks, dnList.get(1), dnList.get(2),
        DatanodeDetails.newBuilder().setUuid(UUID.randomUUID().toString())
            .build());

    blocks = getTransactions(50);
    Assert.assertEquals(30, blocks.size());
    commitTransactions(blocks, dnList.get(0));

    blocks = getTransactions(50);
    Assert.assertEquals(0, blocks.size());
  }

  @Test
  public void testRandomOperateTransactions() throws Exception {
    Random random = new Random();
    int added = 0, committed = 0;
    List<DeletedBlocksTransaction> blocks = new ArrayList<>();
    List<Long> txIDs = new ArrayList<>();
    byte[] latestTxid = DFSUtil.string2Bytes("#LATEST_TXID#");
    MetadataKeyFilters.MetadataKeyFilter avoidLatestTxid =
        (preKey, currentKey, nextKey) ->
            !Arrays.equals(latestTxid, currentKey);
    // Randomly add/get/commit/increase transactions.
    for (int i = 0; i < 100; i++) {
      int state = random.nextInt(4);
      if (state == 0) {
        for (Map.Entry<Long, List<Long>> entry :
            generateData(10).entrySet()){
          deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
        }
        added += 10;
      } else if (state == 1) {
        blocks = getTransactions(20);
        txIDs = new ArrayList<>();
        for (DeletedBlocksTransaction block : blocks) {
          txIDs.add(block.getTxID());
        }
        deletedBlockLog.incrementCount(txIDs);
      } else if (state == 2) {
        commitTransactions(blocks);
        committed += blocks.size();
        blocks = new ArrayList<>();
      } else {
        // verify the number of added and committed.
        try (TableIterator<Long,
            ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iter =
            scm.getScmMetadataStore().getDeletedBlocksTXTable().iterator()) {
          AtomicInteger count = new AtomicInteger();
          iter.forEachRemaining((keyValue) -> count.incrementAndGet());
          Assert.assertEquals(added, count.get() + committed);
        }
      }
    }
    blocks = getTransactions(1000);
    commitTransactions(blocks);
  }

  @Test
  public void testPersistence() throws Exception {
    for (Map.Entry<Long, List<Long>> entry : generateData(50).entrySet()){
      deletedBlockLog.addTransaction(entry.getKey(), entry.getValue());
    }
    // close db and reopen it again to make sure
    // transactions are stored persistently.
    deletedBlockLog.close();
    deletedBlockLog = new DeletedBlockLogImpl(conf, containerManager,
        scm.getScmMetadataStore());
    List<DeletedBlocksTransaction> blocks =
        getTransactions(10);
    commitTransactions(blocks);
    blocks = getTransactions(100);
    Assert.assertEquals(40, blocks.size());
    commitTransactions(blocks);
  }

  @Test
  public void testDeletedBlockTransactions() throws IOException {
    int txNum = 10;
    int maximumAllowedTXNum = 5;
    List<DeletedBlocksTransaction> blocks = null;
    List<Long> containerIDs = new LinkedList<>();
    DatanodeDetails dnId1 = dnList.get(0), dnId2 = dnList.get(1);

    int count = 0;
    long containerID = 0L;

    // Creates {TXNum} TX in the log.
    for (Map.Entry<Long, List<Long>> entry : generateData(txNum)
        .entrySet()) {
      count++;
      containerID = entry.getKey();
      containerIDs.add(containerID);
      deletedBlockLog.addTransaction(containerID, entry.getValue());

      // make TX[1-6] for datanode1; TX[7-10] for datanode2
      if (count <= (maximumAllowedTXNum + 1)) {
        mockContainerInfo(containerID, dnId1);
      } else {
        mockContainerInfo(containerID, dnId2);
      }
    }

    DatanodeDeletedBlockTransactions transactions =
        new DatanodeDeletedBlockTransactions(containerManager,
            maximumAllowedTXNum, 2);
    deletedBlockLog.getTransactions(transactions);

    for (UUID id : transactions.getDatanodeIDs()) {
      List<DeletedBlocksTransaction> txs = transactions
          .getDatanodeTransactions(id);
      // delete TX ID
      commitTransactions(txs);
    }

    blocks = getTransactions(txNum);
    // There should be one block remained since dnID1 reaches
    // the maximum value (5).
    Assert.assertEquals(1, blocks.size());

    Assert.assertFalse(transactions.isFull());
    // The number of TX in dnID1 won't more than maximum value.
    Assert.assertEquals(maximumAllowedTXNum,
        transactions.getDatanodeTransactions(dnId1.getUuid()).size());

    int size = transactions.getDatanodeTransactions(dnId2.getUuid()).size();
    // add duplicated container in dnID2, this should be failed.
    DeletedBlocksTransaction.Builder builder =
        DeletedBlocksTransaction.newBuilder();
    builder.setTxID(11);
    builder.setContainerID(containerID);
    builder.setCount(0);
    transactions.addTransaction(builder.build(),
        null);

    // The number of TX in dnID2 should not be changed.
    Assert.assertEquals(size,
        transactions.getDatanodeTransactions(dnId2.getUuid()).size());

    // Add new TX in dnID2, then dnID2 will reach maximum value.
    containerID = RandomUtils.nextLong();
    builder = DeletedBlocksTransaction.newBuilder();
    builder.setTxID(12);
    builder.setContainerID(containerID);
    builder.setCount(0);
    mockContainerInfo(containerID, dnId2);
    transactions.addTransaction(builder.build(),
        null);
    // Since all node are full, then transactions is full.
    Assert.assertTrue(transactions.isFull());
  }

  private void mockContainerInfo(long containerID, DatanodeDetails dd)
      throws IOException {
    List<DatanodeDetails> dns = Collections.singletonList(dd);
    Pipeline pipeline = Pipeline.newBuilder()
            .setType(ReplicationType.STAND_ALONE)
            .setFactor(ReplicationFactor.ONE)
            .setState(Pipeline.PipelineState.OPEN)
            .setId(PipelineID.randomId())
            .setNodes(dns)
            .build();

    ContainerInfo.Builder builder = new ContainerInfo.Builder();
    builder.setPipelineID(pipeline.getId())
        .setReplicationType(pipeline.getType())
        .setReplicationFactor(pipeline.getFactor());

    ContainerInfo containerInfo = builder.build();
    Mockito.doReturn(containerInfo).when(containerManager)
        .getContainer(ContainerID.valueof(containerID));

    final Set<ContainerReplica> replicaSet = dns.stream()
        .map(datanodeDetails -> ContainerReplica.newBuilder()
            .setContainerID(containerInfo.containerID())
            .setContainerState(ContainerReplicaProto.State.OPEN)
            .setDatanodeDetails(datanodeDetails)
            .build())
        .collect(Collectors.toSet());
    when(containerManager.getContainerReplicas(
        ContainerID.valueof(containerID)))
        .thenReturn(replicaSet);
  }
}
