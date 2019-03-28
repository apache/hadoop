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
package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import com.google.common.primitives.Longs;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.ozShell.TestOzoneShell;
import org.apache.hadoop.ozone.protocol.commands.RetriableDatanodeEventWatcher;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.Math.max;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds
    .HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone
    .OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;

/**
 * Tests for Block deletion.
 */
public class TestBlockDeletion {
  private static OzoneConfiguration conf = null;
  private static ObjectStore store;
  private static MiniOzoneCluster cluster = null;
  private static StorageContainerManager scm = null;
  private static OzoneManager om = null;
  private static Set<Long> containerIdsWithDeletedBlocks;
  private static long maxTransactionId = 0;

  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    GenericTestUtils.setLogLevel(DeletedBlockLogImpl.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SCMBlockDeletingService.LOG, Level.DEBUG);

    String path =
        GenericTestUtils.getTempPath(TestOzoneShell.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();

    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL,
        3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setHbInterval(200)
        .build();
    cluster.waitForClusterToBeReady();
    store = OzoneClientFactory.getRpcClient(conf).getObjectStore();
    om = cluster.getOzoneManager();
    scm = cluster.getStorageContainerManager();
    containerIdsWithDeletedBlocks = new HashSet<>();
  }

  @Test
  public void testBlockDeletion() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    String value = RandomStringUtils.random(10000000);
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String keyName = UUID.randomUUID().toString();

    OzoneOutputStream out = bucket.createKey(keyName, value.getBytes().length,
        ReplicationType.RATIS, ReplicationFactor.ONE, new HashMap<>());
    for (int i = 0; i < 100; i++) {
      out.write(value.getBytes());
    }
    out.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(volumeName)
        .setBucketName(bucketName).setKeyName(keyName).setDataSize(0)
        .setType(HddsProtos.ReplicationType.RATIS)
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setRefreshPipeline(true)
        .build();
    List<OmKeyLocationInfoGroup> omKeyLocationInfoGroupList =
        om.lookupKey(keyArgs).getKeyLocationVersions();

    // verify key blocks were created in DN.
    verifyBlocksCreated(omKeyLocationInfoGroupList);
    // No containers with deleted blocks
    Assert.assertTrue(containerIdsWithDeletedBlocks.isEmpty());
    // Delete transactionIds for the containers should be 0.
    // NOTE: this test assumes that all the container is KetValueContainer. If
    // other container types is going to be added, this test should be checked.
    matchContainerTransactionIds();
    om.deleteKey(keyArgs);
    Thread.sleep(5000);
    // The blocks should not be deleted in the DN as the container is open
    try {
      verifyBlocksDeleted(omKeyLocationInfoGroupList);
      Assert.fail("Blocks should not have been deleted");
    } catch (Throwable e) {
      Assert.assertTrue(e.getMessage().contains("expected null, but was"));
      Assert.assertEquals(e.getClass(), AssertionError.class);
    }

    // close the containers which hold the blocks for the key
    OzoneTestUtils.closeContainers(omKeyLocationInfoGroupList, scm);

    waitForDatanodeCommandRetry();
    waitForDatanodeBlockDeletionStart();
    // The blocks should be deleted in the DN.
    verifyBlocksDeleted(omKeyLocationInfoGroupList);

    // Few containers with deleted blocks
    Assert.assertTrue(!containerIdsWithDeletedBlocks.isEmpty());
    // Containers in the DN and SCM should have same delete transactionIds
    matchContainerTransactionIds();
    // Containers in the DN and SCM should have same delete transactionIds
    // after DN restart. The assertion is just to verify that the state of
    // containerInfos in dn and scm is consistent after dn restart.
    cluster.restartHddsDatanode(0, true);
    matchContainerTransactionIds();

    // verify PENDING_DELETE_STATUS event is fired
    verifyPendingDeleteEvent();

    // Verify transactions committed
    verifyTransactionsCommitted();
  }

  private void waitForDatanodeBlockDeletionStart()
      throws TimeoutException, InterruptedException {
    LogCapturer logCapturer =
        LogCapturer.captureLogs(DeleteBlocksCommandHandler.LOG);
    logCapturer.clearOutput();
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
            .contains("Start to delete container block"),
        500, 10000);
    Thread.sleep(1000);
  }

  /**
   * Waits for datanode command to be retried when datanode is dead.
   */
  private void waitForDatanodeCommandRetry()
      throws TimeoutException, InterruptedException {
    cluster.shutdownHddsDatanode(0);
    LogCapturer logCapturer =
        LogCapturer.captureLogs(RetriableDatanodeEventWatcher.LOG);
    logCapturer.clearOutput();
    GenericTestUtils.waitFor(() -> logCapturer.getOutput()
            .contains("RetriableDatanodeCommand type=deleteBlocksCommand"),
        500, 5000);
    cluster.restartHddsDatanode(0, true);
  }

  private void verifyTransactionsCommitted() throws IOException {
    DeletedBlockLogImpl deletedBlockLog =
        (DeletedBlockLogImpl) scm.getScmBlockManager().getDeletedBlockLog();
    for (long txnID = 1; txnID <= maxTransactionId; txnID++) {
      Assert.assertNull(
          scm.getScmMetadataStore().getDeletedBlocksTXTable().get(txnID));
    }
  }

  private void verifyPendingDeleteEvent()
      throws IOException, InterruptedException {
    ContainerSet dnContainerSet =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet();
    LogCapturer logCapturer =
        LogCapturer.captureLogs(SCMBlockDeletingService.LOG);
    // Create dummy container reports with deleteTransactionId set as 0
    ContainerReportsProto containerReport = dnContainerSet.getContainerReport();
    ContainerReportsProto.Builder dummyReportsBuilder =
        ContainerReportsProto.newBuilder();
    for (ContainerReplicaProto containerInfo :
        containerReport.getReportsList()) {
      dummyReportsBuilder.addReports(
          ContainerReplicaProto.newBuilder(containerInfo)
              .setDeleteTransactionId(0)
              .build());
    }
    ContainerReportsProto dummyReport = dummyReportsBuilder.build();

    logCapturer.clearOutput();
    cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().getContext().addReport(dummyReport);
    cluster.getHddsDatanodes().get(0)
        .getDatanodeStateMachine().triggerHeartbeat();
    // wait for event to be handled by event handler
    Thread.sleep(1000);
    String output = logCapturer.getOutput();
    for (ContainerReplicaProto containerInfo : dummyReport.getReportsList()) {
      long containerId = containerInfo.getContainerID();
      // Event should be triggered only for containers which have deleted blocks
      if (containerIdsWithDeletedBlocks.contains(containerId)) {
        Assert.assertTrue(output.contains(
            "for containerID " + containerId + ". Datanode delete txnID"));
      } else {
        Assert.assertTrue(!output.contains(
            "for containerID " + containerId + ". Datanode delete txnID"));
      }
    }
    logCapturer.clearOutput();
  }

  private void matchContainerTransactionIds() throws IOException {
    ContainerSet dnContainerSet =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet();
    List<ContainerData> containerDataList = new ArrayList<>();
    dnContainerSet.listContainer(0, 10000, containerDataList);
    for (ContainerData containerData : containerDataList) {
      long containerId = containerData.getContainerID();
      if (containerIdsWithDeletedBlocks.contains(containerId)) {
        Assert.assertTrue(
            scm.getContainerInfo(containerId).getDeleteTransactionId() > 0);
        maxTransactionId = max(maxTransactionId,
            scm.getContainerInfo(containerId).getDeleteTransactionId());
      } else {
        Assert.assertEquals(
            scm.getContainerInfo(containerId).getDeleteTransactionId(), 0);
      }
      Assert.assertEquals(((KeyValueContainerData)dnContainerSet
              .getContainer(containerId).getContainerData())
              .getDeleteTransactionId(),
          scm.getContainerInfo(containerId).getDeleteTransactionId());
    }
  }

  private void verifyBlocksCreated(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {
    ContainerSet dnContainerSet =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet();
    OzoneTestUtils.performOperationOnKeyContainers((blockID) -> {
      MetadataStore db = BlockUtils.getDB((KeyValueContainerData) dnContainerSet
          .getContainer(blockID.getContainerID()).getContainerData(), conf);
      Assert.assertNotNull(db.get(Longs.toByteArray(blockID.getLocalID())));
    }, omKeyLocationInfoGroups);
  }

  private void verifyBlocksDeleted(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {
    ContainerSet dnContainerSet =
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet();
    OzoneTestUtils.performOperationOnKeyContainers((blockID) -> {
      MetadataStore db = BlockUtils.getDB((KeyValueContainerData) dnContainerSet
          .getContainer(blockID.getContainerID()).getContainerData(), conf);
      Assert.assertNull(db.get(Longs.toByteArray(blockID.getLocalID())));
      Assert.assertNull(db.get(DFSUtil.string2Bytes(
          OzoneConsts.DELETING_KEY_PREFIX + blockID.getLocalID())));
      Assert.assertNotNull(DFSUtil
          .string2Bytes(OzoneConsts.DELETED_KEY_PREFIX + blockID.getLocalID()));
      containerIdsWithDeletedBlocks.add(blockID.getContainerID());
    }, omKeyLocationInfoGroups);
  }
}