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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.fs.StorageType.DEFAULT;
import static org.apache.hadoop.fs.StorageType.RAM_DISK;
import static org.junit.Assert.assertTrue;

public class TestLazyPersistReplicaRecovery extends LazyPersistTestCase {
  @Test
  public void testDnRestartWithSavedReplicas()
      throws IOException, InterruptedException, TimeoutException {

    getClusterBuilder().build();
    FSNamesystem fsn = cluster.getNamesystem();
    final DataNode dn = cluster.getDataNodes().get(0);
    DatanodeDescriptor dnd =
        NameNodeAdapter.getDatanode(fsn, dn.getDatanodeId());
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");

    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    // Sleep for a short time to allow the lazy writer thread to do its job.
    // However the block replica should not be evicted from RAM_DISK yet.
    FsDatasetImpl fsDImpl = (FsDatasetImpl) DataNodeTestUtils.getFSDataset(dn);
    GenericTestUtils
        .waitFor(() -> fsDImpl.getNonPersistentReplicas() == 0, 10,
            3 * LAZY_WRITER_INTERVAL_SEC * 1000);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    assertTrue("DN did not restart properly",
        cluster.restartDataNode(0, true));
    // wait for blockreport
    waitForBlockReport(dn, dnd);
    // Ensure that the replica is now on persistent storage.
    ensureFileReplicasOnStorageType(path1, DEFAULT);
  }

  @Test
  public void testDnRestartWithUnsavedReplicas()
      throws IOException, InterruptedException, TimeoutException {

    getClusterBuilder().build();
    FsDatasetTestUtil.stopLazyWriter(cluster.getDataNodes().get(0));

    final String METHOD_NAME = GenericTestUtils.getMethodName();
    Path path1 = new Path("/" + METHOD_NAME + ".01.dat");
    makeTestFile(path1, BLOCK_SIZE, true);
    ensureFileReplicasOnStorageType(path1, RAM_DISK);

    LOG.info("Restarting the DataNode");
    cluster.restartDataNode(0, true);
    cluster.waitActive();

    // Ensure that the replica is still on transient storage.
    ensureFileReplicasOnStorageType(path1, RAM_DISK);
  }

  private boolean waitForBlockReport(final DataNode dn,
      final DatanodeDescriptor dnd) throws IOException, InterruptedException {
    final DatanodeStorageInfo storage = dnd.getStorageInfos()[0];
    final long lastCount = storage.getBlockReportCount();
    dn.triggerBlockReport(
        new BlockReportOptions.Factory().setIncremental(false).build());
    try {
      GenericTestUtils
          .waitFor(() -> lastCount != storage.getBlockReportCount(), 10, 10000);
    } catch (TimeoutException te) {
      LOG.error("Timeout waiting for block report for {}", dnd);
      return false;
    }
    return true;
  }
}
