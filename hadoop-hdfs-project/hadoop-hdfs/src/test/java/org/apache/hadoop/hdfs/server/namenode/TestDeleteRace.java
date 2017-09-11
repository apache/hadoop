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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.InternalDataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.DelayAnswer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY;

/**
 * Test race between delete and other operations.  For now only addBlock()
 * is tested since all others are acquiring FSNamesystem lock for the 
 * whole duration.
 */
public class TestDeleteRace {
  private static final int BLOCK_SIZE = 4096;
  private static final Log LOG = LogFactory.getLog(TestDeleteRace.class);
  private static final Configuration conf = new HdfsConfiguration();
  private MiniDFSCluster cluster;

  @Rule
  public Timeout timeout = new Timeout(60000 * 3);

  @Test  
  public void testDeleteAddBlockRace() throws Exception {
    testDeleteAddBlockRace(false);
  }

  @Test  
  public void testDeleteAddBlockRaceWithSnapshot() throws Exception {
    testDeleteAddBlockRace(true);
  }

  private void testDeleteAddBlockRace(boolean hasSnapshot) throws Exception {
    try {
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          SlowBlockPlacementPolicy.class, BlockPlacementPolicy.class);
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();
      final String fileName = "/testDeleteAddBlockRace";
      Path filePath = new Path(fileName);

      FSDataOutputStream out = null;
      out = fs.create(filePath);
      if (hasSnapshot) {
        SnapshotTestHelper.createSnapshot((DistributedFileSystem) fs, new Path(
            "/"), "s1");
      }

      Thread deleteThread = new DeleteThread(fs, filePath);
      deleteThread.start();

      try {
        // write data and syn to make sure a block is allocated.
        out.write(new byte[32], 0, 32);
        out.hsync();
        Assert.fail("Should have failed.");
      } catch (FileNotFoundException e) {
        GenericTestUtils.assertExceptionContains(filePath.getName(), e);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private static class SlowBlockPlacementPolicy extends
      BlockPlacementPolicyDefault {
    @Override
    public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                      int numOfReplicas,
                                      Node writer,
                                      List<DatanodeStorageInfo> chosenNodes,
                                      boolean returnChosenNodes,
                                      Set<Node> excludedNodes,
                                      long blocksize,
                                      final BlockStoragePolicy storagePolicy,
                                      EnumSet<AddBlockFlag> flags) {
      DatanodeStorageInfo[] results = super.chooseTarget(srcPath,
          numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
          blocksize, storagePolicy, flags);
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {}
      return results;
    }
  }

  private class DeleteThread extends Thread {
    private FileSystem fs;
    private Path path;

    DeleteThread(FileSystem fs, Path path) {
      this.fs = fs;
      this.path = path;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(1000);
        LOG.info("Deleting" + path);
        final FSDirectory fsdir = cluster.getNamesystem().dir;
        INode fileINode = fsdir.getINode4Write(path.toString());
        INodeMap inodeMap = (INodeMap) Whitebox.getInternalState(fsdir,
            "inodeMap");

        fs.delete(path, false);
        // after deletion, add the inode back to the inodeMap
        inodeMap.put(fileINode);
        LOG.info("Deleted" + path);
      } catch (Exception e) {
        LOG.info(e);
      }
    }
  }

  private class RenameThread extends Thread {
    private FileSystem fs;
    private Path from;
    private Path to;

    RenameThread(FileSystem fs, Path from, Path to) {
      this.fs = fs;
      this.from = from;
      this.to = to;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(1000);
        LOG.info("Renaming " + from + " to " + to);

        fs.rename(from, to);
        LOG.info("Renamed " + from + " to " + to);
      } catch (Exception e) {
        LOG.info(e);
      }
    }
  }

  @Test
  public void testRenameRace() throws Exception {
    try {
      conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
          SlowBlockPlacementPolicy.class, BlockPlacementPolicy.class);
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();
      Path dirPath1 = new Path("/testRenameRace1");
      Path dirPath2 = new Path("/testRenameRace2");
      Path filePath = new Path("/testRenameRace1/file1");
      

      fs.mkdirs(dirPath1);
      FSDataOutputStream out = fs.create(filePath);
      Thread renameThread = new RenameThread(fs, dirPath1, dirPath2);
      renameThread.start();

      // write data and close to make sure a block is allocated.
      out.write(new byte[32], 0, 32);
      out.close();

      // Restart name node so that it replays edit. If old path was
      // logged in edit, it will fail to come up.
      cluster.restartNameNode(0);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Test race between delete operation and commitBlockSynchronization method.
   * See HDFS-6825.
   * @param hasSnapshot
   * @throws Exception
   */
  private void testDeleteAndCommitBlockSynchronizationRace(boolean hasSnapshot)
      throws Exception {
    LOG.info("Start testing, hasSnapshot: " + hasSnapshot);
    ArrayList<AbstractMap.SimpleImmutableEntry<String, Boolean>> testList =
        new ArrayList<AbstractMap.SimpleImmutableEntry<String, Boolean>> ();
    testList.add(
        new AbstractMap.SimpleImmutableEntry<String, Boolean>("/test-file", false));
    testList.add(     
        new AbstractMap.SimpleImmutableEntry<String, Boolean>("/test-file1", true));
    testList.add(
        new AbstractMap.SimpleImmutableEntry<String, Boolean>(
            "/testdir/testdir1/test-file", false));
    testList.add(
        new AbstractMap.SimpleImmutableEntry<String, Boolean>(
            "/testdir/testdir1/test-file1", true));
    
    final Path rootPath = new Path("/");
    final Configuration conf = new Configuration();
    // Disable permissions so that another user can recover the lease.
    conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    FSDataOutputStream stm = null;
    Map<DataNode, DatanodeProtocolClientSideTranslatorPB> dnMap =
        new HashMap<DataNode, DatanodeProtocolClientSideTranslatorPB>();

    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(3)
          .build();
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();
      int stId = 0;
      for(AbstractMap.SimpleImmutableEntry<String, Boolean> stest : testList) {
        String testPath = stest.getKey();
        Boolean mkSameDir = stest.getValue();
        LOG.info("test on " + testPath + " mkSameDir: " + mkSameDir
            + " snapshot: " + hasSnapshot);
        Path fPath = new Path(testPath);
        //find grandest non-root parent
        Path grandestNonRootParent = fPath;
        while (!grandestNonRootParent.getParent().equals(rootPath)) {
          grandestNonRootParent = grandestNonRootParent.getParent();
        }
        stm = fs.create(fPath);
        LOG.info("test on " + testPath + " created " + fPath);

        // write a half block
        AppendTestUtil.write(stm, 0, BLOCK_SIZE / 2);
        stm.hflush();

        if (hasSnapshot) {
          SnapshotTestHelper.createSnapshot(fs, rootPath,
              "st" + String.valueOf(stId));
          ++stId;
        }

        // Look into the block manager on the active node for the block
        // under construction.
        NameNode nn = cluster.getNameNode();
        ExtendedBlock blk = DFSTestUtil.getFirstBlock(fs, fPath);
        DatanodeDescriptor expectedPrimary =
            DFSTestUtil.getExpectedPrimaryNode(nn, blk);
        LOG.info("Expecting block recovery to be triggered on DN " +
            expectedPrimary);

        // Find the corresponding DN daemon, and spy on its connection to the
        // active.
        DataNode primaryDN = cluster.getDataNode(expectedPrimary.getIpcPort());
        DatanodeProtocolClientSideTranslatorPB nnSpy = dnMap.get(primaryDN);
        if (nnSpy == null) {
          nnSpy = InternalDataNodeTestUtils.spyOnBposToNN(primaryDN, nn);
          dnMap.put(primaryDN, nnSpy);
        }

        // Delay the commitBlockSynchronization call
        DelayAnswer delayer = new DelayAnswer(LOG);
        Mockito.doAnswer(delayer).when(nnSpy).commitBlockSynchronization(
            Mockito.eq(blk),
            Mockito.anyInt(),  // new genstamp
            Mockito.anyLong(), // new length
            Mockito.eq(true),  // close file
            Mockito.eq(false), // delete block
            (DatanodeID[]) Mockito.anyObject(), // new targets
            (String[]) Mockito.anyObject());    // new target storages

        fs.recoverLease(fPath);

        LOG.info("Waiting for commitBlockSynchronization call from primary");
        delayer.waitForCall();

        LOG.info("Deleting recursively " + grandestNonRootParent);
        fs.delete(grandestNonRootParent, true);
        if (mkSameDir && !grandestNonRootParent.toString().equals(testPath)) {
          LOG.info("Recreate dir " + grandestNonRootParent + " testpath: "
              + testPath);
          fs.mkdirs(grandestNonRootParent);
        }
        delayer.proceed();
        LOG.info("Now wait for result");
        delayer.waitForResult();
        Throwable t = delayer.getThrown();
        if (t != null) {
          LOG.info("Result exception (snapshot: " + hasSnapshot + "): " + t);
        }
      } // end of loop each fPath
      LOG.info("Now check we can restart");
      cluster.restartNameNodes();
      LOG.info("Restart finished");
    } finally {
      if (stm != null) {
        IOUtils.closeStream(stm);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test(timeout=600000)
  public void testDeleteAndCommitBlockSynchonizationRaceNoSnapshot()
      throws Exception {
    testDeleteAndCommitBlockSynchronizationRace(false);
  }

  @Test(timeout=600000)
  public void testDeleteAndCommitBlockSynchronizationRaceHasSnapshot()
      throws Exception {
    testDeleteAndCommitBlockSynchronizationRace(true);
  }


  /**
   * Test the sequence of deleting a file that has snapshot,
   * and lease manager's hard limit recovery.
   */
  @Test
  public void testDeleteAndLeaseRecoveryHardLimitSnapshot() throws Exception {
    final Path rootPath = new Path("/");
    final Configuration config = new Configuration();
    // Disable permissions so that another user can recover the lease.
    config.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, false);
    config.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    FSDataOutputStream stm = null;
    try {
      cluster = new MiniDFSCluster.Builder(config).numDataNodes(3).build();
      cluster.waitActive();

      final DistributedFileSystem fs = cluster.getFileSystem();
      final Path testPath = new Path("/testfile");
      stm = fs.create(testPath);
      LOG.info("test on " + testPath);

      // write a half block
      AppendTestUtil.write(stm, 0, BLOCK_SIZE / 2);
      stm.hflush();

      // create a snapshot, so delete does not release the file's inode.
      SnapshotTestHelper.createSnapshot(fs, rootPath, "snap");

      // delete the file without closing it.
      fs.delete(testPath, false);

      // write enough bytes to trigger an addBlock, which would fail in
      // the streamer.
      AppendTestUtil.write(stm, 0, BLOCK_SIZE);

      // Mock a scenario that the lease reached hard limit.
      final LeaseManager lm = (LeaseManager) Whitebox
          .getInternalState(cluster.getNameNode().getNamesystem(),
              "leaseManager");
      final TreeSet<Lease> leases =
          (TreeSet<Lease>) Whitebox.getInternalState(lm, "sortedLeases");
      final TreeSet<Lease> spyLeases = new TreeSet<>(new Comparator<Lease>() {
        @Override
        public int compare(Lease o1, Lease o2) {
          return Long.signum(o1.getLastUpdate() - o2.getLastUpdate());
        }
      });
      while (!leases.isEmpty()) {
        final Lease lease = leases.first();
        final Lease spyLease = Mockito.spy(lease);
        Mockito.doReturn(true).when(spyLease).expiredHardLimit();
        spyLeases.add(spyLease);
        leases.remove(lease);
      }
      Whitebox.setInternalState(lm, "sortedLeases", spyLeases);

      // wait for lease manager's background 'Monitor' class to check leases.
      Thread.sleep(2 * conf.getLong(DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_KEY,
          DFS_NAMENODE_LEASE_RECHECK_INTERVAL_MS_DEFAULT));

      LOG.info("Now check we can restart");
      cluster.restartNameNodes();
      LOG.info("Restart finished");
    } finally {
      if (stm != null) {
        IOUtils.closeStream(stm);
      }
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
