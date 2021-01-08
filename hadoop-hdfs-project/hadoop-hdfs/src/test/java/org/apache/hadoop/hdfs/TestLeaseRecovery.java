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
package org.apache.hadoop.hdfs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.EnumSet;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockUnderConstructionFeature;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.TestInterDatanodeProtocol;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DataChecksum;
import org.junit.After;
import org.junit.Test;

public class TestLeaseRecovery {
  static final int BLOCK_SIZE = 1024;
  static final short REPLICATION_NUM = (short)3;
  private static final long LEASE_PERIOD = 300L;

  private MiniDFSCluster cluster;

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  static void checkMetaInfo(ExtendedBlock b, DataNode dn
      ) throws IOException {
    TestInterDatanodeProtocol.checkMetaInfo(b, dn);
  }
  
  static int min(Integer... x) {
    int m = x[0];
    for(int i = 1; i < x.length; i++) {
      if (x[i] < m) {
        m = x[i];
      }
    }
    return m;
  }

  void waitLeaseRecovery(MiniDFSCluster cluster) {
    cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
    // wait for the lease to expire
    try {
      Thread.sleep(2 * 3000);  // 2 heartbeat intervals
    } catch (InterruptedException e) {
    }
  }

  /**
   * The following test first creates a file with a few blocks.
   * It randomly truncates the replica of the last block stored in each datanode.
   * Finally, it triggers block synchronization to synchronize all stored block.
   */
  @Test
  public void testBlockSynchronization() throws Exception {
    final int ORG_FILE_SIZE = 3000; 
    Configuration conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(5).build();
    cluster.waitActive();

    //create a file
    DistributedFileSystem dfs = cluster.getFileSystem();
    String filestr = "/foo";
    Path filepath = new Path(filestr);
    DFSTestUtil.createFile(dfs, filepath, ORG_FILE_SIZE, REPLICATION_NUM, 0L);
    assertTrue(dfs.exists(filepath));
    DFSTestUtil.waitReplication(dfs, filepath, REPLICATION_NUM);

    //get block info for the last block
    LocatedBlock locatedblock = TestInterDatanodeProtocol.getLastLocatedBlock(
        dfs.dfs.getNamenode(), filestr);
    DatanodeInfo[] datanodeinfos = locatedblock.getLocations();
    assertEquals(REPLICATION_NUM, datanodeinfos.length);

    //connect to data nodes
    DataNode[] datanodes = new DataNode[REPLICATION_NUM];
    for(int i = 0; i < REPLICATION_NUM; i++) {
      datanodes[i] = cluster.getDataNode(datanodeinfos[i].getIpcPort());
      assertTrue(datanodes[i] != null);
    }

    //verify Block Info
    ExtendedBlock lastblock = locatedblock.getBlock();
    DataNode.LOG.info("newblocks=" + lastblock);
    for(int i = 0; i < REPLICATION_NUM; i++) {
      checkMetaInfo(lastblock, datanodes[i]);
    }

    DataNode.LOG.info("dfs.dfs.clientName=" + dfs.dfs.clientName);
    cluster.getNameNodeRpc().append(filestr, dfs.dfs.clientName,
        new EnumSetWritable<>(EnumSet.of(CreateFlag.APPEND)));

    // expire lease to trigger block recovery.
    waitLeaseRecovery(cluster);

    Block[] updatedmetainfo = new Block[REPLICATION_NUM];
    long oldSize = lastblock.getNumBytes();
    lastblock = TestInterDatanodeProtocol.getLastLocatedBlock(
        dfs.dfs.getNamenode(), filestr).getBlock();
    long currentGS = lastblock.getGenerationStamp();
    for(int i = 0; i < REPLICATION_NUM; i++) {
      updatedmetainfo[i] = DataNodeTestUtils.getFSDataset(datanodes[i]).getStoredBlock(
          lastblock.getBlockPoolId(), lastblock.getBlockId());
      assertEquals(lastblock.getBlockId(), updatedmetainfo[i].getBlockId());
      assertEquals(oldSize, updatedmetainfo[i].getNumBytes());
      assertEquals(currentGS, updatedmetainfo[i].getGenerationStamp());
    }

    // verify that lease recovery does not occur when namenode is in safemode
    System.out.println("Testing that lease recovery cannot happen during safemode.");
    filestr = "/foo.safemode";
    filepath = new Path(filestr);
    dfs.create(filepath, (short)1);
    cluster.getNameNodeRpc().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_ENTER, false);
    assertTrue(dfs.dfs.exists(filestr));
    DFSTestUtil.waitReplication(dfs, filepath, (short)1);
    waitLeaseRecovery(cluster);
    // verify that we still cannot recover the lease
    LeaseManager lm = NameNodeAdapter.getLeaseManager(cluster.getNamesystem());
    assertTrue("Found " + lm.countLease() + " lease, expected 1", lm.countLease() == 1);
    cluster.getNameNodeRpc().setSafeMode(
        HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);
  }

  /**
   * Block Recovery when the meta file not having crcs for all chunks in block
   * file
   */
  @Test
  public void testBlockRecoveryWithLessMetafile() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_BLOCK_LOCAL_PATH_ACCESS_USER_KEY,
        UserGroupInformation.getCurrentUser().getShortUserName());
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    Path file = new Path("/testRecoveryFile");
    DistributedFileSystem dfs = cluster.getFileSystem();
    FSDataOutputStream out = dfs.create(file);
    final int FILE_SIZE = 2 * 1024 * 1024;
    int count = 0;
    while (count < FILE_SIZE) {
      out.writeBytes("Data");
      count += 4;
    }
    out.hsync();
    // abort the original stream
    ((DFSOutputStream) out.getWrappedStream()).abort();

    LocatedBlocks locations = cluster.getNameNodeRpc().getBlockLocations(
        file.toString(), 0, count);
    ExtendedBlock block = locations.get(0).getBlock();

    // Calculate meta file size
    // From DataNode.java, checksum size is given by:
    // (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
    // CHECKSUM_SIZE
    final int CHECKSUM_SIZE = 4; // CRC32 & CRC32C
    final int bytesPerChecksum = conf.getInt(
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    final int metaFileSize =
        (FILE_SIZE + bytesPerChecksum - 1) / bytesPerChecksum * CHECKSUM_SIZE +
        8; // meta file header is 8 bytes
    final int newMetaFileSize = metaFileSize - CHECKSUM_SIZE;

    // Corrupt the block meta file by dropping checksum for bytesPerChecksum
    // bytes. Lease recovery is expected to recover the uncorrupted file length.
    cluster.truncateMeta(0, block, newMetaFileSize);

    // restart DN to make replica to RWR
    DataNodeProperties dnProp = cluster.stopDataNode(0);
    cluster.restartDataNode(dnProp, true);

    // try to recover the lease
    DistributedFileSystem newdfs = (DistributedFileSystem) FileSystem
        .newInstance(cluster.getConfiguration(0));
    count = 0;
    while (++count < 10 && !newdfs.recoverLease(file)) {
      Thread.sleep(1000);
    }
    assertTrue("File should be closed", newdfs.recoverLease(file));

    // Verify file length after lease recovery. The new file length should not
    // include the bytes with corrupted checksum.
    final long expectedNewFileLen = FILE_SIZE - bytesPerChecksum;
    final long newFileLen = newdfs.getFileStatus(file).getLen();
    assertEquals(newFileLen, expectedNewFileLen);
  }

  /**
   * Block/lease recovery should be retried with failed nodes from the second
   * stage removed to avoid perpetual recovery failures.
   */
  @Test
  public void testBlockRecoveryRetryAfterFailedRecovery() throws Exception {
    Configuration conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    Path file = new Path("/testBlockRecoveryRetryAfterFailedRecovery");
    DistributedFileSystem dfs = cluster.getFileSystem();

    // Create a file.
    FSDataOutputStream out = dfs.create(file);
    final int FILE_SIZE = 128 * 1024;
    int count = 0;
    while (count < FILE_SIZE) {
      out.writeBytes("DE K9SUL");
      count += 8;
    }
    out.hsync();

    // Abort the original stream.
    ((DFSOutputStream) out.getWrappedStream()).abort();

    LocatedBlocks locations = cluster.getNameNodeRpc().getBlockLocations(
        file.toString(), 0, count);
    ExtendedBlock block = locations.get(0).getBlock();

    // Finalize one replica to simulate a partial close failure.
    cluster.getDataNodes().get(0).getFSDataset().finalizeBlock(block, false);
    // Delete the meta file to simulate a rename/move failure.
    cluster.deleteMeta(0, block);

    // Try to recover the lease.
    DistributedFileSystem newDfs = (DistributedFileSystem) FileSystem
        .newInstance(cluster.getConfiguration(0));
    count = 0;
    while (count++ < 15 && !newDfs.recoverLease(file)) {
      Thread.sleep(1000);
    }
    // The lease should have been recovered.
    assertTrue("File should be closed", newDfs.recoverLease(file));
  }

  /**
   * Recover the lease on a file and append file from another client.
   */
  @Test
  public void testLeaseRecoveryAndAppend() throws Exception {
    testLeaseRecoveryAndAppend(new Configuration());
  }

  /**
   * Recover the lease on a file and append file from another client with
   * ViewDFS enabled.
   */
  @Test
  public void testLeaseRecoveryAndAppendWithViewDFS() throws Exception {
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl", ViewDistributedFileSystem.class.getName());
    testLeaseRecoveryAndAppend(conf);
  }

  private void testLeaseRecoveryAndAppend(Configuration conf) throws Exception {
    try {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    Path file = new Path("/testLeaseRecovery");
    DistributedFileSystem dfs = cluster.getFileSystem();

    // create a file with 0 bytes
    FSDataOutputStream out = dfs.create(file);
    out.hflush();
    out.hsync();

    // abort the original stream
    ((DFSOutputStream) out.getWrappedStream()).abort();
    DistributedFileSystem newdfs =
        (DistributedFileSystem) FileSystem.newInstance
        (cluster.getConfiguration(0));

    // Append to a file , whose lease is held by another client should fail
    try {
        newdfs.append(file);
        fail("Append to a file(lease is held by another client) should fail");
    } catch (RemoteException e) {
      assertTrue(e.getMessage().contains("file lease is currently owned"));
    }

    // Lease recovery on first try should be successful
    boolean recoverLease = newdfs.recoverLease(file);
    assertTrue(recoverLease);
    FSDataOutputStream append = newdfs.append(file);
    append.write("test".getBytes());
    append.close();
    }finally{
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
    }
  }

  /**
   *  HDFS-14498 - test lease can be recovered for a file where the final
   *  block was never registered with the DNs, and hence the IBRs will never
   *  be received. In this case the final block should be zero bytes and can
   *  be removed.
   */
  @Test
  public void testLeaseRecoveryEmptyCommittedLastBlock() throws Exception {
    Configuration conf = new Configuration();
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      DistributedFileSystem dfs = cluster.getFileSystem();
      client =
          new DFSClient(cluster.getNameNode().getServiceRpcAddress(), conf);
      String file = "/test/f1";
      Path filePath = new Path(file);

      createCommittedNotCompleteFile(client, file, null, 1);

      INodeFile inode = cluster.getNamesystem().getFSDirectory()
          .getINode(filePath.toString()).asFile();
      assertTrue(inode.isUnderConstruction());
      assertEquals(1, inode.numBlocks());
      assertNotNull(inode.getLastBlock());

      // Ensure a different client cannot append the file
      try {
        dfs.append(filePath);
        fail("Append to a file(lease is held by another client) should fail");
      } catch (RemoteException e) {
        assertTrue(e.getMessage().contains("file lease is currently owned"));
      }

      // Lease will not be recovered on the first try
      assertEquals(false, client.recoverLease(file));
      for (int i=0; i < 10 && !client.recoverLease(file); i++) {
        Thread.sleep(1000);
      }
      assertTrue(client.recoverLease(file));

      inode = cluster.getNamesystem().getFSDirectory()
          .getINode(filePath.toString()).asFile();
      assertTrue(!inode.isUnderConstruction());
      assertEquals(0, inode.numBlocks());
      assertNull(inode.getLastBlock());

      // Ensure the recovered file can now be written
      FSDataOutputStream append = dfs.append(filePath);
      append.write("test".getBytes());
      append.close();
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
      if (client != null) {
        client.close();
      }
    }
  }

  /**
   *  HDFS-14498 - similar to testLeaseRecoveryEmptyCommittedLastBlock except
   *  we wait for the lease manager to recover the lease automatically.
   */
  @Test
  public void testLeaseManagerRecoversEmptyCommittedLastBlock()
      throws Exception {
    Configuration conf = new Configuration();
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      client =
          new DFSClient(cluster.getNameNode().getServiceRpcAddress(), conf);
      String file = "/test/f1";

      createCommittedNotCompleteFile(client, file, null, 1);
      waitLeaseRecovery(cluster);

      GenericTestUtils.waitFor(() -> {
        String holder = NameNodeAdapter
            .getLeaseHolderForPath(cluster.getNameNode(), file);
        return holder == null;
      }, 100, 10000);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testAbortedRecovery() throws Exception {
    Configuration conf = new Configuration();
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      client =
          new DFSClient(cluster.getNameNode().getServiceRpcAddress(), conf);
      final String file = "/test/f1";

      HdfsFileStatus stat = client.getNamenode()
          .create(file, new FsPermission("777"), client.clientName,
              new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
              true, (short) 1, 1024 * 1024 * 128L,
              new CryptoProtocolVersion[0], null, null);

      assertNotNull(NameNodeAdapter.getLeaseHolderForPath(
          cluster.getNameNode(), file));

      // Add a block to the file
      ExtendedBlock block = client.getNamenode().addBlock(
          file, client.clientName, null, new DatanodeInfo[0], stat.getFileId(),
          new String[0], null).getBlock();

      // update the pipeline to get a new genstamp.
      ExtendedBlock updatedBlock = client.getNamenode()
          .updateBlockForPipeline(block, client.clientName)
          .getBlock();
      // fake that some data was maybe written.  commit block sync will
      // reconcile.
      updatedBlock.setNumBytes(1234);

      // get the stored block and make it look like the DN sent a RBW IBR.
      BlockManager bm = cluster.getNamesystem().getBlockManager();
      BlockInfo storedBlock = bm.getStoredBlock(block.getLocalBlock());
      BlockUnderConstructionFeature uc =
          storedBlock.getUnderConstructionFeature();
      uc.setExpectedLocations(updatedBlock.getLocalBlock(),
          uc.getExpectedStorageLocations(), BlockType.CONTIGUOUS);

      // complete the file w/o updatePipeline to simulate client failure.
      client.getNamenode().complete(file, client.clientName, block,
          stat.getFileId());

      assertNotNull(NameNodeAdapter.getLeaseHolderForPath(
          cluster.getNameNode(), file));

      cluster.setLeasePeriod(LEASE_PERIOD, LEASE_PERIOD);
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          String holder = NameNodeAdapter
              .getLeaseHolderForPath(cluster.getNameNode(), file);
          return holder == null;
        }
      }, 100, 20000);
      // nothing was actually written so the block should be dropped.
      assertTrue(storedBlock.isDeleted());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
      if (client != null) {
        client.close();
      }
    }
  }

  @Test
  public void testLeaseManagerRecoversCommittedLastBlockWithContent()
      throws Exception {
    Configuration conf = new Configuration();
    DFSClient client = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
      client =
          new DFSClient(cluster.getNameNode().getServiceRpcAddress(), conf);
      String file = "/test/f2";

      byte[] bytesToWrite = new byte[1];
      bytesToWrite[0] = 123;
      createCommittedNotCompleteFile(client, file, bytesToWrite, 3);

      waitLeaseRecovery(cluster);

      DistributedFileSystem hdfs = cluster.getFileSystem();

      // Now the least has been recovered, attempt to append the file and then
      // ensure the earlier written and newly written data can be read back.
      FSDataOutputStream op = null;
      try {
        op = hdfs.append(new Path(file));
        op.write(23);
      } finally {
        if (op != null) {
          op.close();
        }
      }

      FSDataInputStream stream = null;
      try {
        stream = cluster.getFileSystem().open(new Path(file));
        assertEquals(123, stream.readByte());
        assertEquals(23, stream.readByte());
      } finally {
        stream.close();
      }

      // Finally check there are no leases for the file and hence the file is
      // closed.
      GenericTestUtils.waitFor(() -> {
        String holder = NameNodeAdapter
            .getLeaseHolderForPath(cluster.getNameNode(), file);
        return holder == null;
      }, 100, 10000);

    } finally {
      if (cluster != null) {
        cluster.shutdown();
        cluster = null;
      }
      if (client != null) {
        client.close();
      }
    }
  }

  private void createCommittedNotCompleteFile(DFSClient client, String file,
      byte[] bytesToWrite, int repFactor)  throws IOException {
    HdfsFileStatus stat = client.getNamenode()
        .create(file, new FsPermission("777"), client.clientName,
            new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
            true, (short) repFactor, 1024 * 1024 * 128L,
            new CryptoProtocolVersion[0], null, null);
    // Add a block to the file
    LocatedBlock blk = client.getNamenode()
        .addBlock(file, client.clientName, null,
            new DatanodeInfo[0], stat.getFileId(), new String[0], null);
    ExtendedBlock finalBlock = blk.getBlock();
    if (bytesToWrite != null) {
      // Here we create a output stream and then abort it so the block gets
      // created on the datanode, but we never send the message to tell the DN
      // to complete the block. This simulates the client crashing after it
      // wrote the data, but before the file gets closed.
      DFSOutputStream s = new DFSOutputStream(client, file, stat,
          EnumSet.of(CreateFlag.CREATE), null,
          DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512),
          null, true);
      s.start();
      s.write(bytesToWrite);
      s.hflush();
      finalBlock = s.getBlock();
      s.abort();
    }
    // Attempt to close the file. This will fail (return false) as the NN will
    // be expecting the registered block to be reported from the DNs via IBR,
    // but that will never happen, as we either did not write it, or we aborted
    // the stream preventing the "close block" message to be sent to the DN.
    boolean closed = client.getNamenode().complete(
        file, client.clientName, finalBlock, stat.getFileId());
    assertEquals(false, closed);
  }

}
