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
package org.apache.hadoop.hdfs.server.datanode;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsTracer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetTestUtil;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.BlockReportContext;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.hdfs.server.protocol.StorageBlockReport;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Fine-grain testing of block files and locations after volume failure.
 */
public class TestDataNodeVolumeFailure {
  final private int block_size = 512;
  MiniDFSCluster cluster = null;
  private Configuration conf;
  final int dn_num = 2;
  final int blocks_num = 30;
  final short repl=2;
  File dataDir = null;
  File data_fail = null;
  File failedDir = null;
  private FileSystem fs;

  // mapping blocks to Meta files(physical files) and locs(NameNode locations)
  private class BlockLocs {
    public int num_files = 0;
    public int num_locs = 0;
  }
  // block id to BlockLocs
  final Map<String, BlockLocs> block_map = new HashMap<String, BlockLocs> ();

  @Before
  public void setUp() throws Exception {
    // bring up a cluster of 2
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, block_size);
    // Allow a single volume failure (there are two volumes)
    conf.setInt(DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dn_num).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dataDir = new File(cluster.getDataDirectory());
  }

  @After
  public void tearDown() throws Exception {
    if(data_fail != null) {
      FileUtil.setWritable(data_fail, true);
      data_fail = null;
    }
    if(failedDir != null) {
      FileUtil.setWritable(failedDir, true);
      failedDir = null;
    }
    if(cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }
  
  /*
   * Verify the number of blocks and files are correct after volume failure,
   * and that we can replicate to both datanodes even after a single volume
   * failure if the configuration parameter allows this.
   */
  @Test
  public void testVolumeFailure() throws Exception {
    System.out.println("Data dir: is " +  dataDir.getPath());
   
    
    // Data dir structure is dataDir/data[1-4]/[current,tmp...]
    // data1,2 is for datanode 1, data2,3 - datanode2 
    String filename = "/test.txt";
    Path filePath = new Path(filename);
    
    // we use only small number of blocks to avoid creating subdirs in the data dir..
    int filesize = block_size*blocks_num;
    DFSTestUtil.createFile(fs, filePath, filesize, repl, 1L);
    DFSTestUtil.waitReplication(fs, filePath, repl);
    System.out.println("file " + filename + "(size " +
        filesize + ") is created and replicated");
   
    // fail the volume
    // delete/make non-writable one of the directories (failed volume)
    data_fail = new File(dataDir, "data3");
    failedDir = MiniDFSCluster.getFinalizedDir(dataDir, 
        cluster.getNamesystem().getBlockPoolId());
    if (failedDir.exists() &&
        //!FileUtil.fullyDelete(failedDir)
        !deteteBlocks(failedDir)
        ) {
      throw new IOException("Could not delete hdfs directory '" + failedDir + "'");
    }
    data_fail.setReadOnly();
    failedDir.setReadOnly();
    System.out.println("Deleteing " + failedDir.getPath() + "; exist=" + failedDir.exists());
    
    // access all the blocks on the "failed" DataNode, 
    // we need to make sure that the "failed" volume is being accessed - 
    // and that will cause failure, blocks removal, "emergency" block report
    triggerFailure(filename, filesize);
    
    // make sure a block report is sent 
    DataNode dn = cluster.getDataNodes().get(1); //corresponds to dir data3
    String bpid = cluster.getNamesystem().getBlockPoolId();
    DatanodeRegistration dnR = dn.getDNRegistrationForBP(bpid);
    
    Map<DatanodeStorage, BlockListAsLongs> perVolumeBlockLists =
        dn.getFSDataset().getBlockReports(bpid);

    // Send block report
    StorageBlockReport[] reports =
        new StorageBlockReport[perVolumeBlockLists.size()];

    int reportIndex = 0;
    for(Map.Entry<DatanodeStorage, BlockListAsLongs> kvPair : perVolumeBlockLists.entrySet()) {
        DatanodeStorage dnStorage = kvPair.getKey();
        BlockListAsLongs blockList = kvPair.getValue();
        reports[reportIndex++] =
            new StorageBlockReport(dnStorage, blockList);
    }
    
    cluster.getNameNodeRpc().blockReport(dnR, bpid, reports,
        new BlockReportContext(1, 0, System.nanoTime(), 0, true));

    // verify number of blocks and files...
    verify(filename, filesize);
    
    // create another file (with one volume failed).
    System.out.println("creating file test1.txt");
    Path fileName1 = new Path("/test1.txt");
    DFSTestUtil.createFile(fs, fileName1, filesize, repl, 1L);
    
    // should be able to replicate to both nodes (2 DN, repl=2)
    DFSTestUtil.waitReplication(fs, fileName1, repl);
    System.out.println("file " + fileName1.getName() + 
        " is created and replicated");
  }

  /**
   * Test that DataStorage and BlockPoolSliceStorage remove the failed volume
   * after failure.
   */
  @Test(timeout=150000)
    public void testFailedVolumeBeingRemovedFromDataNode()
      throws InterruptedException, IOException, TimeoutException {
    // The test uses DataNodeTestUtils#injectDataDirFailure() to simulate
    // volume failures which is currently not supported on Windows.
    assumeTrue(!Path.WINDOWS);

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short) 2, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short) 2);

    File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    DataNode dn0 = cluster.getDataNodes().get(0);
    checkDiskErrorSync(dn0);

    // Verify dn0Vol1 has been completely removed from DN0.
    // 1. dn0Vol1 is removed from DataStorage.
    DataStorage storage = dn0.getStorage();
    assertEquals(1, storage.getNumStorageDirs());
    for (int i = 0; i < storage.getNumStorageDirs(); i++) {
      Storage.StorageDirectory sd = storage.getStorageDir(i);
      assertFalse(sd.getRoot().getAbsolutePath().startsWith(
          dn0Vol1.getAbsolutePath()
      ));
    }
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    BlockPoolSliceStorage bpsStorage = storage.getBPStorage(bpid);
    assertEquals(1, bpsStorage.getNumStorageDirs());
    for (int i = 0; i < bpsStorage.getNumStorageDirs(); i++) {
      Storage.StorageDirectory sd = bpsStorage.getStorageDir(i);
      assertFalse(sd.getRoot().getAbsolutePath().startsWith(
          dn0Vol1.getAbsolutePath()
      ));
    }

    // 2. dn0Vol1 is removed from FsDataset
    FsDatasetSpi<? extends FsVolumeSpi> data = dn0.getFSDataset();
    try (FsDatasetSpi.FsVolumeReferences vols = data.getFsVolumeReferences()) {
      for (FsVolumeSpi volume : vols) {
        assertNotEquals(new File(volume.getBasePath()).getAbsoluteFile(),
            dn0Vol1.getAbsoluteFile());
      }
    }

    // 3. all blocks on dn0Vol1 have been removed.
    for (ReplicaInfo replica : FsDatasetTestUtil.getReplicas(data, bpid)) {
      assertNotNull(replica.getVolume());
      assertNotEquals(
          new File(replica.getVolume().getBasePath()).getAbsoluteFile(),
          dn0Vol1.getAbsoluteFile());
    }

    // 4. dn0Vol1 is not in DN0's configuration and dataDirs anymore.
    String[] dataDirStrs =
        dn0.getConf().get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY).split(",");
    assertEquals(1, dataDirStrs.length);
    assertFalse(dataDirStrs[0].contains(dn0Vol1.getAbsolutePath()));
  }

  private static void checkDiskErrorSync(DataNode dn)
      throws InterruptedException {
    final long lastDiskErrorCheck = dn.getLastDiskErrorCheck();
    dn.checkDiskErrorAsync();
    // Wait 10 seconds for checkDiskError thread to finish and discover volume
    // failures.
    int count = 100;
    while (count > 0 && dn.getLastDiskErrorCheck() == lastDiskErrorCheck) {
      Thread.sleep(100);
      count--;
    }
    assertTrue("Disk checking thread does not finish in 10 seconds",
        count > 0);
  }

  /**
   * Test DataNode stops when the number of failed volumes exceeds
   * dfs.datanode.failed.volumes.tolerated .
   */
  @Test(timeout=10000)
  public void testDataNodeShutdownAfterNumFailedVolumeExceedsTolerated()
      throws InterruptedException, IOException {
    // make both data directories to fail on dn0
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1, dn0Vol2);
    DataNode dn0 = cluster.getDataNodes().get(0);
    checkDiskErrorSync(dn0);

    // DN0 should stop after the number of failure disks exceed tolerated
    // value (1).
    assertFalse(dn0.shouldRun());
  }

  /**
   * Test that DN does not shutdown, as long as failure volumes being hot swapped.
   */
  @Test
  public void testVolumeFailureRecoveredByHotSwappingVolume()
      throws InterruptedException, ReconfigurationException, IOException {
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    final DataNode dn0 = cluster.getDataNodes().get(0);
    final String oldDataDirs = dn0.getConf().get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    // Fail dn0Vol1 first.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    checkDiskErrorSync(dn0);

    // Hot swap out the failure volume.
    String dataDirs = dn0Vol2.getPath();
    assertThat(
        dn0.reconfigurePropertyImpl(
            DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dataDirs),
        is(dn0.getConf().get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY)));

    // Fix failure volume dn0Vol1 and remount it back.
    DataNodeTestUtils.restoreDataDirFromFailure(dn0Vol1);
    assertThat(
        dn0.reconfigurePropertyImpl(
            DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, oldDataDirs),
        is(dn0.getConf().get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY)));

    // Fail dn0Vol2. Now since dn0Vol1 has been fixed, DN0 has sufficient
    // resources, thus it should keep running.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol2);
    checkDiskErrorSync(dn0);
    assertTrue(dn0.shouldRun());
  }

  /**
   * Test changing the number of volumes does not impact the disk failure
   * tolerance.
   */
  @Test
  public void testTolerateVolumeFailuresAfterAddingMoreVolumes()
      throws InterruptedException, ReconfigurationException, IOException {
    final File dn0Vol1 = new File(dataDir, "data" + (2 * 0 + 1));
    final File dn0Vol2 = new File(dataDir, "data" + (2 * 0 + 2));
    final File dn0VolNew = new File(dataDir, "data_new");
    final DataNode dn0 = cluster.getDataNodes().get(0);
    final String oldDataDirs = dn0.getConf().get(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY);

    // Add a new volume to DN0
    assertThat(
        dn0.reconfigurePropertyImpl(
            DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
            oldDataDirs + "," + dn0VolNew.getAbsolutePath()),
        is(dn0.getConf().get(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY)));

    // Fail dn0Vol1 first and hot swap it.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol1);
    checkDiskErrorSync(dn0);
    assertTrue(dn0.shouldRun());

    // Fail dn0Vol2, now dn0 should stop, because we only tolerate 1 disk failure.
    DataNodeTestUtils.injectDataDirFailure(dn0Vol2);
    checkDiskErrorSync(dn0);
    assertFalse(dn0.shouldRun());
  }

  /**
   * Test that there are under replication blocks after vol failures
   */
  @Test
  public void testUnderReplicationAfterVolFailure() throws Exception {
    // The test uses DataNodeTestUtils#injectDataDirFailure() to simulate
    // volume failures which is currently not supported on Windows.
    assumeTrue(!Path.WINDOWS);

    // Bring up one more datanode
    cluster.startDataNodes(conf, 1, true, null, null);
    cluster.waitActive();

    final BlockManager bm = cluster.getNamesystem().getBlockManager();

    Path file1 = new Path("/test1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file1, (short)3);

    // Fail the first volume on both datanodes
    File dn1Vol1 = new File(dataDir, "data"+(2*0+1));
    File dn2Vol1 = new File(dataDir, "data"+(2*1+1));
    DataNodeTestUtils.injectDataDirFailure(dn1Vol1, dn2Vol1);

    Path file2 = new Path("/test2");
    DFSTestUtil.createFile(fs, file2, 1024, (short)3, 1L);
    DFSTestUtil.waitReplication(fs, file2, (short)3);

    // underReplicatedBlocks are due to failed volumes
    int underReplicatedBlocks =
        BlockManagerTestUtil.checkHeartbeatAndGetUnderReplicatedBlocksCount(
            cluster.getNamesystem(), bm);
    assertTrue("There is no under replicated block after volume failure",
        underReplicatedBlocks > 0);
  }

  /**
   * verifies two things:
   *  1. number of locations of each block in the name node
   *   matches number of actual files
   *  2. block files + pending block equals to total number of blocks that a file has 
   *     including the replication (HDFS file has 30 blocks, repl=2 - total 60
   * @param fn - file name
   * @param fs - file size
   * @throws IOException
   */
  private void verify(String fn, int fs) throws IOException{
    // now count how many physical blocks are there
    int totalReal = countRealBlocks(block_map);
    System.out.println("countRealBlocks counted " + totalReal + " blocks");

    // count how many blocks store in NN structures.
    int totalNN = countNNBlocks(block_map, fn, fs);
    System.out.println("countNNBlocks counted " + totalNN + " blocks");

    for(String bid : block_map.keySet()) {
      BlockLocs bl = block_map.get(bid);
      // System.out.println(bid + "->" + bl.num_files + "vs." + bl.num_locs);
      // number of physical files (1 or 2) should be same as number of datanodes
      // in the list of the block locations
      assertEquals("Num files should match num locations",
          bl.num_files, bl.num_locs);
    }
    assertEquals("Num physical blocks should match num stored in the NN",
        totalReal, totalNN);

    // now check the number of under-replicated blocks
    FSNamesystem fsn = cluster.getNamesystem();
    // force update of all the metric counts by calling computeDatanodeWork
    BlockManagerTestUtil.getComputedDatanodeWork(fsn.getBlockManager());
    // get all the counts 
    long underRepl = fsn.getUnderReplicatedBlocks();
    long pendRepl = fsn.getPendingReplicationBlocks();
    long totalRepl = underRepl + pendRepl;
    System.out.println("underreplicated after = "+ underRepl + 
        " and pending repl ="  + pendRepl + "; total underRepl = " + totalRepl);

    System.out.println("total blocks (real and replicating):" + 
        (totalReal + totalRepl) + " vs. all files blocks " + blocks_num*2);

    // together all the blocks should be equal to all real + all underreplicated
    assertEquals("Incorrect total block count",
        totalReal + totalRepl, blocks_num * repl);
  }
  
  /**
   * go to each block on the 2nd DataNode until it fails...
   * @param path
   * @param size
   * @throws IOException
   */
  private void triggerFailure(String path, long size) throws IOException {
    NamenodeProtocols nn = cluster.getNameNodeRpc();
    List<LocatedBlock> locatedBlocks =
      nn.getBlockLocations(path, 0, size).getLocatedBlocks();
    
    for (LocatedBlock lb : locatedBlocks) {
      DatanodeInfo dinfo = lb.getLocations()[1];
      ExtendedBlock b = lb.getBlock();
      try {
        accessBlock(dinfo, lb);
      } catch (IOException e) {
        System.out.println("Failure triggered, on block: " + b.getBlockId() +  
            "; corresponding volume should be removed by now");
        break;
      }
    }
  }
  
  /**
   * simulate failure delete all the block files
   * @param dir
   * @throws IOException
   */
  private boolean deteteBlocks(File dir) {
    File [] fileList = dir.listFiles();
    for(File f : fileList) {
      if(f.getName().startsWith(Block.BLOCK_FILE_PREFIX)) {
        if(!f.delete())
          return false;
        
      }
    }
    return true;
  }
  
  /**
   * try to access a block on a data node. If fails - throws exception
   * @param datanode
   * @param lblock
   * @throws IOException
   */
  private void accessBlock(DatanodeInfo datanode, LocatedBlock lblock)
    throws IOException {
    InetSocketAddress targetAddr = null;
    ExtendedBlock block = lblock.getBlock(); 
   
    targetAddr = NetUtils.createSocketAddr(datanode.getXferAddr());

    BlockReader blockReader = new BlockReaderFactory(new DfsClientConf(conf)).
      setInetSocketAddress(targetAddr).
      setBlock(block).
      setFileName(BlockReaderFactory.getFileName(targetAddr,
                    "test-blockpoolid", block.getBlockId())).
      setBlockToken(lblock.getBlockToken()).
      setStartOffset(0).
      setLength(0).
      setVerifyChecksum(true).
      setClientName("TestDataNodeVolumeFailure").
      setDatanodeInfo(datanode).
      setCachingStrategy(CachingStrategy.newDefaultStrategy()).
      setClientCacheContext(ClientContext.getFromConf(conf)).
      setConfiguration(conf).
      setTracer(FsTracer.get(conf)).
      setRemotePeerFactory(new RemotePeerFactory() {
        @Override
        public Peer newConnectedPeer(InetSocketAddress addr,
            Token<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
            throws IOException {
          Peer peer = null;
          Socket sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
          try {
            sock.connect(addr, HdfsConstants.READ_TIMEOUT);
            sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);
            peer = DFSUtilClient.peerFromSocket(sock);
          } finally {
            if (peer == null) {
              IOUtils.closeSocket(sock);
            }
          }
          return peer;
        }
      }).
      build();
    blockReader.close();
  }
  
  /**
   * Count datanodes that have copies of the blocks for a file
   * put it into the map
   * @param map
   * @param path
   * @param size
   * @return
   * @throws IOException
   */
  private int countNNBlocks(Map<String, BlockLocs> map, String path, long size) 
    throws IOException {
    int total = 0;
    
    NamenodeProtocols nn = cluster.getNameNodeRpc();
    List<LocatedBlock> locatedBlocks = 
      nn.getBlockLocations(path, 0, size).getLocatedBlocks();
    //System.out.println("Number of blocks: " + locatedBlocks.size()); 
        
    for(LocatedBlock lb : locatedBlocks) {
      String blockId = ""+lb.getBlock().getBlockId();
      //System.out.print(blockId + ": ");
      DatanodeInfo[] dn_locs = lb.getLocations();
      BlockLocs bl = map.get(blockId);
      if(bl == null) {
        bl = new BlockLocs();
      }
      //System.out.print(dn_info.name+",");
      total += dn_locs.length;        
      bl.num_locs += dn_locs.length;
      map.put(blockId, bl);
      //System.out.println();
    }
    return total;
  }
  
  /**
   *  look for real blocks
   *  by counting *.meta files in all the storage dirs 
   * @param map
   * @return
   */
  private int countRealBlocks(Map<String, BlockLocs> map) {
    int total = 0;
    final String bpid = cluster.getNamesystem().getBlockPoolId();
    for(int i=0; i<dn_num; i++) {
      for(int j=0; j<=1; j++) {
        File storageDir = cluster.getInstanceStorageDir(i, j);
        File dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
        if(dir == null) {
          System.out.println("dir is null for dn=" + i + " and data_dir=" + j);
          continue;
        }
      
        List<File> res = MiniDFSCluster.getAllBlockMetadataFiles(dir);
        if(res == null) {
          System.out.println("res is null for dir = " + dir + " i=" + i + " and j=" + j);
          continue;
        }
        //System.out.println("for dn" + i + "." + j + ": " + dir + "=" + res.length+ " files");
      
        //int ii = 0;
        for(File f: res) {
          String s = f.getName();
          // cut off "blk_-" at the beginning and ".meta" at the end
          assertNotNull("Block file name should not be null", s);
          String bid = s.substring(s.indexOf("_")+1, s.lastIndexOf("_"));
          //System.out.println(ii++ + ". block " + s + "; id=" + bid);
          BlockLocs val = map.get(bid);
          if(val == null) {
            val = new BlockLocs();
          }
          val.num_files ++; // one more file for the block
          map.put(bid, val);

        }
        //System.out.println("dir1="+dir.getPath() + "blocks=" + res.length);
        //System.out.println("dir2="+dir2.getPath() + "blocks=" + res2.length);

        total += res.size();
      }
    }
    return total;
  }
}
