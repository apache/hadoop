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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.impl.CorruptFileBlockIterator;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;

/**
 * This class tests the listCorruptFileBlocks API.
 * We create 3 files; intentionally delete their blocks
 * Use listCorruptFileBlocks to validate that we get the list of corrupt
 * files/blocks; also test the "paging" support by calling the API
 * with a block # from a previous call and validate that the subsequent
 * blocks/files are also returned.
 */
public class TestListCorruptFileBlocks {
  static final Logger LOG = NameNode.stateChangeLog;

  /** check if nn.getCorruptFiles() returns a file that has corrupted blocks */
  @Test (timeout=300000)
  public void testListCorruptFilesCorruptedBlock() throws Exception {
    MiniDFSCluster cluster = null;
    
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1); // datanode scans directories
      conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3 * 1000); // datanode sends block reports
      // Set short retry timeouts so this test runs faster
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();

      // Files are corrupted with 2 bytes before the end of the file,
      // so that's the minimum length.
      final int corruptionLength = 2;
      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("testCorruptFilesCorruptedBlock").setNumFiles(2).
          setMaxLevels(1).setMinSize(corruptionLength).setMaxSize(512).build();
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.
        getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Namenode has " + badFiles.size()
          + " corrupt files. Expecting None.", badFiles.size() == 0);

      // Now deliberately corrupt one block
      String bpid = cluster.getNamesystem().getBlockPoolId();
      File storageDir = cluster.getInstanceStorageDir(0, 1);
      File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
      assertTrue("data directory does not exist", data_dir.exists());
      List<File> metaFiles = MiniDFSCluster.getAllBlockFiles(data_dir);
      assertTrue("Data directory does not contain any blocks or there was an "
          + "IO error", metaFiles != null && !metaFiles.isEmpty());
      File metaFile = metaFiles.get(0);
      RandomAccessFile file = new RandomAccessFile(metaFile, "rw");
      FileChannel channel = file.getChannel();
      long position = channel.size() - corruptionLength;
      byte[] buffer = new byte[corruptionLength];
      new Random(13L).nextBytes(buffer);
      channel.write(ByteBuffer.wrap(buffer), position);
      file.close();
      LOG.info("Deliberately corrupting file " + metaFile.getName() +
          " at offset " + position + " length " + corruptionLength);

      // read all files to trigger detection of corrupted replica
      try {
        util.checkFiles(fs, "/srcdat10");
      } catch (BlockMissingException e) {
        System.out.println("Received BlockMissingException as expected.");
      } catch (IOException e) {
        assertTrue("Corrupted replicas not handled properly. Expecting BlockMissingException " +
            " but received IOException " + e, false);
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = namenode.getNamesystem().listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);
      util.cleanup(fs, "/srcdat10");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /**
   * Check that listCorruptFileBlocks works while the namenode is still in safemode.
   */
  @Test (timeout=300000)
  public void testListCorruptFileBlocksInSafeMode() throws Exception {
    MiniDFSCluster cluster = null;

    try {
      Configuration conf = new HdfsConfiguration();
      // datanode scans directories
      conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);
      // datanode sends block reports
      conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3 * 1000);
      // never leave safemode automatically
      conf.setFloat(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_THRESHOLD_PCT_KEY,
                    1.5f);
      // start populating repl queues immediately 
      conf.setFloat(DFSConfigKeys.DFS_NAMENODE_REPL_QUEUE_THRESHOLD_PCT_KEY,
                    0f);
      // Set short retry timeouts so this test runs faster
      conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
      cluster = new MiniDFSCluster.Builder(conf).waitSafeMode(false).build();
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);
      FileSystem fs = cluster.getFileSystem();

      // Files are corrupted with 2 bytes before the end of the file,
      // so that's the minimum length.
      final int corruptionLength = 2;
      // create two files with one block each
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("testListCorruptFileBlocksInSafeMode").setNumFiles(2).
          setMaxLevels(1).setMinSize(corruptionLength).setMaxSize(512).build();
      util.createFiles(fs, "/srcdat10");

      // fetch bad file list from namenode. There should be none.
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = 
        cluster.getNameNode().getNamesystem().listCorruptFileBlocks("/", null);
      assertTrue("Namenode has " + badFiles.size()
          + " corrupt files. Expecting None.", badFiles.size() == 0);

      // Now deliberately corrupt one block
      File storageDir = cluster.getInstanceStorageDir(0, 0);
      File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, 
          cluster.getNamesystem().getBlockPoolId());
      assertTrue("data directory does not exist", data_dir.exists());
      List<File> metaFiles = MiniDFSCluster.getAllBlockFiles(data_dir);
      assertTrue("Data directory does not contain any blocks or there was an "
          + "IO error", metaFiles != null && !metaFiles.isEmpty());
      File metaFile = metaFiles.get(0);
      RandomAccessFile file = new RandomAccessFile(metaFile, "rw");
      FileChannel channel = file.getChannel();
      long position = channel.size() - corruptionLength;
      byte[] buffer = new byte[corruptionLength];
      new Random(13L).nextBytes(buffer);
      channel.write(ByteBuffer.wrap(buffer), position);
      file.close();
      LOG.info("Deliberately corrupting file " + metaFile.getName() +
          " at offset " + position + " length " + corruptionLength);

      // read all files to trigger detection of corrupted replica
      try {
        util.checkFiles(fs, "/srcdat10");
      } catch (BlockMissingException e) {
        System.out.println("Received BlockMissingException as expected.");
      } catch (IOException e) {
        assertTrue("Corrupted replicas not handled properly. " +
                   "Expecting BlockMissingException " +
                   " but received IOException " + e, false);
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = cluster.getNameNode().getNamesystem().
        listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);
 
      // restart namenode
      cluster.restartNameNode(0);
      fs = cluster.getFileSystem();

      // wait until replication queues have been initialized
      while (!cluster.getNameNode().namesystem.getBlockManager()
          .isPopulatingReplQueues()) {
        try {
          LOG.info("waiting for replication queues");
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
        }
      }

      // read all files to trigger detection of corrupted replica
      try {
        util.checkFiles(fs, "/srcdat10");
      } catch (BlockMissingException e) {
        System.out.println("Received BlockMissingException as expected.");
      } catch (IOException e) {
        assertTrue("Corrupted replicas not handled properly. " +
                   "Expecting BlockMissingException " +
                   " but received IOException " + e, false);
      }

      // fetch bad file list from namenode. There should be one file.
      badFiles = cluster.getNameNode().getNamesystem().
        listCorruptFileBlocks("/", null);
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting 1.",
          badFiles.size() == 1);

      // check that we are still in safe mode
      assertTrue("Namenode is not in safe mode", 
                 cluster.getNameNode().isInSafeMode());

      // now leave safe mode so that we can clean up
      cluster.getNameNodeRpc().setSafeMode(
          HdfsConstants.SafeModeAction.SAFEMODE_LEAVE, false);

      util.cleanup(fs, "/srcdat10");
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw e;
    } finally {
      if (cluster != null) {
        cluster.shutdown(); 
      }
    }
  }
  
  // deliberately remove blocks from a file and validate the list-corrupt-file-blocks API
  @Test (timeout=300000)
  public void testlistCorruptFileBlocks() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1); // datanode scans
                                                           // directories
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("testGetCorruptFiles").setNumFiles(3).setMaxLevels(1).
          setMaxSize(1024).build();
      util.createFiles(fs, "/corruptData");

      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks = 
        namenode.getNamesystem().listCorruptFileBlocks("/corruptData", null);
      int numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      // delete the blocks
      String bpid = cluster.getNamesystem().getBlockPoolId();
      for (int i = 0; i < 4; i++) {
        for (int j = 0; j <= 1; j++) {
          File storageDir = cluster.getInstanceStorageDir(i, j);
          File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
          List<File> metadataFiles = MiniDFSCluster.getAllBlockMetadataFiles(
              data_dir);
          if (metadataFiles == null)
            continue;
          // assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
          // (blocks.length > 0));
          for (File metadataFile : metadataFiles) {
            File blockFile = Block.metaToBlockFile(metadataFile);
            LOG.info("Deliberately removing file " + blockFile.getName());
            assertTrue("Cannot remove file.", blockFile.delete());
            LOG.info("Deliberately removing file " + metadataFile.getName());
            assertTrue("Cannot remove file.", metadataFile.delete());
            // break;
          }
        }
      }

      int count = 0;
      corruptFileBlocks = namenode.getNamesystem().
        listCorruptFileBlocks("/corruptData", null);
      numCorrupt = corruptFileBlocks.size();
      while (numCorrupt < 3) {
        Thread.sleep(1000);
        corruptFileBlocks = namenode.getNamesystem()
            .listCorruptFileBlocks("/corruptData", null);
        numCorrupt = corruptFileBlocks.size();
        count++;
        if (count > 30)
          break;
      }
      // Validate we get all the corrupt files
      LOG.info("Namenode has bad files. " + numCorrupt);
      assertTrue(numCorrupt == 3);
      // test the paging here

      FSNamesystem.CorruptFileBlockInfo[] cfb = corruptFileBlocks
          .toArray(new FSNamesystem.CorruptFileBlockInfo[0]);
      // now get the 2nd and 3rd file that is corrupt
      String[] cookie = new String[]{"1"};
      Collection<FSNamesystem.CorruptFileBlockInfo> nextCorruptFileBlocks =
        namenode.getNamesystem()
          .listCorruptFileBlocks("/corruptData", cookie);
      FSNamesystem.CorruptFileBlockInfo[] ncfb = nextCorruptFileBlocks
          .toArray(new FSNamesystem.CorruptFileBlockInfo[0]);
      numCorrupt = nextCorruptFileBlocks.size();
      assertTrue(numCorrupt == 2);
      assertTrue(ncfb[0].block.getBlockName()
          .equalsIgnoreCase(cfb[1].block.getBlockName()));

      corruptFileBlocks =
        namenode.getNamesystem()
          .listCorruptFileBlocks("/corruptData", cookie);
      numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      // Do a listing on a dir which doesn't have any corrupt blocks and
      // validate
      util.createFiles(fs, "/goodData");
      corruptFileBlocks = 
        namenode.getNamesystem().listCorruptFileBlocks("/goodData", null);
      numCorrupt = corruptFileBlocks.size();
      assertTrue(numCorrupt == 0);
      util.cleanup(fs, "/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private int countPaths(RemoteIterator<Path> iter) throws IOException {
    int i = 0;
    while (iter.hasNext()) {
      LOG.info("PATH: " + iter.next().toUri().getPath());
      i++;
    }
    return i;
  }

  /**
   * test listCorruptFileBlocks in DistributedFileSystem
   */
  @Test (timeout=300000)
  public void testlistCorruptFileBlocksDFS() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1); // datanode scans
                                                           // directories
    FileSystem fs = null;

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      DFSTestUtil util = new DFSTestUtil.Builder().
          setName("testGetCorruptFiles").setNumFiles(3).
          setMaxLevels(1).setMaxSize(1024).build();
      util.createFiles(fs, "/corruptData");

      RemoteIterator<Path> corruptFileBlocks = 
        dfs.listCorruptFileBlocks(new Path("/corruptData"));
      int numCorrupt = countPaths(corruptFileBlocks);
      assertTrue(numCorrupt == 0);
      // delete the blocks
      String bpid = cluster.getNamesystem().getBlockPoolId();
      // For loop through number of datadirectories per datanode (2)
      for (int i = 0; i < 2; i++) {
        File storageDir = cluster.getInstanceStorageDir(0, i);
        File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
        List<File> metadataFiles = MiniDFSCluster.getAllBlockMetadataFiles(
            data_dir);
        if (metadataFiles == null)
          continue;
        // assertTrue("Blocks do not exist in data-dir", (blocks != null) &&
        // (blocks.length > 0));
        for (File metadataFile : metadataFiles) {
          File blockFile = Block.metaToBlockFile(metadataFile);
          LOG.info("Deliberately removing file " + blockFile.getName());
          assertTrue("Cannot remove file.", blockFile.delete());
          LOG.info("Deliberately removing file " + metadataFile.getName());
          assertTrue("Cannot remove file.", metadataFile.delete());
          // break;
        }
      }

      int count = 0;
      corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("/corruptData"));
      numCorrupt = countPaths(corruptFileBlocks);
      while (numCorrupt < 3) {
        Thread.sleep(1000);
        corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("/corruptData"));
        numCorrupt = countPaths(corruptFileBlocks);
        count++;
        if (count > 30)
          break;
      }
      // Validate we get all the corrupt files
      LOG.info("Namenode has bad files. " + numCorrupt);
      assertTrue(numCorrupt == 3);

      util.cleanup(fs, "/corruptData");
      util.cleanup(fs, "/goodData");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
    
  /**
   * Test if NN.listCorruptFiles() returns the right number of results.
   * The corrupt blocks are detected by the BlockPoolSliceScanner.
   * Also, test that DFS.listCorruptFileBlocks can make multiple successive
   * calls.
   */
  @Test (timeout=300000)
  public void testMaxCorruptFiles() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new HdfsConfiguration();
      conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 3 * 1000); // datanode sends block reports
      cluster = new MiniDFSCluster.Builder(conf).build();
      FileSystem fs = cluster.getFileSystem();
      final int maxCorruptFileBlocks = 
        FSNamesystem.DEFAULT_MAX_CORRUPT_FILEBLOCKS_RETURNED;

      // create 110 files with one block each
      DFSTestUtil util = new DFSTestUtil.Builder().setName("testMaxCorruptFiles").
          setNumFiles(maxCorruptFileBlocks * 3).setMaxLevels(1).setMaxSize(512).
          build();
      util.createFiles(fs, "/srcdat2", (short) 1);
      util.waitReplication(fs, "/srcdat2", (short) 1);

      // verify that there are no bad blocks.
      final NameNode namenode = cluster.getNameNode();
      Collection<FSNamesystem.CorruptFileBlockInfo> badFiles = namenode.
        getNamesystem().listCorruptFileBlocks("/srcdat2", null);
      assertTrue("Namenode has " + badFiles.size() + " corrupt files. Expecting none.",
          badFiles.size() == 0);

      // Now deliberately blocks from all files
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      for (int i=0; i<4; i++) {
        for (int j=0; j<=1; j++) {
          File storageDir = cluster.getInstanceStorageDir(i, j);
          File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
          LOG.info("Removing files from " + data_dir);
          List<File> metadataFiles = MiniDFSCluster.getAllBlockMetadataFiles(
              data_dir);
          if (metadataFiles == null)
            continue;
          for (File metadataFile : metadataFiles) {
            File blockFile = Block.metaToBlockFile(metadataFile);
            assertTrue("Cannot remove file.", blockFile.delete());
            assertTrue("Cannot remove file.", metadataFile.delete());
          }
        }
      }

      // Run the direcrtoryScanner to update the Datanodes volumeMap
      DataNode dn = cluster.getDataNodes().get(0);
      DataNodeTestUtils.runDirectoryScanner(dn);

      // Occasionally the BlockPoolSliceScanner can run before we have removed
      // the blocks. Restart the Datanode to trigger the scanner into running
      // once more.
      LOG.info("Restarting Datanode to trigger BlockPoolSliceScanner");
      cluster.restartDataNodes();
      cluster.waitActive();

      badFiles = 
        namenode.getNamesystem().listCorruptFileBlocks("/srcdat2", null);
        
       while (badFiles.size() < maxCorruptFileBlocks) {
        LOG.info("# of corrupt files is: " + badFiles.size());
        Thread.sleep(10000);
        badFiles = namenode.getNamesystem().
          listCorruptFileBlocks("/srcdat2", null);
      }
      badFiles = namenode.getNamesystem().
        listCorruptFileBlocks("/srcdat2", null); 
      LOG.info("Namenode has bad files. " + badFiles.size());
      assertTrue("Namenode has " + badFiles.size() + " bad files. Expecting " + 
          maxCorruptFileBlocks + ".",
          badFiles.size() == maxCorruptFileBlocks);

      CorruptFileBlockIterator iter = (CorruptFileBlockIterator)
        fs.listCorruptFileBlocks(new Path("/srcdat2"));
      int corruptPaths = countPaths(iter);
      assertTrue("Expected more than " + maxCorruptFileBlocks +
                 " corrupt file blocks but got " + corruptPaths,
                 corruptPaths > maxCorruptFileBlocks);
      assertTrue("Iterator should have made more than 1 call but made " +
                 iter.getCallsMade(),
                 iter.getCallsMade() > 1);

      util.cleanup(fs, "/srcdat2");
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  @Test(timeout = 60000)
  public void testListCorruptFileBlocksOnRelativePath() throws Exception {
    Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      final Path baseDir = new Path("/somewhere/base");
      fs.mkdirs(baseDir);
      // set working dir
      fs.setWorkingDirectory(baseDir);

      DFSTestUtil util = new DFSTestUtil.Builder()
          .setName("testGetCorruptFilesOnRelativePath").setNumFiles(3)
          .setMaxLevels(1).setMaxSize(1024).build();
      util.createFiles(fs, "corruptData");

      RemoteIterator<Path> corruptFileBlocks = dfs
          .listCorruptFileBlocks(new Path("corruptData"));
      int numCorrupt = countPaths(corruptFileBlocks);
      assertTrue(numCorrupt == 0);

      // delete the blocks
      String bpid = cluster.getNamesystem().getBlockPoolId();
      // For loop through number of data directories per datanode (2)
      for (int i = 0; i < 2; i++) {
        File storageDir = cluster.getInstanceStorageDir(0, i);
        File data_dir = MiniDFSCluster.getFinalizedDir(storageDir, bpid);
        List<File> metadataFiles = MiniDFSCluster
            .getAllBlockMetadataFiles(data_dir);
        if (metadataFiles == null)
          continue;
        for (File metadataFile : metadataFiles) {
          File blockFile = Block.metaToBlockFile(metadataFile);
          LOG.info("Deliberately removing file " + blockFile.getName());
          assertTrue("Cannot remove file.", blockFile.delete());
          LOG.info("Deliberately removing file " + metadataFile.getName());
          assertTrue("Cannot remove file.", metadataFile.delete());
        }
      }

      int count = 0;
      corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("corruptData"));
      numCorrupt = countPaths(corruptFileBlocks);
      while (numCorrupt < 3) {
        Thread.sleep(1000);
        corruptFileBlocks = dfs.listCorruptFileBlocks(new Path("corruptData"));
        numCorrupt = countPaths(corruptFileBlocks);
        count++;
        if (count > 30)
          break;
      }
      // Validate we get all the corrupt files
      LOG.info("Namenode has bad files. " + numCorrupt);
      assertTrue("Failed to get corrupt files!", numCorrupt == 3);

      util.cleanup(fs, "corruptData");
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
