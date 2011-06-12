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
package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;

import org.junit.Test;
import org.junit.After;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.TestRaidDfs;
import org.apache.hadoop.hdfs.RaidDFSUtil;
import org.apache.hadoop.raid.RaidNode;
import org.apache.hadoop.raid.HarIndex;


public class TestRaidShellFsck {
  final static Log LOG =
    LogFactory.getLog("org.apache.hadoop.raid.TestRaidShellFsck");
  final static String TEST_DIR = 
    new File(System.
             getProperty("test.build.data", "build/contrib/raid/test/data")).
    getAbsolutePath();
  final static String CONFIG_FILE = new File(TEST_DIR, "test-raid.xml").
    getAbsolutePath();
  final static long RELOAD_INTERVAL = 1000;
  final static int NUM_DATANODES = 4;
  final static int STRIPE_BLOCKS = 3; // number of blocks per stripe
  final static int FILE_BLOCKS = 6; // number of blocks that file consists of
  final static short REPL = 1; // replication factor before raiding
  final static long BLOCK_SIZE = 8192L; // size of block in byte
  final static String DIR_PATH = "/user/pkling/raidtest";
  final static Path FILE_PATH0 =
    new Path("/user/pkling/raidtest/raidfsck.test");
  final static Path FILE_PATH1 =
    new Path("/user/pkling/raidtest/raidfsck2.test");
  final static Path RAID_PATH = new Path("/destraid/user/pkling/raidtest");
  final static String HAR_NAME = "raidtest_raid.har";
  final static String RAID_DIR = "/destraid";

  Configuration conf = null;
  Configuration raidConf = null;
  Configuration clientConf = null;
  MiniDFSCluster cluster = null;
  DistributedFileSystem dfs = null;
  RaidNode rnode = null;
  
  
  RaidShell shell = null;
  String[] args = null;


  /**
   * creates a MiniDFS instance with a raided file in it
   */
  private void setUp(boolean doHar) throws IOException, ClassNotFoundException {

    final int timeBeforeHar;
    if (doHar) {
      timeBeforeHar = 0;
    } else {
      timeBeforeHar = -1;
    }


    new File(TEST_DIR).mkdirs(); // Make sure data directory exists
    conf = new Configuration();

    conf.set("raid.config.file", CONFIG_FILE);
    conf.setBoolean("raid.config.reload", true);
    conf.setLong("raid.config.reload.interval", RELOAD_INTERVAL);

    // scan all policies once every 5 second
    conf.setLong("raid.policy.rescan.interval", 5000);

    // make all deletions not go through Trash
    conf.set("fs.shell.delete.classname", "org.apache.hadoop.hdfs.DFSClient");

    // do not use map-reduce cluster for Raiding
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    // use local block fixer
    conf.set("raid.blockfix.classname", 
             "org.apache.hadoop.raid.LocalBlockFixer");

    conf.set("raid.server.address", "localhost:0");
    conf.setInt("hdfs.raid.stripeLength", STRIPE_BLOCKS);
    conf.set("hdfs.raid.locations", RAID_DIR);

    conf.setInt("dfs.corruptfilesreturned.max", 500);

    conf.setBoolean("dfs.permissions", false);

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    String namenode = dfs.getUri().toString();

    FileSystem.setDefaultUri(conf, namenode);

    FileWriter fileWriter = new FileWriter(CONFIG_FILE);
    fileWriter.write("<?xml version=\"1.0\"?>\n");
    String str =
      "<configuration> " +
      "  <srcPath prefix=\"" + DIR_PATH + "\"> " +
      "    <policy name = \"RaidTest1\"> " +
      "      <erasureCode>xor</erasureCode> " +
      "      <destPath> " + RAID_DIR + " </destPath> " +
      "      <property> " +
      "        <name>targetReplication</name> " +
      "        <value>1</value> " +
      "        <description>after RAIDing, decrease the replication " +
      "factor of a file to this value.</description> " +
      "      </property> " +
      "      <property> " +
      "        <name>metaReplication</name> " +
      "        <value>1</value> " +
      "        <description> replication factor of parity file</description> " +
      "      </property> " +
      "      <property> " +
      "        <name>modTimePeriod</name> " +
      "        <value>2000</value> " +
      "        <description>time (milliseconds) after a file is modified " + 
      "to make it a candidate for RAIDing</description> " +
      "      </property> ";
   
    if (timeBeforeHar >= 0) {
      str +=
        "      <property> " +
        "        <name>time_before_har</name> " +
        "        <value>" + timeBeforeHar + "</value> " +
        "        <description> amount of time waited before har'ing parity " +
        "files</description> " +
        "     </property> ";
    }

    str +=
      "    </policy>" +
      "  </srcPath>" +
      "</configuration>";

    fileWriter.write(str);
    fileWriter.close();

    createTestFile(FILE_PATH0);
    createTestFile(FILE_PATH1);

    Path[] filePaths = { FILE_PATH0, FILE_PATH1 };
    raidTestFiles(RAID_PATH, filePaths, doHar);

    clientConf = new Configuration(raidConf);
    clientConf.set("fs.hdfs.impl",
                   "org.apache.hadoop.hdfs.DistributedRaidFileSystem");
    clientConf.set("fs.raid.underlyingfs.impl",
                   "org.apache.hadoop.hdfs.DistributedFileSystem");

    // prepare shell and arguments
    shell = new RaidShell(clientConf);
    args = new String[2];
    args[0] = "-fsck";
    args[1] = DIR_PATH;

  }
  
  /**
   * Creates test file consisting of random data
   */
  private void createTestFile(Path filePath) throws IOException {
    Random rand = new Random();
    FSDataOutputStream stm = dfs.create(filePath, true, 
                                        conf.getInt("io.file.buffer.size",
                                                    4096), REPL, BLOCK_SIZE);

    final byte[] b = new byte[(int) BLOCK_SIZE];
    for (int i = 0; i < FILE_BLOCKS; i++) {
      rand.nextBytes(b);
      stm.write(b);
    }
    stm.close();
    LOG.info("test file created");

  }

  /**
   * raids test file
   */
  private void raidTestFiles(Path raidPath, Path[] filePaths, boolean doHar)
    throws IOException, ClassNotFoundException {
    // create RaidNode
    raidConf = new Configuration(conf);
    raidConf.set(RaidNode.RAID_LOCATION_KEY, RAID_DIR);
    raidConf.setInt("raid.blockfix.interval", 1000);
    raidConf.setLong("har.block.size", BLOCK_SIZE * 3);
    // the RaidNode does the raiding inline (instead of submitting to MR node)
    conf.set("raid.classname", "org.apache.hadoop.raid.LocalRaidNode");
    rnode = RaidNode.createRaidNode(null, raidConf);

    for (Path filePath: filePaths) {
      long waitStart = System.currentTimeMillis();
      boolean raided = false;
      
      Path parityFilePath = new Path(RAID_DIR, 
                                     filePath.toString().substring(1));

      while (!raided) {
        try {
          FileStatus[] listPaths = dfs.listStatus(raidPath);
          if (listPaths != null) {
            if (doHar) {
              // case with HAR
              for (FileStatus f: listPaths) {
                if (f.getPath().toString().endsWith(".har")) {
                  // check if the parity file is in the index
                  final Path indexPath = new Path(f.getPath(), "_index");
                  final FileStatus indexFileStatus = 
                    dfs.getFileStatus(indexPath);
                  final HarIndex harIndex = 
                    new HarIndex(dfs.open(indexPath), indexFileStatus.getLen());
                  final HarIndex.IndexEntry indexEntry =
                    harIndex.findEntryByFileName(parityFilePath.toString());
                  if (indexEntry != null) {
                    LOG.info("raid file " + parityFilePath.toString() + 
                             " found in Har archive: " + 
                             f.getPath().toString() +
                             " ts=" + indexEntry.mtime);
                    raided = true;
                    break;
                  }
                }
              }
              
            } else {
              // case without HAR
              for (FileStatus f : listPaths) {
                Path found = new Path(f.getPath().toUri().getPath());
                if (parityFilePath.equals(found)) {
                  LOG.info("raid file found: " + f.getPath().toString());
                  raided = true;
                  break;
                }
              }
            }
          }
        } catch (FileNotFoundException ignore) {
        }
        if (!raided) {
          if (System.currentTimeMillis() > waitStart + 40000L) {
            LOG.error("parity file not created after 40s");
            throw new IOException("parity file not HARed after 40s");
          } else {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException ignore) {
            }
          }
        }
      }
    }

    rnode.stop();
    rnode.join();
    rnode = null;
    LOG.info("test file raided");    
  }

  /**
   * sleeps for up to 20s until the number of corrupt files 
   * in the file system is equal to the number specified
   */
  private void waitUntilCorruptFileCount(DistributedFileSystem dfs,
                                         int corruptFiles)
    throws IOException {
    long waitStart = System.currentTimeMillis();
    while (RaidDFSUtil.getCorruptFiles(dfs).length != corruptFiles) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignore) {
        
      }

      if (System.currentTimeMillis() > waitStart + 20000L) {
        break;
      }
    }
    
    int corruptFilesFound = RaidDFSUtil.getCorruptFiles(dfs).length;
    if (corruptFilesFound != corruptFiles) {
      throw new IOException("expected " + corruptFiles + 
                            " corrupt files but got " +
                            corruptFilesFound);
    }
  }

  /**
   * removes a specified block from MiniDFS storage and reports it as corrupt
   */
  private void removeAndReportBlock(DistributedFileSystem blockDfs,
                                    Path filePath,
                                    LocatedBlock block) 
    throws IOException {
    TestRaidDfs.corruptBlock(cluster, filePath, block.getBlock(), NUM_DATANODES, true);
   
    // report deleted block to the name node
    LocatedBlock[] toReport = { block };
    blockDfs.getClient().getNamenode().reportBadBlocks(toReport);

  }


  /**
   * removes a file block in the specified stripe
   */
  private void removeFileBlock(Path filePath, int stripe, int blockInStripe)
    throws IOException {
    LocatedBlocks fileBlocks = dfs.getClient().getNamenode().
      getBlockLocations(filePath.toString(), 0, FILE_BLOCKS * BLOCK_SIZE);
    if (fileBlocks.locatedBlockCount() != FILE_BLOCKS) {
      throw new IOException("expected " + FILE_BLOCKS + 
                            " file blocks but found " + 
                            fileBlocks.locatedBlockCount());
    }
    if (blockInStripe >= STRIPE_BLOCKS) {
      throw new IOException("blockInStripe is " + blockInStripe +
                            " but must be smaller than " + STRIPE_BLOCKS);
    }
    LocatedBlock block = fileBlocks.get(stripe * STRIPE_BLOCKS + blockInStripe);
    removeAndReportBlock(dfs, filePath, block);
    LOG.info("removed file " + filePath.toString() + " block " +
             stripe * STRIPE_BLOCKS + " in stripe " + stripe);
  }

  /**
   * removes a parity block in the specified stripe
   */
  private void removeParityBlock(Path filePath, int stripe) throws IOException {
    // find parity file
    Path destPath = new Path(RAID_DIR);
    RaidNode.ParityFilePair ppair = null;

    ppair = RaidNode.getParityFile(destPath, filePath, conf);
    String parityPathStr = ppair.getPath().toUri().getPath();
    LOG.info("parity path: " + parityPathStr);
    FileSystem parityFS = ppair.getFileSystem();
    if (!(parityFS instanceof DistributedFileSystem)) {
      throw new IOException("parity file is not on distributed file system");
    }
    DistributedFileSystem parityDFS = (DistributedFileSystem) parityFS;

    
    // now corrupt the block corresponding to the stripe selected
    FileStatus parityFileStatus =
      parityDFS.getFileStatus(new Path(parityPathStr));
    long parityBlockSize = parityFileStatus.getBlockSize();
    long parityFileLength = parityFileStatus.getLen();
    long parityFileLengthInBlocks = (parityFileLength / parityBlockSize) + 
      (((parityFileLength % parityBlockSize) == 0) ? 0L : 1L);
    if (parityFileLengthInBlocks <= stripe) {
      throw new IOException("selected stripe " + stripe + 
                            " but parity file only has " + 
                            parityFileLengthInBlocks + " blocks");
    }
    if (parityBlockSize != BLOCK_SIZE) {
      throw new IOException("file block size is " + BLOCK_SIZE + 
                            " but parity file block size is " + 
                            parityBlockSize);
    }
    LocatedBlocks parityFileBlocks = parityDFS.getClient().getNamenode().
      getBlockLocations(parityPathStr, 0, parityFileLength);
    if (parityFileBlocks.locatedBlockCount() != parityFileLengthInBlocks) {
      throw new IOException("expected " + parityFileLengthInBlocks + 
                            " parity file blocks but got " + 
                            parityFileBlocks.locatedBlockCount() + 
                            " blocks");
    }
    LocatedBlock parityFileBlock = parityFileBlocks.get(stripe);
    removeAndReportBlock(parityDFS, new Path(parityPathStr), parityFileBlock);
    LOG.info("removed parity file block/stripe " + stripe +
             " for " + filePath.toString());

  }

  /**
   * removes a block from the har part file
   */
  private void removeHarParityBlock(int block) throws IOException {
    Path harPath = new Path(RAID_PATH, HAR_NAME);
    FileStatus [] listPaths = dfs.listStatus(harPath);
    
    boolean deleted = false;
    
    for (FileStatus f: listPaths) {
      if (f.getPath().getName().startsWith("part-")) {
        final Path partPath = new Path(f.getPath().toUri().getPath());
        final LocatedBlocks partBlocks  = dfs.getClient().getNamenode().
          getBlockLocations(partPath.toString(),
                            0,
                            f.getLen());
        
        if (partBlocks.locatedBlockCount() <= block) {
          throw new IOException("invalid har block " + block);
        }

        final LocatedBlock partBlock = partBlocks.get(block);
        removeAndReportBlock(dfs, partPath, partBlock);
        LOG.info("removed block " + block + "/" + 
                 partBlocks.locatedBlockCount() +
                 " of file " + partPath.toString() +
                 " block size " + partBlock.getBlockSize());
        deleted = true;
        break;
      }
    }

    if (!deleted) {
      throw new IOException("cannot find part file in " + harPath.toString());
    }
  }

  /**
   * checks fsck with no missing blocks
   */
  @Test
  public void testClean() throws Exception {
    LOG.info("testClean");
    setUp(false);
    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " +
               Integer.toString(result), result == 0);
  }


  /**
   * checks fsck with missing block in file block but not in parity block
   */
  @Test
  public void testFileBlockMissing() throws Exception {
    LOG.info("testFileBlockMissing");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 0, 0);
    waitUntilCorruptFileCount(dfs, 1);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " + 
               Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with missing block in parity block but not in file block
   */
  @Test
  public void testParityBlockMissing() throws Exception {
    LOG.info("testParityBlockMissing");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeParityBlock(FILE_PATH0, 0);
    waitUntilCorruptFileCount(dfs, 1);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " +
               Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with missing block in both file block and parity block
   * in different stripes
   */
  @Test
  public void testFileBlockAndParityBlockMissingInDifferentStripes() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingInDifferentStripes");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 0, 0);
    waitUntilCorruptFileCount(dfs, 1);
    removeParityBlock(FILE_PATH0, 1);
    waitUntilCorruptFileCount(dfs, 2);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " + 
               Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with missing block in both file block and parity block
   * in same stripe
   */
  @Test
  public void testFileBlockAndParityBlockMissingInSameStripe() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingInSameStripe");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeParityBlock(FILE_PATH0, 1);
    waitUntilCorruptFileCount(dfs, 1);
    removeFileBlock(FILE_PATH0, 1, 0);
    waitUntilCorruptFileCount(dfs, 2);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 1, but returns " + 
               Integer.toString(result), result == 1);
  }

  /**
   * checks fsck with two missing file blocks in same stripe
   */
  @Test
  public void test2FileBlocksMissingInSameStripe() 
    throws Exception {
    LOG.info("test2FileBlocksMissingInSameStripe");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 1, 1);
    waitUntilCorruptFileCount(dfs, 1);
    removeFileBlock(FILE_PATH0, 1, 0);
    waitUntilCorruptFileCount(dfs, 1);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 1, but returns " + 
               Integer.toString(result), result == 1);
  }

  /**
   * checks fsck with two missing file blocks in different stripes
   */
  @Test
  public void test2FileBlocksMissingInDifferentStripes() 
    throws Exception {
    LOG.info("test2FileBlocksMissingInDifferentStripes");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 1, 1);
    waitUntilCorruptFileCount(dfs, 1);
    removeFileBlock(FILE_PATH0, 0, 0);
    waitUntilCorruptFileCount(dfs, 1);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " +
               Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with file block missing (HAR)
   * use 2 files to verify HAR offset logic in RaidShell fsck
   * both files have one corrupt block, parity blocks are clean
   *
   * parity blocks in har (file.stripe):
   * +-----+-----+-----+  +-----+
   * | 0.0 | 0.1 | 1.0 |  | 1.1 |
   * +-----+-----+-----+  +-----+
   *  0                    1
   *
   */
  @Test
  public void testFileBlockMissingHar() 
    throws Exception {
    LOG.info("testFileBlockMissingHar");
    setUp(true);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 1, 1);
    removeFileBlock(FILE_PATH1, 1, 1);
    waitUntilCorruptFileCount(dfs, 2);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 0, but returns " +
               Integer.toString(result), result == 0);
  }

  /**
   * checks fsck with file block missing (HAR)
   * use 2 files to verify HAR offset logic in RaidShell fsck
   *
   * parity blocks in har (file.stripe):
   * +-----+-----+-----+  +-----+
   * | 0.0 | 0.1 | 1.0 |  | 1.1 |
   * +-----+-----+-----+  +-----+
   *  0                    1
   *
   * corrupt file 0, stripe 0 file block 0
   * corrupt file 0, stripe 1 file block 0
   * corrupt file 1, stripe 0 file block 0
   * corrupt file 1, stripe 1 file block 0
   * corrupt har block 0
   * both files should be corrupt
   */
  @Test
  public void testFileBlockAndParityBlockMissingHar1() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingHar1");
    setUp(true);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 0, 0);
    removeFileBlock(FILE_PATH0, 1, 0);
    removeFileBlock(FILE_PATH1, 0, 0);
    removeFileBlock(FILE_PATH1, 1, 0);
    removeHarParityBlock(0);
    waitUntilCorruptFileCount(dfs, 3);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 2, but returns " +
               Integer.toString(result), result == 2);
  }

  /**
   * checks fsck with file block missing (HAR)
   * use 2 files to verify HAR offset logic in RaidShell fsck
   *
   * parity blocks in har (file.stripe):
   * +-----+-----+-----+  +-----+
   * | 0.0 | 0.1 | 1.0 |  | 1.1 |
   * +-----+-----+-----+  +-----+
   *  0                    1
   *
   * corrupt file 0, stripe 0 file block 0
   * corrupt file 0, stripe 1 file block 0
   * corrupt file 1, stripe 0 file block 0
   * corrupt file 1, stripe 1 file block 0
   * corrupt har block 1
   * only file 2 should be corrupt
   */
  @Test
  public void testFileBlockAndParityBlockMissingHar2() 
    throws Exception {
    LOG.info("testFileBlockAndParityBlockMissingHar2");
    setUp(true);
    waitUntilCorruptFileCount(dfs, 0);
    removeFileBlock(FILE_PATH0, 0, 0);
    removeFileBlock(FILE_PATH0, 1, 0);
    removeFileBlock(FILE_PATH1, 0, 0);
    removeFileBlock(FILE_PATH1, 1, 0);
    removeHarParityBlock(1);
    waitUntilCorruptFileCount(dfs, 3);

    int result = ToolRunner.run(shell, args);

    assertTrue("fsck should return 1, but returns " +
               Integer.toString(result), result == 1);
  }

  /**
   * checks that fsck does not report corrupt file that is not in
   * the specified path
   */
  @Test
  public void testPathFilter() 
    throws Exception {
    LOG.info("testPathFilter");
    setUp(false);
    waitUntilCorruptFileCount(dfs, 0);
    removeParityBlock(FILE_PATH0, 1);
    waitUntilCorruptFileCount(dfs, 1);
    removeFileBlock(FILE_PATH0, 1, 0);
    waitUntilCorruptFileCount(dfs, 2);

    String[] otherArgs = new String[2];
    otherArgs[0] = "-fsck";
    otherArgs[1] = "/user/pkling/other";
    int result = ToolRunner.run(shell, otherArgs);

    assertTrue("fsck should return 0, but returns " + 
               Integer.toString(result), result == 0);
  }


  @After
  public void tearDown() throws Exception {
    if (rnode != null) {
      rnode.stop();
      rnode.join();
      rnode = null;
    }

    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    
    dfs = null;

    LOG.info("Test cluster shut down");
  }
  

}

