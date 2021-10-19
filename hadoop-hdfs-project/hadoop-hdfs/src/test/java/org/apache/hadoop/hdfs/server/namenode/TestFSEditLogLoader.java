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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyState;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeDirType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.GenericTestUtils.LogCapturer;
import org.apache.hadoop.test.PathUtils;
import org.apache.hadoop.util.FakeTimer;
import org.slf4j.event.Level;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;

@RunWith(Parameterized.class)
public class TestFSEditLogLoader {
  @Parameters
  public static Collection<Object[]> data() {
    Collection<Object[]> params = new ArrayList<Object[]>();
    params.add(new Object[]{ Boolean.FALSE });
    params.add(new Object[]{ Boolean.TRUE });
    return params;
  }

  private static boolean useAsyncEditLog;
  public TestFSEditLogLoader(Boolean async) {
    useAsyncEditLog = async;
  }

  private static Configuration getConf() {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        useAsyncEditLog);
    return conf;
  }

  static {
    GenericTestUtils.setLogLevel(FSImage.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(FSEditLogLoader.LOG, Level.TRACE);
  }

  private static final File TEST_DIR = PathUtils.getTestDir(TestFSEditLogLoader.class);

  private static final int NUM_DATA_NODES = 0;
  private static final String FAKE_EDIT_STREAM_NAME = "FAKE_STREAM";

  private final ErasureCodingPolicy testECPolicy
      = StripedFileTestUtil.getDefaultECPolicy();

  @Test
  public void testDisplayRecentEditLogOpCodes() throws IOException {
    // start a cluster
    Configuration conf = getConf();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
        .enableManagedDfsDirsRedundancy(false).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    for (int i = 0; i < 20; i++) {
      fileSys.mkdirs(new Path("/tmp/tmp" + i));
    }
    StorageDirectory sd = fsimage.getStorage().dirIterator(NameNodeDirType.EDITS).next();
    cluster.shutdown();

    File editFile = FSImageTestUtil.findLatestEditsLog(sd).getFile();
    assertTrue("Should exist: " + editFile, editFile.exists());

    // Corrupt the edits file.
    long fileLen = editFile.length();
    RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
    rwf.seek(fileLen - 40);
    for (int i = 0; i < 20; i++) {
      rwf.write(FSEditLogOpCodes.OP_DELETE.getOpCode());
    }
    rwf.close();
    
    StringBuilder bld = new StringBuilder();
    bld.append("^Error replaying edit log at offset \\d+.  ");
    bld.append("Expected transaction ID was \\d+\n");
    bld.append("Recent opcode offsets: (\\d+\\s*){4}$");
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
          .enableManagedDfsDirsRedundancy(false).format(false).build();
      fail("should not be able to start");
    } catch (IOException e) {
      assertTrue("error message contains opcodes message",
          e.getMessage().matches(bld.toString()));
    }
  }
  
  /**
   * Test that, if the NN restarts with a new minimum replication,
   * any files created with the old replication count will get
   * automatically bumped up to the new minimum upon restart.
   */
  @Test
  public void testReplicationAdjusted() throws Exception {
    // start a cluster 
    Configuration conf = getConf();
    // Replicate and heartbeat fast to shave a few seconds off test
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
  
      // Create a file with replication count 1
      Path p = new Path("/testfile");
      DFSTestUtil.createFile(fs, p, 10, /*repl*/ (short)1, 1);
      DFSTestUtil.waitReplication(fs, p, (short)1);
  
      // Shut down and restart cluster with new minimum replication of 2
      cluster.shutdown();
      cluster = null;
      
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
  
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      
      // The file should get adjusted to replication 2 when
      // the edit log is replayed.
      DFSTestUtil.waitReplication(fs, p, (short)2);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  /**
   * Corrupt the byte at the given offset in the given file,
   * by subtracting 1 from it.
   */
  private void corruptByteInFile(File file, long offset)
      throws IOException {
    RandomAccessFile raf = new RandomAccessFile(file, "rw");
    try {
      raf.seek(offset);
      int origByte = raf.read();
      raf.seek(offset);
      raf.writeByte(origByte - 1);
    } finally {
      IOUtils.closeStream(raf);
    }
  }

  /**
   * Truncate the given file to the given length
   */
  private void truncateFile(File logFile, long newLength)
      throws IOException {
    RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
    raf.setLength(newLength);
    raf.close();
  }

  /**
   * Return the length of bytes in the given file after subtracting
   * the trailer of 0xFF (OP_INVALID)s.
   * This seeks to the end of the file and reads chunks backwards until
   * it finds a non-0xFF byte.
   * @throws IOException if the file cannot be read
   */
  private static long getNonTrailerLength(File f) throws IOException {
    final int chunkSizeToRead = 256*1024;
    FileInputStream fis = new FileInputStream(f);
    try {
      
      byte buf[] = new byte[chunkSizeToRead];
  
      FileChannel fc = fis.getChannel();
      long size = fc.size();
      long pos = size - (size % chunkSizeToRead);
      
      while (pos >= 0) {
        fc.position(pos);
  
        int readLen = (int) Math.min(size - pos, chunkSizeToRead);
        IOUtils.readFully(fis, buf, 0, readLen);
        for (int i = readLen - 1; i >= 0; i--) {
          if (buf[i] != FSEditLogOpCodes.OP_INVALID.getOpCode()) {
            return pos + i + 1; // + 1 since we count this byte!
          }
        }
        
        pos -= chunkSizeToRead;
      }
      return 0;
    } finally {
      fis.close();
    }
  }

  @Test
  public void testStreamLimiter() throws IOException {
    final File LIMITER_TEST_FILE = new File(TEST_DIR, "limiter.test");
    
    FileOutputStream fos = new FileOutputStream(LIMITER_TEST_FILE);
    try {
      fos.write(0x12);
      fos.write(0x12);
      fos.write(0x12);
    } finally {
      fos.close();
    }
    
    FileInputStream fin = new FileInputStream(LIMITER_TEST_FILE);
    BufferedInputStream bin = new BufferedInputStream(fin);
    FSEditLogLoader.PositionTrackingInputStream tracker = 
        new FSEditLogLoader.PositionTrackingInputStream(bin);
    try {
      tracker.setLimit(2);
      tracker.mark(100);
      tracker.read();
      tracker.read();
      try {
        tracker.read();
        fail("expected to get IOException after reading past the limit");
      } catch (IOException e) {
      }
      tracker.reset();
      tracker.mark(100);
      byte arr[] = new byte[3];
      try {
        tracker.read(arr);
        fail("expected to get IOException after reading past the limit");
      } catch (IOException e) {
      }
      tracker.reset();
      arr = new byte[2];
      tracker.read(arr);
    } finally {
      tracker.close();
    }
  }

  /**
   * Create an unfinalized edit log for testing purposes
   *
   * @param testDir           Directory to create the edit log in
   * @param numTx             Number of transactions to add to the new edit log
   * @param offsetToTxId      A map from transaction IDs to offsets in the 
   *                          edit log file.
   * @return                  The new edit log file name.
   * @throws IOException
   */
  static private File prepareUnfinalizedTestEditLog(File testDir, int numTx,
      SortedMap<Long, Long> offsetToTxId) throws IOException {
    File inProgressFile = new File(testDir, NNStorage.getInProgressEditsFileName(1));
    FSEditLog fsel = null, spyLog = null;
    try {
      fsel = FSImageTestUtil.createStandaloneEditLog(testDir);
      spyLog = spy(fsel);
      // Normally, the in-progress edit log would be finalized by
      // FSEditLog#endCurrentLogSegment.  For testing purposes, we
      // disable that here.
      doNothing().when(spyLog).endCurrentLogSegment(true);
      spyLog.openForWrite(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);
      assertTrue("should exist: " + inProgressFile, inProgressFile.exists());
      
      for (int i = 0; i < numTx; i++) {
        long trueOffset = getNonTrailerLength(inProgressFile);
        long thisTxId = spyLog.getLastWrittenTxId() + 1;
        offsetToTxId.put(trueOffset, thisTxId);
        System.err.println("txid " + thisTxId + " at offset " + trueOffset);
        spyLog.logDelete("path" + i, i, false);
        spyLog.logSync();
      }
    } finally {
      if (spyLog != null) {
        spyLog.close();
      } else if (fsel != null) {
        fsel.close();
      }
    }
    return inProgressFile;
  }

  @Test
  public void testValidateEditLogWithCorruptHeader() throws IOException {
    File testDir = new File(TEST_DIR, "testValidateEditLogWithCorruptHeader");
    SortedMap<Long, Long> offsetToTxId = Maps.newTreeMap();
    File logFile = prepareUnfinalizedTestEditLog(testDir, 2, offsetToTxId);
    RandomAccessFile rwf = new RandomAccessFile(logFile, "rw");
    try {
      rwf.seek(0);
      rwf.writeLong(42); // corrupt header
    } finally {
      rwf.close();
    }
    EditLogValidation validation =
        EditLogFileInputStream.scanEditLog(logFile, Long.MAX_VALUE, true);
    assertTrue(validation.hasCorruptHeader());
  }

  @Test
  public void testValidateEditLogWithCorruptBody() throws IOException {
    File testDir = new File(TEST_DIR, "testValidateEditLogWithCorruptBody");
    SortedMap<Long, Long> offsetToTxId = Maps.newTreeMap();
    final int NUM_TXNS = 20;
    File logFile = prepareUnfinalizedTestEditLog(testDir, NUM_TXNS,
        offsetToTxId);
    // Back up the uncorrupted log
    File logFileBak = new File(testDir, logFile.getName() + ".bak");
    Files.copy(logFile, logFileBak);
    EditLogValidation validation =
        EditLogFileInputStream.scanEditLog(logFile, Long.MAX_VALUE, true);
    assertTrue(!validation.hasCorruptHeader());
    // We expect that there will be an OP_START_LOG_SEGMENT, followed by
    // NUM_TXNS opcodes, followed by an OP_END_LOG_SEGMENT.
    assertEquals(NUM_TXNS + 1, validation.getEndTxId());
    // Corrupt each edit and verify that validation continues to work
    for (Map.Entry<Long, Long> entry : offsetToTxId.entrySet()) {
      long txOffset = entry.getKey();
      long txId = entry.getValue();

      // Restore backup, corrupt the txn opcode
      Files.copy(logFileBak, logFile);
      corruptByteInFile(logFile, txOffset);
      validation = EditLogFileInputStream.scanEditLog(logFile,
          Long.MAX_VALUE, true);
      long expectedEndTxId = (txId == (NUM_TXNS + 1)) ?
          NUM_TXNS : (NUM_TXNS + 1);
      assertEquals("Failed when corrupting txn opcode at " + txOffset,
          expectedEndTxId, validation.getEndTxId());
      assertTrue(!validation.hasCorruptHeader());
    }

    // Truncate right before each edit and verify that validation continues
    // to work
    for (Map.Entry<Long, Long> entry : offsetToTxId.entrySet()) {
      long txOffset = entry.getKey();
      long txId = entry.getValue();

      // Restore backup, corrupt the txn opcode
      Files.copy(logFileBak, logFile);
      truncateFile(logFile, txOffset);
      validation = EditLogFileInputStream.scanEditLog(logFile,
          Long.MAX_VALUE, true);
      long expectedEndTxId = (txId == 0) ?
          HdfsServerConstants.INVALID_TXID : (txId - 1);
      assertEquals("Failed when corrupting txid " + txId + " txn opcode " +
        "at " + txOffset, expectedEndTxId, validation.getEndTxId());
      assertTrue(!validation.hasCorruptHeader());
    }
  }

  @Test
  public void testValidateEmptyEditLog() throws IOException {
    File testDir = new File(TEST_DIR, "testValidateEmptyEditLog");
    SortedMap<Long, Long> offsetToTxId = Maps.newTreeMap();
    File logFile = prepareUnfinalizedTestEditLog(testDir, 0, offsetToTxId);
    // Truncate the file so that there is nothing except the header and
    // layout flags section.
    truncateFile(logFile, 8);
    EditLogValidation validation =
        EditLogFileInputStream.scanEditLog(logFile, Long.MAX_VALUE, true);
    assertTrue(!validation.hasCorruptHeader());
    assertEquals(HdfsServerConstants.INVALID_TXID, validation.getEndTxId());
  }

  private static final Map<Byte, FSEditLogOpCodes> byteToEnum =
      new HashMap<Byte, FSEditLogOpCodes>();
  static {
    for(FSEditLogOpCodes opCode : FSEditLogOpCodes.values()) {
      byteToEnum.put(opCode.getOpCode(), opCode);
    }
  }

  private static FSEditLogOpCodes fromByte(byte opCode) {
    return byteToEnum.get(opCode);
  }

  @Test
  public void testFSEditLogOpCodes() throws IOException {
    //try all codes
    for(FSEditLogOpCodes c : FSEditLogOpCodes.values()) {
      final byte code = c.getOpCode();
      assertEquals("c=" + c + ", code=" + code,
          c, FSEditLogOpCodes.fromByte(code));
    }

    //try all byte values
    for(int b = 0; b < (1 << Byte.SIZE); b++) {
      final byte code = (byte)b;
      assertEquals("b=" + b + ", code=" + code,
          fromByte(code), FSEditLogOpCodes.fromByte(code));
    }
  }

  @Test
  public void testAddNewStripedBlock() throws IOException{
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      FSNamesystem fns = cluster.getNamesystem();
      fs.enableErasureCodingPolicy(testECPolicy.getName());

      String testDir = "/ec";
      String testFile = "testfile_001";
      String testFilePath = testDir + "/" + testFile;
      String clientName = "testUser1";
      String clientMachine = "testMachine1";
      long blkId = 1;
      long blkNumBytes = 1024;
      long timestamp = 1426222918;
      short blockNum = (short) testECPolicy.getNumDataUnits();
      short parityNum = (short) testECPolicy.getNumParityUnits();

      //set the storage policy of the directory
      fs.mkdir(new Path(testDir), new FsPermission("755"));
      fs.getClient().getNamenode().setErasureCodingPolicy(
          testDir, testECPolicy.getName());

      // Create a file with striped block
      Path p = new Path(testFilePath);
      DFSTestUtil.createFile(fs, p, 0, (short) 1, 1);

      fns.enterSafeMode(false);
      fns.saveNamespace(0, 0);
      fns.leaveSafeMode(false);

      // Add a striped block to the file
      BlockInfoStriped stripedBlk = new BlockInfoStriped(
          new Block(blkId, blkNumBytes, timestamp), testECPolicy);
      INodeFile file = (INodeFile)fns.getFSDirectory().getINode(testFilePath);
      file.toUnderConstruction(clientName, clientMachine);
      file.addBlock(stripedBlk);
      fns.getEditLog().logAddBlock(testFilePath, file);
      TestINodeFile.toCompleteFile(file);

      //If the block by loaded is the same as above it means that
      //we have successfully applied the edit log to the fsimage.
      cluster.restartNameNodes();
      cluster.waitActive();
      fns = cluster.getNamesystem();

      INodeFile inodeLoaded = (INodeFile)fns.getFSDirectory()
          .getINode(testFilePath);

      assertTrue(inodeLoaded.isStriped());

      BlockInfo[] blks = inodeLoaded.getBlocks();
      assertEquals(1, blks.length);
      assertEquals(blkId, blks[0].getBlockId());
      assertEquals(blkNumBytes, blks[0].getNumBytes());
      assertEquals(timestamp, blks[0].getGenerationStamp());
      assertEquals(blockNum, ((BlockInfoStriped)blks[0]).getDataBlockNum());
      assertEquals(parityNum, ((BlockInfoStriped)blks[0]).getParityBlockNum());

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testUpdateStripedBlocks() throws IOException{
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      FSNamesystem fns = cluster.getNamesystem();
      fs.enableErasureCodingPolicy(testECPolicy.getName());

      String testDir = "/ec";
      String testFile = "testfile_002";
      String testFilePath = testDir + "/" + testFile;
      String clientName = "testUser2";
      String clientMachine = "testMachine2";
      long blkId = 1;
      long blkNumBytes = 1024;
      long timestamp = 1426222918;
      short blockNum = (short) testECPolicy.getNumDataUnits();
      short parityNum = (short) testECPolicy.getNumParityUnits();

      //set the storage policy of the directory
      fs.mkdir(new Path(testDir), new FsPermission("755"));
      fs.getClient().getNamenode().setErasureCodingPolicy(
          testDir, testECPolicy.getName());

      //create a file with striped blocks
      Path p = new Path(testFilePath);
      DFSTestUtil.createFile(fs, p, 0, (short) 1, 1);
      BlockInfoStriped stripedBlk = new BlockInfoStriped(
          new Block(blkId, blkNumBytes, timestamp), testECPolicy);
      INodeFile file = (INodeFile)fns.getFSDirectory().getINode(testFilePath);
      file.toUnderConstruction(clientName, clientMachine);
      file.addBlock(stripedBlk);
      fns.getEditLog().logAddBlock(testFilePath, file);
      TestINodeFile.toCompleteFile(file);
      fns.enterSafeMode(false);
      fns.saveNamespace(0, 0);
      fns.leaveSafeMode(false);

      //update the last block
      long newBlkNumBytes = 1024*8;
      long newTimestamp = 1426222918+3600;
      file.toUnderConstruction(clientName, clientMachine);
      file.getLastBlock().setNumBytes(newBlkNumBytes);
      file.getLastBlock().setGenerationStamp(newTimestamp);
      fns.getEditLog().logUpdateBlocks(testFilePath, file, true);
      TestINodeFile.toCompleteFile(file);

      //After the namenode restarts if the block by loaded is the same as above
      //(new block size and timestamp) it means that we have successfully
      //applied the edit log to the fsimage.
      cluster.restartNameNodes();
      cluster.waitActive();
      fns = cluster.getNamesystem();

      INodeFile inodeLoaded = (INodeFile)fns.getFSDirectory()
          .getINode(testFilePath);

      assertTrue(inodeLoaded.isStriped());

      BlockInfo[] blks = inodeLoaded.getBlocks();
      assertEquals(1, blks.length);
      assertTrue(blks[0].isStriped());
      assertEquals(blkId, blks[0].getBlockId());
      assertEquals(newBlkNumBytes, blks[0].getNumBytes());
      assertEquals(newTimestamp, blks[0].getGenerationStamp());
      assertEquals(blockNum, ((BlockInfoStriped)blks[0]).getDataBlockNum());
      assertEquals(parityNum, ((BlockInfoStriped)blks[0]).getParityBlockNum());

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testHasNonEcBlockUsingStripedIDForAddBlock() throws IOException{
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      FSNamesystem fns = cluster.getNamesystem();

      String testDir = "/test_block_manager";
      String testFile = "testfile_addblock";
      String testFilePath = testDir + "/" + testFile;
      String clientName = "testUser_addblock";
      String clientMachine = "testMachine_addblock";
      long blkId = -1;
      long blkNumBytes = 1024;
      long timestamp = 1426222918;

      fs.mkdir(new Path(testDir), new FsPermission("755"));
      Path p = new Path(testFilePath);

      //check whether the hasNonEcBlockUsingStripedID is set
      //after loading a addblock-editlog
      DFSTestUtil.createFile(fs, p, 0, (short) 1, 1);
      BlockInfoContiguous cBlk = new BlockInfoContiguous(
          new Block(blkId, blkNumBytes, timestamp), (short)3);
      INodeFile file = (INodeFile)fns.getFSDirectory().getINode(testFilePath);
      file.toUnderConstruction(clientName, clientMachine);
      file.addBlock(cBlk);
      fns.getEditLog().logAddBlock(testFilePath, file);
      TestINodeFile.toCompleteFile(file);
      cluster.restartNameNodes();
      cluster.waitActive();
      fns = cluster.getNamesystem();
      assertTrue(fns.getBlockManager().hasNonEcBlockUsingStripedID());

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testHasNonEcBlockUsingStripedIDForUpdateBlocks()
      throws IOException{
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      FSNamesystem fns = cluster.getNamesystem();

      String testDir = "/test_block_manager";
      String testFile = "testfile_002";
      String testFilePath = testDir + "/" + testFile;
      String clientName = "testUser2";
      String clientMachine = "testMachine1";
      long blkId = 100;
      long blkNumBytes = 1024;
      long timestamp = 1426222918;

      fs.mkdir(new Path(testDir), new FsPermission("755"));
      Path p = new Path(testFilePath);

      DFSTestUtil.createFile(fs, p, 0, (short) 1, 1);
      BlockInfoContiguous cBlk = new BlockInfoContiguous(
          new Block(blkId, blkNumBytes, timestamp), (short)3);
      INodeFile file = (INodeFile)fns.getFSDirectory().getINode(testFilePath);
      file.toUnderConstruction(clientName, clientMachine);
      file.addBlock(cBlk);
      TestINodeFile.toCompleteFile(file);

      long newBlkNumBytes = 1024*8;
      long newTimestamp = 1426222918+3600;
      file.toUnderConstruction(clientName, clientMachine);
      file.getLastBlock().setBlockId(-100);
      file.getLastBlock().setNumBytes(newBlkNumBytes);
      file.getLastBlock().setGenerationStamp(newTimestamp);
      fns.getEditLog().logUpdateBlocks(testFilePath, file, true);
      TestINodeFile.toCompleteFile(file);
      cluster.restartNameNodes();
      cluster.waitActive();
      fns = cluster.getNamesystem();
      assertTrue(fns.getBlockManager().hasNonEcBlockUsingStripedID());

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testErasureCodingPolicyOperations() throws IOException {
    // start a cluster
    Configuration conf = new HdfsConfiguration();
    final int blockSize = 16 * 1024;
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(9)
          .build();
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      // 1. add new policy
      ECSchema schema = new ECSchema("rs", 5, 3);
      int cellSize = 2 * 1024;
      ErasureCodingPolicy newPolicy =
          new ErasureCodingPolicy(schema, cellSize, (byte) 0);
      ErasureCodingPolicy[] policyArray = new ErasureCodingPolicy[]{newPolicy};
      AddErasureCodingPolicyResponse[] responses =
          fs.addErasureCodingPolicies(policyArray);
      assertEquals(1, responses.length);
      assertTrue(responses[0].isSucceed());
      newPolicy = responses[0].getPolicy();

      // Restart NameNode without saving namespace
      cluster.restartNameNodes();
      cluster.waitActive();

      // check if new policy is reapplied through edit log
      ErasureCodingPolicy ecPolicy =
          ErasureCodingPolicyManager.getInstance().getByID(newPolicy.getId());
      assertEquals(ErasureCodingPolicyState.DISABLED,
          DFSTestUtil.getECPolicyState(ecPolicy));

      // 2. enable policy
      fs.enableErasureCodingPolicy(newPolicy.getName());
      cluster.restartNameNodes();
      cluster.waitActive();
      ecPolicy =
          ErasureCodingPolicyManager.getInstance().getByID(newPolicy.getId());
      assertEquals(ErasureCodingPolicyState.ENABLED,
          DFSTestUtil.getECPolicyState(ecPolicy));

      // create a new file, use the policy
      final Path dirPath = new Path("/striped");
      final Path filePath = new Path(dirPath, "file");
      final int fileLength = blockSize * newPolicy.getNumDataUnits();
      fs.mkdirs(dirPath);
      fs.setErasureCodingPolicy(dirPath, newPolicy.getName());
      final byte[] bytes = StripedFileTestUtil.generateBytes(fileLength);
      DFSTestUtil.writeFile(fs, filePath, bytes);

      // 3. disable policy
      fs.disableErasureCodingPolicy(newPolicy.getName());
      cluster.restartNameNodes();
      cluster.waitActive();
      ecPolicy =
          ErasureCodingPolicyManager.getInstance().getByID(newPolicy.getId());
      assertEquals(ErasureCodingPolicyState.DISABLED,
          DFSTestUtil.getECPolicyState(ecPolicy));
      // read file
      DFSTestUtil.readFileAsBytes(fs, filePath);

      // 4. remove policy
      fs.removeErasureCodingPolicy(newPolicy.getName());
      cluster.restartNameNodes();
      cluster.waitActive();
      ecPolicy =
          ErasureCodingPolicyManager.getInstance().getByID(newPolicy.getId());
      assertEquals(ErasureCodingPolicyState.REMOVED,
          DFSTestUtil.getECPolicyState(ecPolicy));
      // read file
      DFSTestUtil.readFileAsBytes(fs, filePath);

      cluster.shutdown();
      cluster = null;
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void setLoadFSEditLogThrottling() throws Exception {
    FSNamesystem namesystem = mock(FSNamesystem.class);
    namesystem.dir = mock(FSDirectory.class);

    FakeTimer timer = new FakeTimer();
    FSEditLogLoader loader = new FSEditLogLoader(namesystem, 0, timer);

    LogCapturer capture = LogCapturer.captureLogs(FSImage.LOG);
    loader.loadFSEdits(getFakeEditLogInputStream(1, 10), 1);
    assertTrue(capture.getOutput().contains("Start loading edits file " +
        FAKE_EDIT_STREAM_NAME));
    assertTrue(capture.getOutput().contains("Loaded 1 edits file(s)"));
    assertFalse(capture.getOutput().contains("suppressed"));

    timer.advance(FSEditLogLoader.LOAD_EDIT_LOG_INTERVAL_MS / 2);
    capture.clearOutput();
    loader.loadFSEdits(getFakeEditLogInputStream(11, 20), 11);
    assertFalse(capture.getOutput().contains("Start loading edits file"));
    assertFalse(capture.getOutput().contains("edits file(s)"));

    timer.advance(FSEditLogLoader.LOAD_EDIT_LOG_INTERVAL_MS);
    capture.clearOutput();
    loader.loadFSEdits(getFakeEditLogInputStream(21, 30), 21);
    assertTrue(capture.getOutput().contains("Start loading edits file " +
        FAKE_EDIT_STREAM_NAME));
    assertTrue(capture.getOutput().contains("suppressed logging 1 times"));
    assertTrue(capture.getOutput().contains("Loaded 2 edits file(s)"));
    assertTrue(capture.getOutput().contains("total size 2.0"));
  }

  private EditLogInputStream getFakeEditLogInputStream(long startTx, long endTx)
      throws IOException {
    EditLogInputStream fakeStream = mock(EditLogInputStream.class);
    when(fakeStream.getName()).thenReturn(FAKE_EDIT_STREAM_NAME);
    when(fakeStream.getFirstTxId()).thenReturn(startTx);
    when(fakeStream.getLastTxId()).thenReturn(endTx);
    when(fakeStream.length()).thenReturn(1L);
    return fakeStream;
  }

}
