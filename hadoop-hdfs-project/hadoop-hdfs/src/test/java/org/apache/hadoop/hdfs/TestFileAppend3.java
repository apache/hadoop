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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.fs.CreateFlag;
import org.mockito.invocation.InvocationOnMock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** This class implements some of tests posted in HADOOP-2658. */
public class TestFileAppend3  {
  {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(InterDatanodeProtocol.LOG, Level.ALL);
  }

  static final long BLOCK_SIZE = 64 * 1024;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 5;

  private static Configuration conf;
  private static int buffersize;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  @BeforeClass
  public static void setUp() throws java.lang.Exception {
    AppendTestUtil.LOG.info("setUp()");
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 512);
    buffersize = conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    fs = cluster.getFileSystem();
  }
   
  @AfterClass
  public static void tearDown() throws Exception {
    AppendTestUtil.LOG.info("tearDown()");
    if(fs != null) fs.close();
    if(cluster != null) cluster.shutdown();
  }

  /**
   * TC1: Append on block boundary.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testTC1() throws Exception {
    final Path p = new Path("/TC1/foo");
    System.out.println("p=" + p);

    //a. Create file and write one block of data. Close file.
    final int len1 = (int)BLOCK_SIZE; 
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    //   Reopen file to append. Append half block of data. Close file.
    final int len2 = (int)BLOCK_SIZE/2; 
    {
      FSDataOutputStream out = fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }
    
    //b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);
  }

  @Test
  public void testTC1ForAppend2() throws Exception {
    final Path p = new Path("/TC1/foo2");

    //a. Create file and write one block of data. Close file.
    final int len1 = (int) BLOCK_SIZE;
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    // Reopen file to append. Append half block of data. Close file.
    final int len2 = (int) BLOCK_SIZE / 2;
    {
      FSDataOutputStream out = fs.append(p,
          EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    // b. Reopen file and read 1.5 blocks worth of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);
  }

  /**
   * TC2: Append on non-block boundary.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testTC2() throws Exception {
    final Path p = new Path("/TC2/foo");
    System.out.println("p=" + p);

    //a. Create file with one and a half block of data. Close file.
    final int len1 = (int)(BLOCK_SIZE + BLOCK_SIZE/2); 
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    AppendTestUtil.check(fs, p, len1);

    //   Reopen file to append quarter block of data. Close file.
    final int len2 = (int)BLOCK_SIZE/4; 
    {
      FSDataOutputStream out = fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    //b. Reopen file and read 1.75 blocks of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);
  }

  @Test
  public void testTC2ForAppend2() throws Exception {
    final Path p = new Path("/TC2/foo2");

    //a. Create file with one and a half block of data. Close file.
    final int len1 = (int) (BLOCK_SIZE + BLOCK_SIZE / 2);
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    AppendTestUtil.check(fs, p, len1);

    //   Reopen file to append quarter block of data. Close file.
    final int len2 = (int) BLOCK_SIZE / 4;
    {
      FSDataOutputStream out = fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK),
          4096, null);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    // b. Reopen file and read 1.75 blocks of data. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);
    List<LocatedBlock> blocks = fs.getClient().getLocatedBlocks(
        p.toString(), 0L).getLocatedBlocks();
    Assert.assertEquals(3, blocks.size());
    Assert.assertEquals(BLOCK_SIZE, blocks.get(0).getBlockSize());
    Assert.assertEquals(BLOCK_SIZE / 2, blocks.get(1).getBlockSize());
    Assert.assertEquals(BLOCK_SIZE / 4, blocks.get(2).getBlockSize());
  }

  /**
   * TC5: Only one simultaneous append.
   * @throws IOException an exception might be thrown
   */
  @Test
  public void testTC5() throws Exception {
    final Path p = new Path("/TC5/foo");
    System.out.println("p=" + p);

    //a. Create file on Machine M1. Write half block to it. Close file.
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, (int)(BLOCK_SIZE/2));
      out.close();
    }

    //b. Reopen file in "append" mode on Machine M1.
    FSDataOutputStream out = fs.append(p);

    //c. On Machine M2, reopen file in "append" mode. This should fail.
    try {
      AppendTestUtil.createHdfsWithDifferentUsername(conf).append(p);
      fail("This should fail.");
    } catch(IOException ioe) {
      AppendTestUtil.LOG.info("GOOD: got an exception", ioe);
    }

    try {
      ((DistributedFileSystem) AppendTestUtil
          .createHdfsWithDifferentUsername(conf)).append(p,
          EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
      fail("This should fail.");
    } catch(IOException ioe) {
      AppendTestUtil.LOG.info("GOOD: got an exception", ioe);
    }

    //d. On Machine M1, close file.
    out.close();        
  }

  @Test
  public void testTC5ForAppend2() throws Exception {
    final Path p = new Path("/TC5/foo2");

    // a. Create file on Machine M1. Write half block to it. Close file.
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION,
          BLOCK_SIZE);
      AppendTestUtil.write(out, 0, (int)(BLOCK_SIZE/2));
      out.close();
    }

    // b. Reopen file in "append" mode on Machine M1.
    FSDataOutputStream out = fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK),
        4096, null);

    // c. On Machine M2, reopen file in "append" mode. This should fail.
    try {
      ((DistributedFileSystem) AppendTestUtil
          .createHdfsWithDifferentUsername(conf)).append(p,
          EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null);
      fail("This should fail.");
    } catch(IOException ioe) {
      AppendTestUtil.LOG.info("GOOD: got an exception", ioe);
    }

    try {
      AppendTestUtil.createHdfsWithDifferentUsername(conf).append(p);
      fail("This should fail.");
    } catch(IOException ioe) {
      AppendTestUtil.LOG.info("GOOD: got an exception", ioe);
    }

    // d. On Machine M1, close file.
    out.close();
  }

  /**
   * TC7: Corrupted replicas are present.
   * @throws IOException an exception might be thrown
   */
  private void testTC7(boolean appendToNewBlock) throws Exception {
    final short repl = 2;
    final Path p = new Path("/TC7/foo" + (appendToNewBlock ? "0" : "1"));
    System.out.println("p=" + p);
    
    //a. Create file with replication factor of 2. Write half block of data. Close file.
    final int len1 = (int)(BLOCK_SIZE/2); 
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, repl, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }
    DFSTestUtil.waitReplication(fs, p, repl);

    //b. Log into one datanode that has one replica of this block.
    //   Find the block file on this datanode and truncate it to zero size.
    final LocatedBlocks locatedblocks = fs.dfs.getNamenode().getBlockLocations(p.toString(), 0L, len1);
    assertEquals(1, locatedblocks.locatedBlockCount());
    final LocatedBlock lb = locatedblocks.get(0);
    final ExtendedBlock blk = lb.getBlock();
    assertEquals(len1, lb.getBlockSize());

    DatanodeInfo[] datanodeinfos = lb.getLocations();
    assertEquals(repl, datanodeinfos.length);
    final DataNode dn = cluster.getDataNode(datanodeinfos[0].getIpcPort());
    final File f = DataNodeTestUtils.getBlockFile(
        dn, blk.getBlockPoolId(), blk.getLocalBlock());
    final RandomAccessFile raf = new RandomAccessFile(f, "rw");
    AppendTestUtil.LOG.info("dn=" + dn + ", blk=" + blk + " (length=" + blk.getNumBytes() + ")");
    assertEquals(len1, raf.length());
    raf.setLength(0);
    raf.close();

    //c. Open file in "append mode".  Append a new block worth of data. Close file.
    final int len2 = (int)BLOCK_SIZE; 
    {
      FSDataOutputStream out = appendToNewBlock ?
          fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) : fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    //d. Reopen file and read two blocks worth of data.
    AppendTestUtil.check(fs, p, len1 + len2);
  }

  @Test
  public void testTC7() throws Exception {
    testTC7(false);
  }

  @Test
  public void testTC7ForAppend2() throws Exception {
    testTC7(true);
  }

  /**
   * TC11: Racing rename
   */
  private void testTC11(boolean appendToNewBlock) throws Exception {
    final Path p = new Path("/TC11/foo" + (appendToNewBlock ? "0" : "1"));
    System.out.println("p=" + p);

    //a. Create file and write one block of data. Close file.
    final int len1 = (int)BLOCK_SIZE; 
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    //b. Reopen file in "append" mode. Append half block of data.
    FSDataOutputStream out = appendToNewBlock ?
        fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
        fs.append(p);
    final int len2 = (int)BLOCK_SIZE/2; 
    AppendTestUtil.write(out, len1, len2);
    out.hflush();
    
    //c. Rename file to file.new.
    final Path pnew = new Path(p + ".new");
    assertTrue(fs.rename(p, pnew));

    //d. Close file handle that was opened in (b). 
    out.close();

    //check block sizes
    final long len = fs.getFileStatus(pnew).getLen();
    final LocatedBlocks locatedblocks = fs.dfs.getNamenode().getBlockLocations(pnew.toString(), 0L, len);
    final int numblock = locatedblocks.locatedBlockCount();
    for(int i = 0; i < numblock; i++) {
      final LocatedBlock lb = locatedblocks.get(i);
      final ExtendedBlock blk = lb.getBlock();
      final long size = lb.getBlockSize();
      if (i < numblock - 1) {
        assertEquals(BLOCK_SIZE, size);
      }
      for(DatanodeInfo datanodeinfo : lb.getLocations()) {
        final DataNode dn = cluster.getDataNode(datanodeinfo.getIpcPort());
        final Block metainfo = DataNodeTestUtils.getFSDataset(dn).getStoredBlock(
            blk.getBlockPoolId(), blk.getBlockId());
        assertEquals(size, metainfo.getNumBytes());
      }
    }
  }

  @Test
  public void testTC11() throws Exception {
    testTC11(false);
  }

  @Test
  public void testTC11ForAppend2() throws Exception {
    testTC11(true);
  }

  /** 
   * TC12: Append to partial CRC chunk
   */
  private void testTC12(boolean appendToNewBlock) throws Exception {
    final Path p = new Path("/TC12/foo" + (appendToNewBlock ? "0" : "1"));
    System.out.println("p=" + p);
    
    //a. Create file with a block size of 64KB
    //   and a default io.bytes.per.checksum of 512 bytes.
    //   Write 25687 bytes of data. Close file.
    final int len1 = 25687; 
    {
      FSDataOutputStream out = fs.create(p, false, buffersize, REPLICATION, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, len1);
      out.close();
    }

    //b. Reopen file in "append" mode. Append another 5877 bytes of data. Close file.
    final int len2 = 5877; 
    {
      FSDataOutputStream out = appendToNewBlock ?
          fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
          fs.append(p);
      AppendTestUtil.write(out, len1, len2);
      out.close();
    }

    //c. Reopen file and read 25687+5877 bytes of data from file. Close file.
    AppendTestUtil.check(fs, p, len1 + len2);
    if (appendToNewBlock) {
      LocatedBlocks blks = fs.dfs.getLocatedBlocks(p.toString(), 0);
      Assert.assertEquals(2, blks.getLocatedBlocks().size());
      Assert.assertEquals(len1, blks.getLocatedBlocks().get(0).getBlockSize());
      Assert.assertEquals(len2, blks.getLocatedBlocks().get(1).getBlockSize());
      AppendTestUtil.check(fs, p, 0, len1);
      AppendTestUtil.check(fs, p, len1, len2);
    }
  }

  @Test
  public void testTC12() throws Exception {
    testTC12(false);
  }

  @Test
  public void testTC12ForAppend2() throws Exception {
    testTC12(true);
  }

  /**
   * Append to a partial CRC chunk and the first write does not fill up the
   * partial CRC trunk
   */
  private void testAppendToPartialChunk(boolean appendToNewBlock)
      throws IOException {
    final Path p = new Path("/partialChunk/foo"
        + (appendToNewBlock ? "0" : "1"));
    final int fileLen = 513;
    System.out.println("p=" + p);
    
    byte[] fileContents = AppendTestUtil.initBuffer(fileLen);

    // create a new file.
    FSDataOutputStream stm = AppendTestUtil.createFile(fs, p, 1);

    // create 1 byte file
    stm.write(fileContents, 0, 1);
    stm.close();
    System.out.println("Wrote 1 byte and closed the file " + p);

    // append to file
    stm = appendToNewBlock ?
        fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
        fs.append(p);
    // Append to a partial CRC trunk
    stm.write(fileContents, 1, 1);
    stm.hflush();
    // The partial CRC trunk is not full yet and close the file
    stm.close();
    System.out.println("Append 1 byte and closed the file " + p);

    // write the remainder of the file
    stm = appendToNewBlock ?
        fs.append(p, EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null) :
        fs.append(p);

    // ensure getPos is set to reflect existing size of the file
    assertEquals(2, stm.getPos());

    // append to a partial CRC trunk
    stm.write(fileContents, 2, 1);
    // The partial chunk is not full yet, force to send a packet to DN
    stm.hflush();
    System.out.println("Append and flush 1 byte");
    // The partial chunk is not full yet, force to send another packet to DN
    stm.write(fileContents, 3, 2);
    stm.hflush();
    System.out.println("Append and flush 2 byte");

    // fill up the partial chunk and close the file
    stm.write(fileContents, 5, fileLen-5);
    stm.close();
    System.out.println("Flush 508 byte and closed the file " + p);

    // verify that entire file is good
    AppendTestUtil.checkFullFile(fs, p, fileLen,
        fileContents, "Failed to append to a partial chunk");
  }

  // Do small appends.
  void doSmallAppends(Path file, DistributedFileSystem fs, int iterations)
    throws IOException {
    for (int i = 0; i < iterations; i++) {
      FSDataOutputStream stm;
      try {
        stm = fs.append(file);
      } catch (IOException e) {
        // If another thread is already appending, skip this time.
        continue;
      }
      // Failure in write or close will be terminal.
      AppendTestUtil.write(stm, 0, 123);
      stm.close();
    }
  }


  @Test
  public void testSmallAppendRace()  throws Exception {
    final Path file = new Path("/testSmallAppendRace");
    final String fName = file.toUri().getPath();

    // Create the file and write a small amount of data.
    FSDataOutputStream stm = fs.create(file);
    AppendTestUtil.write(stm, 0, 123);
    stm.close();

    // Introduce a delay between getFileInfo and calling append() against NN.
    final DFSClient client = DFSClientAdapter.getDFSClient(fs);
    DFSClient spyClient = spy(client);
    when(spyClient.getFileInfo(fName)).thenAnswer(new Answer<HdfsFileStatus>() {
      @Override
      public HdfsFileStatus answer(InvocationOnMock invocation){
        try {
          HdfsFileStatus stat = client.getFileInfo(fName);
          Thread.sleep(100);
          return stat;
        } catch (Exception e) {
          return null;
        }
      }
    });

    DFSClientAdapter.setDFSClient(fs, spyClient);

    // Create two threads for doing appends to the same file.
    Thread worker1 = new Thread() {
      @Override
      public void run() {
        try {
          doSmallAppends(file, fs, 20);
        } catch (IOException e) {
        }
      }
    };

    Thread worker2 = new Thread() {
      @Override
      public void run() {
        try {
          doSmallAppends(file, fs, 20);
        } catch (IOException e) {
        }
      }
    };

    worker1.start();
    worker2.start();

    // append will fail when the file size crosses the checksum chunk boundary,
    // if append was called with a stale file stat.
    doSmallAppends(file, fs, 20);
  }

  @Test
  public void testAppendToPartialChunk() throws IOException {
    testAppendToPartialChunk(false);
  }

  @Test
  public void testAppendToPartialChunkforAppend2() throws IOException {
    testAppendToPartialChunk(true);
  }
}
