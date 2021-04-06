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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Test;

/** Class contains a set of tests to verify the correctness of 
 * newly introduced {@link FSDataOutputStream#hflush()} method */
public class TestHFlush {
  {
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }
  
  private final String fName = "hflushtest.dat";
  
  /**
   * The test uses
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)} 
   * to write a file with a standard block size
   */
  @Test
  public void hFlush_01() throws IOException {
    doTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BLOCK_SIZE,
        (short) 2, false, EnumSet.noneOf(SyncFlag.class));
  }

  /**
   * The test uses
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)} 
   * to write a file with a custom block size so the writes will be 
   * happening across block' boundaries
   */
  @Test
  public void hFlush_02() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, false,
        EnumSet.noneOf(SyncFlag.class));
  }

  /**
   * The test uses
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)} 
   * to write a file with a custom block size so the writes will be 
   * happening across block's and checksum' boundaries
   */
  @Test
  public void hFlush_03() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, false,
        EnumSet.noneOf(SyncFlag.class));
  }

  /**
   * Test hsync (with updating block length in NameNode) while no data is
   * actually written yet
   */
  @Test
  public void hSyncUpdateLength_00() throws IOException {
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        2).build();
    DistributedFileSystem fileSystem =
        cluster.getFileSystem();
    
    try {
      Path path = new Path(fName);
      FSDataOutputStream stm = fileSystem.create(path, true, 4096, (short) 2,
          AppendTestUtil.BLOCK_SIZE);
      System.out.println("Created file " + path.toString());
      ((DFSOutputStream) stm.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.UPDATE_LENGTH));
      long currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(0L, currentFileLength);
      stm.close();
    } finally {
      fileSystem.close();
      cluster.shutdown();
    }
  }

  /**
   * Test hsync with END_BLOCK flag.
   */
  @Test
  public void hSyncEndBlock_00() throws IOException {
    final int preferredBlockSize = 1024;
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, preferredBlockSize);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .build();
    DistributedFileSystem fileSystem = cluster.getFileSystem();
    FSDataOutputStream stm = null;
    try {
      Path path = new Path("/" + fName);
      stm = fileSystem.create(path, true, 4096, (short) 2,
          AppendTestUtil.BLOCK_SIZE);
      System.out.println("Created file " + path.toString());
      ((DFSOutputStream) stm.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.END_BLOCK));
      long currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(0L, currentFileLength);
      LocatedBlocks blocks = fileSystem.dfs.getLocatedBlocks(path.toString(), 0);
      assertEquals(0, blocks.getLocatedBlocks().size());

      // write a block and call hsync(end_block) at the block boundary
      stm.write(new byte[preferredBlockSize]);
      ((DFSOutputStream) stm.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.END_BLOCK));
      currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(preferredBlockSize, currentFileLength);
      blocks = fileSystem.dfs.getLocatedBlocks(path.toString(), 0);
      assertEquals(1, blocks.getLocatedBlocks().size());

      // call hsync then call hsync(end_block) immediately
      stm.write(new byte[preferredBlockSize / 2]);
      stm.hsync();
      ((DFSOutputStream) stm.getWrappedStream()).hsync(EnumSet
          .of(SyncFlag.END_BLOCK));
      currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(preferredBlockSize + preferredBlockSize / 2,
          currentFileLength);
      blocks = fileSystem.dfs.getLocatedBlocks(path.toString(), 0);
      assertEquals(2, blocks.getLocatedBlocks().size());

      stm.write(new byte[preferredBlockSize / 4]);
      stm.hsync();
      currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(preferredBlockSize + preferredBlockSize / 2
          + preferredBlockSize / 4, currentFileLength);
      blocks = fileSystem.dfs.getLocatedBlocks(path.toString(), 0);
      assertEquals(3, blocks.getLocatedBlocks().size());
    } finally {
      IOUtils.cleanupWithLogger(null, stm, fileSystem);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * The test calls
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)}
   * while requiring the semantic of {@link SyncFlag#UPDATE_LENGTH}.
   */
  @Test
  public void hSyncUpdateLength_01() throws IOException {
    doTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BLOCK_SIZE,
        (short) 2, true, EnumSet.of(SyncFlag.UPDATE_LENGTH));
  }

  /**
   * The test calls
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)}
   * while requiring the semantic of {@link SyncFlag#END_BLOCK}.
   */
  @Test
  public void hSyncEndBlock_01() throws IOException {
    doTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BLOCK_SIZE,
        (short) 2, true, EnumSet.of(SyncFlag.END_BLOCK));
  }

  /**
   * The test calls
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)}
   * while requiring the semantic of {@link SyncFlag#END_BLOCK} and
   * {@link SyncFlag#UPDATE_LENGTH}.
   */
  @Test
  public void hSyncEndBlockAndUpdateLength() throws IOException {
    doTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BLOCK_SIZE,
        (short) 2, true, EnumSet.of(SyncFlag.END_BLOCK, SyncFlag.UPDATE_LENGTH));
  }

  /**
   * The test calls
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)}
   * while requiring the semantic of {@link SyncFlag#UPDATE_LENGTH}.
   * Similar with {@link #hFlush_02()} , it writes a file with a custom block
   * size so the writes will be happening across block' boundaries
   */
  @Test
  public void hSyncUpdateLength_02() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true,
        EnumSet.of(SyncFlag.UPDATE_LENGTH));
  }

  @Test
  public void hSyncEndBlock_02() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true,
        EnumSet.of(SyncFlag.END_BLOCK));
  }

  /**
   * The test calls
   * {@link #doTheJob(Configuration, String, long, short, boolean, EnumSet)}
   * while requiring the semantic of {@link SyncFlag#UPDATE_LENGTH}.
   * Similar with {@link #hFlush_03()} , it writes a file with a custom block
   * size so the writes will be happening across block's and checksum'
   * boundaries.
   */
  @Test
  public void hSyncUpdateLength_03() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true,
        EnumSet.of(SyncFlag.UPDATE_LENGTH));
  }

  @Test
  public void hSyncEndBlock_03() throws IOException {
    Configuration conf = new HdfsConfiguration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true,
        EnumSet.of(SyncFlag.END_BLOCK));
  }

  /**
   * The method starts new cluster with defined Configuration; creates a file
   * with specified block_size and writes 10 equal sections in it; it also calls
   * hflush/hsync after each write and throws an IOException in case of an error.
   * 
   * @param conf cluster configuration
   * @param fileName of the file to be created and processed as required
   * @param block_size value to be used for the file's creation
   * @param replicas is the number of replicas
   * @param isSync hsync or hflush         
   * @param syncFlags specify the semantic of the sync/flush
   * @throws IOException in case of any errors
   */
  public static void doTheJob(Configuration conf, final String fileName,
      long block_size, short replicas, boolean isSync,
      EnumSet<SyncFlag> syncFlags) throws IOException {
    byte[] fileContent;
    final int SECTIONS = 10;

    fileContent = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
                                               .numDataNodes(replicas).build();
    // Make sure we work with DFS in order to utilize all its functionality
    DistributedFileSystem fileSystem = cluster.getFileSystem();

    FSDataInputStream is;
    try {
      Path path = new Path(fileName);
      final String pathName = new Path(fileSystem.getWorkingDirectory(), path)
          .toUri().getPath();
      FSDataOutputStream stm = fileSystem.create(path, false, 4096, replicas,
          block_size);
      System.out.println("Created file " + fileName);

      int tenth = AppendTestUtil.FILE_SIZE/SECTIONS;
      int rounding = AppendTestUtil.FILE_SIZE - tenth * SECTIONS;
      for (int i=0; i<SECTIONS; i++) {
        System.out.println("Writing " + (tenth * i) + " to "
            + (tenth * (i + 1)) + " section to file " + fileName);
        // write to the file
        stm.write(fileContent, tenth * i, tenth);
        
        // Wait while hflush/hsync pushes all packets through built pipeline
        if (isSync) {
          ((DFSOutputStream)stm.getWrappedStream()).hsync(syncFlags);
        } else {
          ((DFSOutputStream)stm.getWrappedStream()).hflush();
        }
        
        // Check file length if updatelength is required
        if (isSync && syncFlags.contains(SyncFlag.UPDATE_LENGTH)) {
          long currentFileLength = fileSystem.getFileStatus(path).getLen();
          assertEquals(
            "File size doesn't match for hsync/hflush with updating the length",
            tenth * (i + 1), currentFileLength);
        } else if (isSync && syncFlags.contains(SyncFlag.END_BLOCK)) {
          LocatedBlocks blocks = fileSystem.dfs.getLocatedBlocks(pathName, 0);
          assertEquals(i + 1, blocks.getLocatedBlocks().size());
        }

        byte [] toRead = new byte[tenth];
        byte [] expected = new byte[tenth];
        System.arraycopy(fileContent, tenth * i, expected, 0, tenth);
        // Open the same file for read. Need to create new reader after every write operation(!)
        is = fileSystem.open(path);
        is.seek(tenth * i);
        int readBytes = is.read(toRead, 0, tenth);
        System.out.println("Has read " + readBytes);
        assertTrue("Should've get more bytes", (readBytes > 0) && (readBytes <= tenth));
        is.close();
        checkData(toRead, 0, readBytes, expected, "Partial verification");
      }
      System.out.println("Writing " + (tenth * SECTIONS) + " to " + (tenth * SECTIONS + rounding) + " section to file " + fileName);
      stm.write(fileContent, tenth * SECTIONS, rounding);
      stm.close();

      assertEquals("File size doesn't match ", AppendTestUtil.FILE_SIZE, fileSystem.getFileStatus(path).getLen());
      AppendTestUtil.checkFullFile(fileSystem, path, fileContent.length, fileContent, "hflush()");
    } finally {
      fileSystem.close();
      cluster.shutdown();
    }
  }
  static void checkData(final byte[] actual, int from, int len,
                        final byte[] expected, String message) {
    for (int idx = 0; idx < len; idx++) {
      assertEquals(message+" byte "+(from+idx)+" differs. expected "+
                   expected[from+idx]+" actual "+actual[idx],
                   expected[from+idx], actual[idx]);
      actual[idx] = 0;
    }
  }
  
  /** This creates a slow writer and check to see 
   * if pipeline heartbeats work fine
   */
 @Test
  public void testPipelineHeartbeat() throws Exception {
    final int DATANODE_NUM = 2;
    final int fileLen = 6;
    Configuration conf = new HdfsConfiguration();
    final int timeout = 2000;
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY,
        timeout);

    final Path p = new Path("/pipelineHeartbeat/foo");
    System.out.println("p=" + p);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      byte[] fileContents = AppendTestUtil.initBuffer(fileLen);

      // create a new file.
      FSDataOutputStream stm = AppendTestUtil.createFile(fs, p, DATANODE_NUM);

      stm.write(fileContents, 0, 1);
      Thread.sleep(timeout);
      stm.hflush();
      System.out.println("Wrote 1 byte and hflush " + p);

      // write another byte
      Thread.sleep(timeout);
      stm.write(fileContents, 1, 1);
      stm.hflush();

      stm.write(fileContents, 2, 1);
      Thread.sleep(timeout);
      stm.hflush();

      stm.write(fileContents, 3, 1);
      Thread.sleep(timeout);
      stm.write(fileContents, 4, 1);
      stm.hflush();

      stm.write(fileContents, 5, 1);
      Thread.sleep(timeout);
      stm.close();

      // verify that entire file is good
      AppendTestUtil.checkFullFile(fs, p, fileLen,
          fileContents, "Failed to slowly write to a file");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testHFlushInterrupted() throws Exception {
    final int DATANODE_NUM = 2;
    final int fileLen = 6;
    byte[] fileContents = AppendTestUtil.initBuffer(fileLen);
    Configuration conf = new HdfsConfiguration();
    final Path p = new Path("/hflush-interrupted");

    System.out.println("p=" + p);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM).build();
    try {
      DistributedFileSystem fs = cluster.getFileSystem();

      // create a new file.
      FSDataOutputStream stm = AppendTestUtil.createFile(fs, p, DATANODE_NUM);

      stm.write(fileContents, 0, 2);
      Thread.currentThread().interrupt();
      try {
        stm.hflush();
        // If we made it past the hflush(), then that means that the ack made it back
        // from the pipeline before we got to the wait() call. In that case we should
        // still have interrupted status.
        assertTrue(Thread.interrupted());
      } catch (InterruptedIOException ie) {
        System.out.println("Got expected exception during flush");
      }
      assertFalse(Thread.interrupted());

      // Try again to flush should succeed since we no longer have interrupt status
      stm.hflush();

      // Write some more data and flush
      stm.write(fileContents, 2, 2);
      stm.hflush();

      // Write some data and close while interrupted

      stm.write(fileContents, 4, 2);
      Thread.currentThread().interrupt();
      try {
        stm.close();
        // If we made it past the close(), then that means that the ack made it back
        // from the pipeline before we got to the wait() call. In that case we should
        // still have interrupted status.
        assertTrue(Thread.interrupted());
      } catch (InterruptedIOException ioe) {
        System.out.println("Got expected exception during close");
        // If we got the exception, we shouldn't have interrupted status anymore.
        assertFalse(Thread.interrupted());

        // Do the try-catch in the second stm.close() avoid that streamer was already
        // closed in other places.
        try {
          // Now do a successful close.
          stm.close();
        } catch (IOException e) {
          System.out.println("Got expected exception during second close");
        }
      }


      // verify that entire file is good
      AppendTestUtil.checkFullFile(fs, p, 4, fileContents,
          "Failed to deal with thread interruptions", false);
    } finally {
      cluster.shutdown();
    }
  }
}
