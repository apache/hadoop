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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import java.io.IOException;

/** Class contains a set of tests to verify the correctness of 
 * newly introduced {@link FSDataOutputStream#hflush()} method */
public class TestHFlush {
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private final String fName = "hflushtest.dat";
  
  /** The test uses {@link #doTheJob(Configuration, String, long, short)
   * to write a file with a standard block size
   */
  @Test
  public void hFlush_01() throws IOException {
    doTheJob(new HdfsConfiguration(), fName, AppendTestUtil.BLOCK_SIZE, (short)2);
  }

  /** The test uses {@link #doTheJob(Configuration, String, long, short)
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

    doTheJob(conf, fName, customBlockSize, (short)2);
  }

  /** The test uses {@link #doTheJob(Configuration, String, long, short)
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

    doTheJob(conf, fName, customBlockSize, (short)2);
  }

  /**
    The method starts new cluster with defined Configuration;
    creates a file with specified block_size and writes 10 equal sections in it;
    it also calls hflush() after each write and throws an IOException in case of 
    an error.
    @param conf cluster configuration
    @param fileName of the file to be created and processed as required
    @param block_size value to be used for the file's creation
    @param replicas is the number of replicas
    @throws IOException in case of any errors 
   */
  public static void doTheJob(Configuration conf, final String fileName,
                              long block_size, short replicas) throws IOException {
    byte[] fileContent;
    final int SECTIONS = 10;

    fileContent = AppendTestUtil.initBuffer(AppendTestUtil.FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, replicas, true, null);
    // Make sure we work with DFS in order to utilize all its functionality
    DistributedFileSystem fileSystem =
        (DistributedFileSystem)cluster.getFileSystem();

    FSDataInputStream is;
    try {
      Path path = new Path(fileName);
      FSDataOutputStream stm = fileSystem.create(path, false, 4096, replicas,
          block_size);
      System.out.println("Created file " + fileName);

      int tenth = AppendTestUtil.FILE_SIZE/SECTIONS;
      int rounding = AppendTestUtil.FILE_SIZE - tenth * SECTIONS;
      for (int i=0; i<SECTIONS; i++) {
        System.out.println("Writing " + (tenth * i) + " to " + (tenth * (i+1)) + " section to file " + fileName);
        // write to the file
        stm.write(fileContent, tenth * i, tenth);
        // Wait while hflush() pushes all packets through built pipeline
        ((DFSOutputStream)stm.getWrappedStream()).hflush();
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
    } catch (Exception e) {
      e.printStackTrace();
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
    conf.setInt(DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 
        timeout);

    final Path p = new Path("/pipelineHeartbeat/foo");
    System.out.println("p=" + p);
    
    MiniDFSCluster cluster = new MiniDFSCluster(conf, DATANODE_NUM, true, null);
    DistributedFileSystem fs = (DistributedFileSystem)cluster.getFileSystem();

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
  }
}
