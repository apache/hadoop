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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Assert;
import org.junit.Test;


/**
 * Ensure during large directory delete, namenode does not block until the 
 * deletion completes and handles new requests from other clients
 */
public class TestLargeDirectoryDelete {
  private static final Log LOG = LogFactory.getLog(TestLargeDirectoryDelete.class);
  private static final Configuration CONF = new HdfsConfiguration();
  private static final int TOTAL_BLOCKS = 10000;
  private MiniDFSCluster mc = null;
  private int createOps = 0;
  private int lockOps = 0;
  
  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
  }
  
  /** create a file with a length of <code>filelen</code> */
  private void createFile(final String fileName, final long filelen) throws IOException {
    FileSystem fs = mc.getFileSystem();
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, filelen, (short) 1, 0);
  }
  
  /** Create a large number of directories and files */
  private void createFiles() throws IOException {
    Random rand = new Random();
    // Create files in a directory with random depth
    // ranging from 0-10.
    for (int i = 0; i < TOTAL_BLOCKS; i+=100) {
      String filename = "/root/";
      int dirs = rand.nextInt(10);  // Depth of the directory
      for (int j=i; j >=(i-dirs); j--) {
        filename += j + "/";
      }
      filename += "file" + i;
      createFile(filename, 100);
    }
  }
  
  private int getBlockCount() {
    return (int)mc.getNamesystem().getBlocksTotal();
  }

  /** Run multiple threads doing simultaneous operations on the namenode
   * while a large directory is being deleted.
   */
  private void runThreads() throws IOException {
    final Thread threads[] = new Thread[2];
    
    // Thread for creating files
    threads[0] = new Thread() {
      @Override
      public void run() {
        while(true) {
          try {
            int blockcount = getBlockCount();
            if (blockcount < TOTAL_BLOCKS && blockcount > 0) {
              String file = "/tmp" + createOps;
              createFile(file, 1);
              mc.getFileSystem().delete(new Path(file), true);
              createOps++;
            }
          } catch (IOException ex) {
            LOG.info("createFile exception ", ex);
            break;
          }
        }
      }
    };
    
    // Thread that periodically acquires the FSNamesystem lock
    threads[1] = new Thread() {
      @Override
      public void run() {
        while(true) {
          try {
            int blockcount = getBlockCount();
            if (blockcount < TOTAL_BLOCKS && blockcount > 0) {
              synchronized(mc.getNamesystem()) {
                lockOps++;
              }
              Thread.sleep(1);
            }
          } catch (InterruptedException ex) {
            LOG.info("lockOperation exception ", ex);
            break;
          }
        }
      }
    };
    threads[0].start();
    threads[1].start();
    
    final long start = System.currentTimeMillis();
    FSNamesystem.BLOCK_DELETION_INCREMENT = 1;
    mc.getFileSystem().delete(new Path("/root"), true); // recursive delete
    final long end = System.currentTimeMillis();
    threads[0].interrupt();
    threads[1].interrupt();
    LOG.info("Deletion took " + (end - start) + "msecs");
    LOG.info("createOperations " + createOps);
    LOG.info("lockOperations " + lockOps);
    Assert.assertTrue(lockOps + createOps > 0);
  }
  
  @Test
  public void largeDelete() throws IOException, InterruptedException {
    mc = new MiniDFSCluster(CONF, 1, true, null);
    try {
      mc.waitActive();
      createFiles();
      Assert.assertEquals(TOTAL_BLOCKS, getBlockCount());
      runThreads();
    } finally {
      mc.shutdown();
    }
  }
}
