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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;


/**
 * Ensure during large directory delete, namenode does not block until the 
 * deletion completes and handles new requests from other clients
 */
public class TestLargeDirectoryDelete {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestLargeDirectoryDelete.class);
  private static final Configuration CONF = new HdfsConfiguration();
  private static final int TOTAL_BLOCKS = 10000;
  private MiniDFSCluster mc = null;
  private int createOps = 0;
  private int lockOps = 0;
  
  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_BLOCK_DELETION_INCREMENT_KEY, 1);
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
    Assert.assertNotNull("Null cluster", mc);
    Assert.assertNotNull("No Namenode in cluster", mc.getNameNode());
    FSNamesystem namesystem = mc.getNamesystem();
    Assert.assertNotNull("Null Namesystem in cluster", namesystem);
    Assert.assertNotNull("Null Namesystem.blockmanager", namesystem.getBlockManager());
    return (int) namesystem.getBlocksTotal();
  }

  /** Run multiple threads doing simultaneous operations on the namenode
   * while a large directory is being deleted.
   */
  private void runThreads() throws Throwable {
    final TestThread threads[] = new TestThread[2];
    
    // Thread for creating files
    threads[0] = new TestThread() {
      @Override
      protected void execute() throws Throwable {
        while(live) {
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
    threads[1] = new TestThread() {
      @Override
      protected void execute() throws Throwable {
        while(live) {
          try {
            int blockcount = getBlockCount();
            if (blockcount < TOTAL_BLOCKS && blockcount > 0) {
              mc.getNamesystem().writeLock();
              try {
                lockOps++;
              } finally {
                mc.getNamesystem().writeUnlock();
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
    
    final long start = Time.now();
    mc.getFileSystem().delete(new Path("/root"), true); // recursive delete
    final long end = Time.now();
    threads[0].endThread();
    threads[1].endThread();
    LOG.info("Deletion took " + (end - start) + "msecs");
    LOG.info("createOperations " + createOps);
    LOG.info("lockOperations " + lockOps);
    Assert.assertTrue(lockOps + createOps > 0);
    threads[0].rethrow();
    threads[1].rethrow();
  }


  /**
   * An abstract class for tests that catches exceptions and can 
   * rethrow them on a different thread, and has an {@link #endThread()} 
   * operation that flips a volatile boolean before interrupting the thread.
   * Also: after running the implementation of {@link #execute()} in the 
   * implementation class, the thread is notified: other threads can wait
   * for it to terminate
   */
  private abstract class TestThread extends Thread {
    volatile Throwable thrown;
    protected volatile boolean live = true;

    @Override
    public void run() {
      try {
        execute();
      } catch (Throwable throwable) {
        LOG.warn("{}", throwable);
        setThrown(throwable);
      } finally {
        synchronized (this) {
          this.notify();
        }
      }
    }

    protected abstract void execute() throws Throwable;

    protected synchronized void setThrown(Throwable thrown) {
      this.thrown = thrown;
    }

    /**
     * Rethrow anything caught
     * @throws Throwable any non-null throwable raised by the execute method.
     */
    public synchronized void rethrow() throws Throwable {
      if (thrown != null) {
        throw thrown;
      }
    }

    /**
     * End the thread by setting the live p
     */
    public synchronized void endThread() {
      live = false;
      interrupt();
      try {
        wait();
      } catch (InterruptedException e) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Ignoring " + e, e);
        }
      }
    }
  }
  
  @Test
  public void largeDelete() throws Throwable {
    mc = new MiniDFSCluster.Builder(CONF).build();
    try {
      mc.waitActive();
      Assert.assertNotNull("No Namenode in cluster", mc.getNameNode());
      createFiles();
      Assert.assertEquals(TOTAL_BLOCKS, getBlockCount());
      runThreads();
    } finally {
      mc.shutdown();
    }
  }
}
