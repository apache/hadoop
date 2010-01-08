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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.security.MessageDigest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.*;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.AccessControlException;

import junit.framework.TestCase;


/**
 * These tests make sure that DFSClient retries fetching data from DFS
 * properly in case of errors.
 */
public class TestDFSClientRetries extends TestCase {
  public static final Log LOG =
    LogFactory.getLog(TestDFSClientRetries.class.getName());
  
  // writes 'len' bytes of data to out.
  private static void writeData(OutputStream out, int len) throws IOException {
    byte [] buf = new byte[4096*16];
    while(len > 0) {
      int toWrite = Math.min(len, buf.length);
      out.write(buf, 0, toWrite);
      len -= toWrite;
    }
  }
  
  /**
   * This makes sure that when DN closes clients socket after client had
   * successfully connected earlier, the data can still be fetched.
   */
  public void testWriteTimeoutAtDataNode() throws IOException,
                                                  InterruptedException { 
    Configuration conf = new HdfsConfiguration();
    
    final int writeTimeout = 100; //milliseconds.
    // set a very short write timeout for datanode, so that tests runs fast.
    conf.setInt("dfs.datanode.socket.write.timeout", writeTimeout); 
    // set a smaller block size
    final int blockSize = 10*1024*1024;
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt("dfs.client.max.block.acquire.failures", 1);
    // set a small buffer size
    final int bufferSize = 4096;
    conf.setInt("io.file.buffer.size", bufferSize);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    
    try {
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
    
      Path filePath = new Path("/testWriteTimeoutAtDataNode");
      OutputStream out = fs.create(filePath, true, bufferSize);
    
      // write a 2 block file.
      writeData(out, 2*blockSize);
      out.close();
      
      byte[] buf = new byte[1024*1024]; // enough to empty TCP buffers.
      
      InputStream in = fs.open(filePath, bufferSize);
      
      //first read a few bytes
      IOUtils.readFully(in, buf, 0, bufferSize/2);
      //now read few more chunks of data by sleeping in between :
      for(int i=0; i<10; i++) {
        Thread.sleep(2*writeTimeout); // force write timeout at the datanode.
        // read enough to empty out socket buffers.
        IOUtils.readFully(in, buf, 0, buf.length); 
      }
      // successfully read with write timeout on datanodes.
      in.close();
    } finally {
      cluster.shutdown();
    }
  }
  
  // more tests related to different failure cases can be added here.
  
  class TestNameNode implements ClientProtocol
  {
    int num_calls = 0;
    
    // The total number of calls that can be made to addBlock
    // before an exception is thrown
    int num_calls_allowed; 
    public final String ADD_BLOCK_EXCEPTION = "Testing exception thrown from"
                                             + "TestDFSClientRetries::"
                                             + "TestNameNode::addBlock";
    public final String RETRY_CONFIG
          = "dfs.client.block.write.locateFollowingBlock.retries";
          
    public TestNameNode(Configuration conf) throws IOException
    {
      // +1 because the configuration value is the number of retries and
      // the first call is not a retry (e.g., 2 retries == 3 total
      // calls allowed)
      this.num_calls_allowed = conf.getInt(RETRY_CONFIG, 5) + 1;
    }

    public long getProtocolVersion(String protocol, 
                                     long clientVersion)
    throws IOException
    {
      return versionID;
    }

    public LocatedBlock addBlock(String src, String clientName, Block previous)
    throws IOException
    {
      num_calls++;
      if (num_calls > num_calls_allowed) { 
        throw new IOException("addBlock called more times than "
                              + RETRY_CONFIG
                              + " allows.");
      } else {
          throw new RemoteException(NotReplicatedYetException.class.getName(),
                                    ADD_BLOCK_EXCEPTION);
      }
    }
    
    
    // The following methods are stub methods that are not needed by this mock class
    
    public LocatedBlocks  getBlockLocations(String src, long offset, long length) throws IOException { return null; }
    
    public FsServerDefaults getServerDefaults() throws IOException { return null; }
    
    public void create(String src, FsPermission masked, String clientName, EnumSetWritable<CreateFlag> flag, boolean createParent, short replication, long blockSize) throws IOException {}
    
    public LocatedBlock append(String src, String clientName) throws IOException { return null; }

    public boolean setReplication(String src, short replication) throws IOException { return false; }

    public void setPermission(String src, FsPermission permission) throws IOException {}

    public void setOwner(String src, String username, String groupname) throws IOException {}

    public void abandonBlock(Block b, String src, String holder) throws IOException {}

    public boolean complete(String src, String clientName, Block last) throws IOException { return false; }

    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {}

    @Deprecated
    public boolean rename(String src, String dst) throws IOException { return false; }
    
    public void concat(String trg, String[] srcs) throws IOException {  }

    public void rename(String src, String dst, Rename... options) throws IOException { }

    public boolean delete(String src) throws IOException { return false; }

    public boolean delete(String src, boolean recursive) throws IOException { return false; }

    public boolean mkdirs(String src, FsPermission masked, boolean createParent) throws IOException { return false; }

    public FileStatus[] getListing(String src) throws IOException { return null; }

    public void renewLease(String clientName) throws IOException {}

    public long[] getStats() throws IOException { return null; }

    public DatanodeInfo[] getDatanodeReport(FSConstants.DatanodeReportType type) throws IOException { return null; }

    public long getPreferredBlockSize(String filename) throws IOException { return 0; }

    public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException { return false; }

    public void saveNamespace() throws IOException {}

    public boolean restoreFailedStorage(String arg) throws AccessControlException { return false; }

    public void refreshNodes() throws IOException {}

    public void finalizeUpgrade() throws IOException {}

    public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException { return null; }

    public void metaSave(String filename) throws IOException {}

    public FileStatus getFileInfo(String src) throws IOException { return null; }

    public ContentSummary getContentSummary(String path) throws IOException { return null; }

    public void setQuota(String path, long namespaceQuota, long diskspaceQuota) throws IOException {}

    public void fsync(String src, String client) throws IOException {}

    public void setTimes(String src, long mtime, long atime) throws IOException {}

    @Override public LocatedBlock updateBlockForPipeline(Block block, 
        String clientName) throws IOException { return null; }

    @Override public void updatePipeline(String clientName, Block oldblock,
        Block newBlock, DatanodeID[] newNodes)
        throws IOException {}
  }
  
  public void testNotYetReplicatedErrors() throws IOException
  {   
    Configuration conf = new HdfsConfiguration();
    
    // allow 1 retry (2 total calls)
    conf.setInt("dfs.client.block.write.locateFollowingBlock.retries", 1);
        
    TestNameNode tnn = new TestNameNode(conf);
    final DFSClient client = new DFSClient(null, tnn, conf, null);
    OutputStream os = client.create("testfile", true);
    os.write(20); // write one random byte
    
    try {
      os.close();
    } catch (Exception e) {
      assertTrue("Retries are not being stopped correctly",
           e.getMessage().equals(tnn.ADD_BLOCK_EXCEPTION));
    }
  }
  
  
  /**
   * Test that a DFSClient waits for random time before retry on busy blocks.
   */
  public void testDFSClientRetriesOnBusyBlocks() throws IOException {
    
    System.out.println("Testing DFSClient random waiting on busy blocks.");
    
    //
    // Test settings: 
    // 
    //           xcievers    fileLen   #clients  timeWindow    #retries
    //           ========    =======   ========  ==========    ========
    // Test 1:          2       6 MB         50      300 ms           3
    // Test 2:          2       6 MB         50      300 ms          50
    // Test 3:          2       6 MB         50     1000 ms           3
    // Test 4:          2       6 MB         50     1000 ms          50
    // 
    //   Minimum xcievers is 2 since 1 thread is reserved for registry.
    //   Test 1 & 3 may fail since # retries is low. 
    //   Test 2 & 4 should never fail since (#threads)/(xcievers-1) is the upper
    //   bound for guarantee to not throw BlockMissingException.
    //
    int xcievers  = 2;
    int fileLen   = 6*1024*1024;
    int threads   = 50;
    int retries   = 3;
    int timeWin   = 300;
    
    //
    // Test 1: might fail
    // 
    long timestamp = System.currentTimeMillis();
    boolean pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    long timestamp2 = System.currentTimeMillis();
    if ( pass ) {
      LOG.info("Test 1 succeeded! Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    } else {
      LOG.warn("Test 1 failed, but relax. Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    }
    
    //
    // Test 2: should never fail
    // 
    retries = 50;
    timestamp = System.currentTimeMillis();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = System.currentTimeMillis();
    assertTrue("Something wrong! Test 2 got Exception with maxmum retries!", pass);
    LOG.info("Test 2 succeeded! Time spent: "  + (timestamp2-timestamp)/1000.0 + " sec.");
    
    //
    // Test 3: might fail
    // 
    retries = 3;
    timeWin = 1000;
    timestamp = System.currentTimeMillis();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = System.currentTimeMillis();
    if ( pass ) {
      LOG.info("Test 3 succeeded! Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    } else {
      LOG.warn("Test 3 failed, but relax. Time spent: " + (timestamp2-timestamp)/1000.0 + " sec.");
    }
    
    //
    // Test 4: should never fail
    //
    retries = 50;
    timeWin = 1000;
    timestamp = System.currentTimeMillis();
    pass = busyTest(xcievers, threads, fileLen, timeWin, retries);
    timestamp2 = System.currentTimeMillis();
    assertTrue("Something wrong! Test 4 got Exception with maxmum retries!", pass);
    LOG.info("Test 4 succeeded! Time spent: "  + (timestamp2-timestamp)/1000.0 + " sec.");
  }

  private boolean busyTest(int xcievers, int threads, int fileLen, int timeWin, int retries) 
    throws IOException {

    boolean ret = true;
    short replicationFactor = 1;
    long blockSize = 128*1024*1024; // DFS block size
    int bufferSize = 4096;
    
    Configuration conf = new HdfsConfiguration();
    conf.setInt("dfs.datanode.max.xcievers",xcievers);
    conf.setInt("dfs.client.max.block.acquire.failures", retries);
    conf.setInt("dfs.client.retry.window.base", timeWin);

    MiniDFSCluster cluster = new MiniDFSCluster(conf, replicationFactor, true, null);
    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("test_data.dat");
    file1 = file1.makeQualified(fs.getUri(), fs.getWorkingDirectory()); // make URI hdfs://
    
    try {
      
      FSDataOutputStream stm = fs.create(file1, true,
                                         bufferSize,
                                         replicationFactor,
                                         blockSize);
      
      // verify that file exists in FS namespace
      assertTrue(file1 + " should be a file", 
                  fs.getFileStatus(file1).isDir() == false);
      System.out.println("Path : \"" + file1 + "\"");
      LOG.info("Path : \"" + file1 + "\"");

      // write 1 block to file
      byte[] buffer = AppendTestUtil.randomBytes(System.currentTimeMillis(), fileLen);
      stm.write(buffer, 0, fileLen);
      stm.close();

      // verify that file size has changed to the full size
      long len = fs.getFileStatus(file1).getLen();
      
      assertTrue(file1 + " should be of size " + fileLen +
                 " but found to be of size " + len, 
                  len == fileLen);
      
      // read back and check data integrigy
      byte[] read_buf = new byte[fileLen];
      InputStream in = fs.open(file1, fileLen);
      IOUtils.readFully(in, read_buf, 0, fileLen);
      assert(Arrays.equals(buffer, read_buf));
      in.close();
      read_buf = null; // GC it if needed
      
      // compute digest of the content to reduce memory space
      MessageDigest m = MessageDigest.getInstance("SHA");
      m.update(buffer, 0, fileLen);
      byte[] hash_sha = m.digest();

      // spawn multiple threads and all trying to access the same block
      Thread[] readers = new Thread[threads];
      Counter counter = new Counter(0);
      for (int i = 0; i < threads; ++i ) {
        DFSClientReader reader = new DFSClientReader(file1, cluster, hash_sha, fileLen, counter);
        readers[i] = new Thread(reader);
        readers[i].start();
      }
      
      // wait for them to exit
      for (int i = 0; i < threads; ++i ) {
        readers[i].join();
      }
      if ( counter.get() == threads )
        ret = true;
      else
        ret = false;
      
    } catch (InterruptedException e) {
      System.out.println("Thread got InterruptedException.");
      e.printStackTrace();
      ret = false;
    } catch (Exception e) {
      e.printStackTrace();
      ret = false;
    } finally {
      fs.delete(file1, false);
      cluster.shutdown();
    }
    return ret;
  }
  
  class DFSClientReader implements Runnable {
    
    DFSClient client;
    Configuration conf;
    byte[] expected_sha;
    FileSystem  fs;
    Path filePath;
    MiniDFSCluster cluster;
    int len;
    Counter counter;

    DFSClientReader(Path file, MiniDFSCluster cluster, byte[] hash_sha, int fileLen, Counter cnt) {
      filePath = file;
      this.cluster = cluster;
      counter = cnt;
      len = fileLen;
      conf = new HdfsConfiguration();
      expected_sha = hash_sha;
      try {
        cluster.waitActive();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    public void run() {
      try {
        fs = cluster.getNewFileSystemInstance();
        
        int bufferSize = len;
        byte[] buf = new byte[bufferSize];

        InputStream in = fs.open(filePath, bufferSize);
        
        // read the whole file
        IOUtils.readFully(in, buf, 0, bufferSize);
        
        // compare with the expected input
        MessageDigest m = MessageDigest.getInstance("SHA");
        m.update(buf, 0, bufferSize);
        byte[] hash_sha = m.digest();
        
        buf = null; // GC if needed since there may be too many threads
        in.close();
        fs.close();

        assertTrue("hashed keys are not the same size",
                   hash_sha.length == expected_sha.length);

        assertTrue("hashed keys are not equal",
                   Arrays.equals(hash_sha, expected_sha));
        
        counter.inc(); // count this thread as successful
        
        LOG.info("Thread correctly read the block.");
        
      } catch (BlockMissingException e) {
        LOG.info("Bad - BlockMissingException is caught.");
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      } 
    }
  }

  class Counter {
    int counter;
    Counter(int n) { counter = n; }
    public synchronized void inc() { ++counter; }
    public int get() { return counter; }
  }
}
