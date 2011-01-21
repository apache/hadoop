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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
    Configuration conf = new Configuration();
    
    final int writeTimeout = 100; //milliseconds.
    // set a very short write timeout for datanode, so that tests runs fast.
    conf.setInt("dfs.datanode.socket.write.timeout", writeTimeout); 
    // set a smaller block size
    final int blockSize = 10*1024*1024;
    conf.setInt("dfs.block.size", blockSize);
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

    public LocatedBlock addBlock(String src, String clientName)
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

    public void create(String src, FsPermission masked, String clientName, boolean overwrite, short replication, long blockSize) throws IOException {}

    public LocatedBlock append(String src, String clientName) throws IOException { return null; }

    public boolean setReplication(String src, short replication) throws IOException { return false; }

    public void setPermission(String src, FsPermission permission) throws IOException {}

    public void setOwner(String src, String username, String groupname) throws IOException {}

    public void abandonBlock(Block b, String src, String holder) throws IOException {}

    public boolean complete(String src, String clientName) throws IOException { return false; }

    public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {}

    public boolean rename(String src, String dst) throws IOException { return false; }

    public boolean delete(String src) throws IOException { return false; }

    public boolean delete(String src, boolean recursive) throws IOException { return false; }

    public boolean mkdirs(String src, FsPermission masked) throws IOException { return false; }

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

  }
  
  public void testNotYetReplicatedErrors() throws IOException
  {   
    Configuration conf = new Configuration();
    
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
  
}
