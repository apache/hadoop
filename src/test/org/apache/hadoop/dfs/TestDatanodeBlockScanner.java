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

package org.apache.hadoop.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.FSConstants.DatanodeReportType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import junit.framework.TestCase;

/**
 * This test verifies that block verification occurs on the datanode
 */
public class TestDatanodeBlockScanner extends TestCase {
  
  private static final Log LOG = 
                 LogFactory.getLog(TestDatanodeBlockScanner.class);
  
  private static String urlGet(URL url) {
    try {
      URLConnection conn = url.openConnection();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      IOUtils.copyBytes(conn.getInputStream(), out, 4096, true);
      return out.toString();
    } catch (IOException e) {
      LOG.warn("Failed to fetch " + url.toString() + " : " +
               e.getMessage());
    }
    return "";
  }

  private static Pattern pattern = 
             Pattern.compile(".*?(blk_[-]*\\d+).*?scan time\\s*:\\s*(\\d+)");
  /**
   * This connects to datanode and fetches block verification data.
   * It repeats this until the given block has a verification time > 0.
   */
  private static long waitForVerification(DatanodeInfo dn, FileSystem fs, 
                                          Path file) throws IOException {
    URL url = new URL("http://localhost:" + dn.getInfoPort() +
                      "/blockScannerReport?listblocks");
    long lastWarnTime = System.currentTimeMillis();
    long verificationTime = 0;
    
    String block = DFSTestUtil.getFirstBlock(fs, file).getBlockName();
    
    while (verificationTime <= 0) {
      String response = urlGet(url);
      for(Matcher matcher = pattern.matcher(response); matcher.find();) {
        if (block.equals(matcher.group(1))) {
          verificationTime = Long.parseLong(matcher.group(2));
          break;
        }
      }
      
      if (verificationTime <= 0) {
        long now = System.currentTimeMillis();
        if ((now - lastWarnTime) >= 5*1000) {
          LOG.info("Waiting for verification of " + block);
          lastWarnTime = now; 
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignored) {}
      }
    }
    
    return verificationTime;
  }

  public void testDatanodeBlockScanner() throws IOException {
    
    long startTime = System.currentTimeMillis();
    
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 1, true, null);
    cluster.waitActive();
    
    FileSystem fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    Path file2 = new Path("/tmp/testBlockVerification/file2");
    
    /*
     * Write the first file and restart the cluster.
     */
    DFSTestUtil.createFile(fs, file1, 10, (short)1, 0);
    cluster.shutdown();
    cluster = new MiniDFSCluster(conf, 1, false, null);
    cluster.waitActive();
    
    DFSClient dfsClient =  new DFSClient(new InetSocketAddress("localhost", 
                                         cluster.getNameNodePort()), conf);
    fs = cluster.getFileSystem();
    DatanodeInfo dn = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
    
    /*
     * The cluster restarted. The block should be verified by now.
     */
    assertTrue(waitForVerification(dn, fs, file1) > startTime);
    
    /*
     * Create a new file and read the block. The block should be marked 
     * verified since the client reads the block and verifies checksum. 
     */
    DFSTestUtil.createFile(fs, file2, 10, (short)1, 0);
    IOUtils.copyBytes(fs.open(file2), new IOUtils.NullOutputStream(), 
                      conf, true); 
    assertTrue(waitForVerification(dn, fs, file2) > startTime);
    
    cluster.shutdown();
  }

  void corruptReplica(String blockName, int replica) throws IOException {
    Random random = new Random();
    File baseDir = new File(System.getProperty("test.build.data"), "dfs/data");
    for (int i=replica*2; i<replica*2+2; i++) {
      File blockFile = new File(baseDir, "data" + (i+1)+ "/current/" + 
                               blockName);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int)channel.size()/2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }
    }
  }

  public void testBlockCorruptionPolicy() throws IOException {
    Configuration conf = new Configuration();
    Random random = new Random();
    FileSystem fs = null;
    DFSClient dfsClient = null;
    LocatedBlocks blocks = null;
    int blockCount = 0;

    MiniDFSCluster cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    Path file1 = new Path("/tmp/testBlockVerification/file1");
    DFSTestUtil.createFile(fs, file1, 1024, (short)3, 0);
    String block = DFSTestUtil.getFirstBlock(fs, file1).toString();
    
    dfsClient = new DFSClient(new InetSocketAddress("localhost", 
                                        cluster.getNameNodePort()), conf);
    blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    blockCount = blocks.get(0).getLocations().length;
    assertTrue(blockCount == 3);
    assertTrue(blocks.get(0).isCorrupt() == false);

    // Corrupt random replica of block 
    corruptReplica(block, random.nextInt(3));
    cluster.shutdown();

    // Restart the cluster hoping the corrupt block to be reported
    // We have 2 good replicas and block is not corrupt
    cluster = new MiniDFSCluster(conf, 3, false, null);
    cluster.waitActive();
    fs = cluster.getFileSystem();
    dfsClient = new DFSClient(new InetSocketAddress("localhost", 
                                        cluster.getNameNodePort()), conf);
    blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    blockCount = blocks.get(0).getLocations().length;
    assertTrue (blockCount == 2);
    assertTrue(blocks.get(0).isCorrupt() == false);
  
    // Corrupt all replicas. Now, block should be marked as corrupt
    // and we should get all the replicas 
    corruptReplica(block, 0);
    corruptReplica(block, 1);
    corruptReplica(block, 2);

    // Read the file to trigger reportBadBlocks by client
    try {
      IOUtils.copyBytes(fs.open(file1), new IOUtils.NullOutputStream(), 
                        conf, true);
    } catch (IOException e) {
      // Ignore exception
    }

    // We now have he blocks to be marked as corrup and we get back all
    // its replicas
    blocks = dfsClient.namenode.
                   getBlockLocations(file1.toString(), 0, Long.MAX_VALUE);
    blockCount = blocks.get(0).getLocations().length;
    assertTrue (blockCount == 3);
    assertTrue(blocks.get(0).isCorrupt() == true);

    cluster.shutdown();
  }
}
