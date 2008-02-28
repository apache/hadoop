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
    
    String block = DFSTestUtil.getFirstBlock(fs, file).toString();
    
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
}
