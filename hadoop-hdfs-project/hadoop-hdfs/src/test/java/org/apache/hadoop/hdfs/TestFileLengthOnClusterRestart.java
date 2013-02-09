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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.junit.Assert;
import org.junit.Test;

/** Test the fileLength on cluster restarts */
public class TestFileLengthOnClusterRestart {
  /**
   * Tests the fileLength when we sync the file and restart the cluster and
   * Datanodes not report to Namenode yet.
   */
  @Test(timeout = 60000)
  public void testFileLengthWithHSyncAndClusterRestartWithOutDNsRegister()
      throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // create cluster
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);

    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(2).build();
    HdfsDataInputStream in = null;
    try {
      Path path = new Path("/tmp/TestFileLengthOnClusterRestart", "test");
      DistributedFileSystem dfs = (DistributedFileSystem) cluster
          .getFileSystem();
      FSDataOutputStream out = dfs.create(path);
      int fileLength = 1030;
      out.write(new byte[fileLength]);
      out.hsync();
      cluster.restartNameNode();
      cluster.waitActive();
      in = (HdfsDataInputStream) dfs.open(path, 1024);
      // Verify the length when we just restart NN. DNs will register
      // immediately.
      Assert.assertEquals(fileLength, in.getVisibleLength());
      cluster.shutdownDataNodes();
      cluster.restartNameNode(false);
      // This is just for ensuring NN started.
      verifyNNIsInSafeMode(dfs);

      try {
        in = (HdfsDataInputStream) dfs.open(path);
        Assert.fail("Expected IOException");
      } catch (IOException e) {
        Assert.assertTrue(e.getLocalizedMessage().indexOf(
            "Name node is in safe mode") >= 0);
      }
    } finally {
      if (null != in) {
        in.close();
      }
      cluster.shutdown();

    }
  }

  private void verifyNNIsInSafeMode(DistributedFileSystem dfs)
      throws IOException {
    while (true) {
      try {
        if (dfs.isInSafeMode()) {
          return;
        } else {
          throw new IOException("Expected to be in SafeMode");
        }
      } catch (IOException e) {
        // NN might not started completely Ignore
      }
    }
  }
}
