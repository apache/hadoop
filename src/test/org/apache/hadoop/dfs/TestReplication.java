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

import junit.framework.TestCase;
import java.io.*;
import java.util.Random;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class tests the replication of a DFS file.
 * @author Milind Bhandarkar
 */
public class TestReplication extends TestCase {
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int numDatanodes = 4;

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    // create and write a file that contains three blocks of data
    FSOutputStream stm = fileSys.createRaw(name, true, (short)repl,
        (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  
  private void checkFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    String[][] locations = fileSys.getFileCacheHints(name, 0, fileSize);
    for (int idx = 0; idx < locations.length; idx++) {
      assertEquals("Number of replicas for block" + idx,
          Math.min(numDatanodes, repl), locations[idx].length);  
    }
  }
  
  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name);
    assertTrue(!fileSys.exists(name));
  }
  
  /**
   * Tests replication in DFS.
   */
  public void testReplication() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(65312, conf, numDatanodes, false);
    // Now wait for 15 seconds to give datanodes chance to register
    // themselves and to report heartbeat
    try {
      Thread.sleep(15000L);
    } catch (InterruptedException e) {
      // nothing
    }
    InetSocketAddress addr = new InetSocketAddress("localhost", 65312);
    DFSClient client = new DFSClient(addr, conf);
    DatanodeInfo[] info = client.datanodeReport();
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("smallblocktest.dat");
      writeFile(fileSys, file1, 3);
      checkFile(fileSys, file1, 3);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 10);
      checkFile(fileSys, file1, 10);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 4);
      checkFile(fileSys, file1, 4);
      cleanupFile(fileSys, file1);
      writeFile(fileSys, file1, 1);
      checkFile(fileSys, file1, 1);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
