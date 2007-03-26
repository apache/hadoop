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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.DNS;

/**
 * This class tests the replication of a DFS file.
 * @author Milind Bhandarkar, Hairong Kuang
 */
public class TestReplication extends TestCase {
  private static final long seed = 0xDEADBEEFL;
  private static final int blockSize = 8192;
  private static final int fileSize = 16384;
  private static final String racks[] = new String[] {
    "/d1/r1", "/d1/r1", "/d1/r2", "/d1/r2", "/d1/r2", "/d2/r3", "/d2/r3"
  };
  private static final int numDatanodes = racks.length;
  private static final Log LOG = LogFactory.getLog(
          "org.apache.hadoop.dfs.TestReplication");

  private void writeFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
    // create and write a file that contains three blocks of data
    FSDataOutputStream stm = fileSys.create(name, true,
            fileSys.getConf().getInt("io.file.buffer.size", 4096),
            (short)repl, (long)blockSize);
    byte[] buffer = new byte[fileSize];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    stm.close();
  }
  
  /* check if there are at least two nodes are on the same rack */
  private void checkFile(FileSystem fileSys, Path name, int repl)
  throws IOException {
      Configuration conf = fileSys.getConf();
      ClientProtocol namenode = (ClientProtocol) RPC.getProxy(
              ClientProtocol.class,
              ClientProtocol.versionID,
              DataNode.createSocketAddr(conf.get("fs.default.name")), 
              conf);
      
      LocatedBlock[] locations;
      boolean isReplicationDone;
      do {
          locations = namenode.open(name.toString());
          isReplicationDone = true;
          for (int idx = 0; idx < locations.length; idx++) {
              DatanodeInfo[] datanodes = locations[idx].getLocations();
              if(Math.min(numDatanodes, repl) != datanodes.length) {
                  isReplicationDone=false;
                  LOG.warn("File has "+datanodes.length+" replicas, expecting "
                          +Math.min(numDatanodes, repl));
                  try {
                      Thread.sleep(15000L);
                 } catch (InterruptedException e) {
                      // nothing
                 }
                 break;
              }
          }
      } while(!isReplicationDone);
      
      boolean isOnSameRack = true, isNotOnSameRack = true;
      for (int idx = 0; idx < locations.length; idx++) {
          DatanodeInfo[] datanodes = locations[idx].getLocations();
          if(datanodes.length <= 1) break;
          if(datanodes.length == 2) {
              isNotOnSameRack = !( datanodes[0].getNetworkLocation().equals(
                      datanodes[1].getNetworkLocation() ) );
              break;
          }
          isOnSameRack = false;
          isNotOnSameRack = false;
          for (int idy = 0; idy < datanodes.length-1; idy++) {
                  LOG.info("datanode "+ idy + ": "+ datanodes[idy].getName());
                  boolean onRack = datanodes[idy].getNetworkLocation().equals(
                          datanodes[idy+1].getNetworkLocation() );
                  if( onRack ) {
                      isOnSameRack = true;
                  }
                  if( !onRack ) {
                      isNotOnSameRack = true;                      
                  }
                  if( isOnSameRack && isNotOnSameRack ) break;
          }
          if( !isOnSameRack || !isNotOnSameRack ) break;
      }
      assertTrue(isOnSameRack);
      assertTrue(isNotOnSameRack);
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
    conf.setBoolean("dfs.replication.considerLoad", false);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, numDatanodes, true, racks);
    cluster.waitActive();
    
    InetSocketAddress addr = new InetSocketAddress("localhost",
            cluster.getNameNodePort());
    DFSClient client = new DFSClient(addr, conf);
    
    DatanodeInfo[] info = client.datanodeReport();
    assertEquals("Number of Datanodes ", numDatanodes, info.length);
    FileSystem fileSys = cluster.getFileSystem();
    try {
      Path file1 = new Path("/smallblocktest.dat");
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
      writeFile(fileSys, file1, 2);
      checkFile(fileSys, file1, 2);
      cleanupFile(fileSys, file1);
    } finally {
      fileSys.close();
      cluster.shutdown();
    }
  }
}
