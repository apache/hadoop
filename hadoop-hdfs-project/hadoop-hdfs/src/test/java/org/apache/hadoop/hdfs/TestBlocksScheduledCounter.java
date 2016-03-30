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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.junit.After;
import org.junit.Test;

/**
 * This class tests DatanodeDescriptor.getBlocksScheduled() at the
 * NameNode. This counter is supposed to keep track of blocks currently
 * scheduled to a datanode.
 */
public class TestBlocksScheduledCounter {
  MiniDFSCluster cluster = null;
  FileSystem fs = null;

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if(cluster!=null){
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testBlocksScheduledCounter() throws IOException {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).build();

    cluster.waitActive();
    fs = cluster.getFileSystem();
    
    //open a file an write a few bytes:
    FSDataOutputStream out = fs.create(new Path("/testBlockScheduledCounter"));
    for (int i=0; i<1024; i++) {
      out.write(i);
    }
    // flush to make sure a block is allocated.
    out.hflush();
    
    ArrayList<DatanodeDescriptor> dnList = new ArrayList<DatanodeDescriptor>();
    final DatanodeManager dm = cluster.getNamesystem().getBlockManager(
        ).getDatanodeManager();
    dm.fetchDatanodes(dnList, dnList, false);
    DatanodeDescriptor dn = dnList.get(0);
    
    assertEquals(1, dn.getBlocksScheduled());
   
    // close the file and the counter should go to zero.
    out.close();   
    assertEquals(0, dn.getBlocksScheduled());
  }

  /**
   * Abandon block should decrement the scheduledBlocks count for the dataNode.
   */
  @Test
  public void testScheduledBlocksCounterShouldDecrementOnAbandonBlock()
      throws Exception {
    cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).numDataNodes(
        2).build();

    cluster.waitActive();
    fs = cluster.getFileSystem();

    DatanodeManager datanodeManager = cluster.getNamesystem().getBlockManager()
        .getDatanodeManager();
    ArrayList<DatanodeDescriptor> dnList = new ArrayList<DatanodeDescriptor>();
    datanodeManager.fetchDatanodes(dnList, dnList, false);
    for (DatanodeDescriptor descriptor : dnList) {
      assertEquals("Blocks scheduled should be 0 for " + descriptor.getName(),
          0, descriptor.getBlocksScheduled());
    }

    cluster.getDataNodes().get(0).shutdown();
    // open a file an write a few bytes:
    FSDataOutputStream out = fs.create(new Path("/testBlockScheduledCounter"),
        (short) 2);
    for (int i = 0; i < 1024; i++) {
      out.write(i);
    }
    // flush to make sure a block is allocated.
    out.hflush();

    DatanodeDescriptor abandonedDn = datanodeManager.getDatanode(cluster
        .getDataNodes().get(0).getDatanodeId());
    assertEquals("for the abandoned dn scheduled counts should be 0", 0,
        abandonedDn.getBlocksScheduled());

    for (DatanodeDescriptor descriptor : dnList) {
      if (descriptor.equals(abandonedDn)) {
        continue;
      }
      assertEquals("Blocks scheduled should be 1 for " + descriptor.getName(),
          1, descriptor.getBlocksScheduled());
    }
    // close the file and the counter should go to zero.
    out.close();
    for (DatanodeDescriptor descriptor : dnList) {
      assertEquals("Blocks scheduled should be 0 for " + descriptor.getName(),
          0, descriptor.getBlocksScheduled());
    }
  }
}
