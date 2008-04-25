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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol extends junit.framework.TestCase {
  public void testGetBlockMetaDataInfo() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filepath = "/foo";
      DFSTestUtil.createFile(dfs, new Path(filepath), 1024L, (short)3, 0L);
      assertTrue(dfs.dfs.exists(filepath));

      //get block info
      ClientProtocol namenode = dfs.dfs.namenode;
      LocatedBlocks locations = namenode.getBlockLocations(
          filepath, 0, Long.MAX_VALUE);
      List<LocatedBlock> blocks = locations.getLocatedBlocks();
      assertTrue(blocks.size() > 0);

      LocatedBlock locatedblock = blocks.get(0);
      DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      assertTrue(datanodeinfo.length > 0);

      //connect to a data node
      InterDatanodeProtocol idp = DataNode.createInterDataNodeProtocolProxy(
          datanodeinfo[0], conf);
      DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      assertTrue(datanode != null);
      
      //stop block scanner, so we could compare lastScanTime
      datanode.blockScannerThread.interrupt();

      //verify BlockMetaDataInfo
      Block b = locatedblock.getBlock();
      InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
      BlockMetaDataInfo metainfo = idp.getBlockMetaDataInfo(b);
      assertEquals(b.getBlockId(), metainfo.getBlockId());
      assertEquals(b.getNumBytes(), metainfo.getNumBytes());
      assertEquals(datanode.blockScanner.getLastScanTime(b),
          metainfo.getLastScanTime());

      //TODO: verify GenerationStamp
      InterDatanodeProtocol.LOG.info("idp.updateGenerationStamp="
          + idp.updateGenerationStamp(b, new GenerationStamp(456789L)));
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
}