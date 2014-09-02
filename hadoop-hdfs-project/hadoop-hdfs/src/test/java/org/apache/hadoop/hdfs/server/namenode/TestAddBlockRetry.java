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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Race between two threads simultaneously calling
 * FSNamesystem.getAdditionalBlock().
 */
public class TestAddBlockRetry {
  public static final Log LOG = LogFactory.getLog(TestAddBlockRetry.class);

  private static final short REPLICATION = 3;

  private Configuration conf;
  private MiniDFSCluster cluster;

  private int count = 0;
  private LocatedBlock lb1;
  private LocatedBlock lb2;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Retry addBlock() while another thread is in chooseTarget().
   * See HDFS-4452.
   */
  @Test
  public void testRetryAddBlockWhileInChooseTarget() throws Exception {
    final String src = "/testRetryAddBlockWhileInChooseTarget";

    FSNamesystem ns = cluster.getNamesystem();
    BlockManager spyBM = spy(ns.getBlockManager());
    final NamenodeProtocols nn = cluster.getNameNodeRpc();

    // substitute mocked BlockManager into FSNamesystem
    Class<? extends FSNamesystem> nsClass = ns.getClass();
    Field bmField = nsClass.getDeclaredField("blockManager");
    bmField.setAccessible(true);
    bmField.set(ns, spyBM);

    doAnswer(new Answer<DatanodeStorageInfo[]>() {
      @Override
      public DatanodeStorageInfo[] answer(InvocationOnMock invocation)
          throws Throwable {
        LOG.info("chooseTarget for " + src);
        DatanodeStorageInfo[] ret =
            (DatanodeStorageInfo[]) invocation.callRealMethod();
        count++;
        if(count == 1) { // run second addBlock()
          LOG.info("Starting second addBlock for " + src);
          nn.addBlock(src, "clientName", null, null,
              INodeId.GRANDFATHER_INODE_ID, null);
          LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
          assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
          lb2 = lbs.get(0);
          assertEquals("Wrong replication",
              REPLICATION, lb2.getLocations().length);
        }
        return ret;
      }
    }).when(spyBM).chooseTarget(Mockito.anyString(), Mockito.anyInt(),
        Mockito.<DatanodeDescriptor>any(), Mockito.<HashSet<Node>>any(),
        Mockito.anyLong(), Mockito.<List<String>>any());

    // create file
    nn.create(src, FsPermission.getFileDefault(),
        "clientName",
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)),
        true, (short)3, 1024, null);

    // start first addBlock()
    LOG.info("Starting first addBlock for " + src);
    nn.addBlock(src, "clientName", null, null, INodeId.GRANDFATHER_INODE_ID, null);

    // check locations
    LocatedBlocks lbs = nn.getBlockLocations(src, 0, Long.MAX_VALUE);
    assertEquals("Must be one block", 1, lbs.getLocatedBlocks().size());
    lb1 = lbs.get(0);
    assertEquals("Wrong replication", REPLICATION, lb1.getLocations().length);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
  }

  /*
   * Since NameNode will not persist any locations of the block, addBlock()
   * retry call after restart NN should re-select the locations and return to
   * client. refer HDFS-5257
   */
  @Test
  public void testAddBlockRetryShouldReturnBlockWithLocations()
      throws Exception {
    final String src = "/testAddBlockRetryShouldReturnBlockWithLocations";
    NamenodeProtocols nameNodeRpc = cluster.getNameNodeRpc();
    // create file
    nameNodeRpc.create(src, FsPermission.getFileDefault(), "clientName",
        new EnumSetWritable<CreateFlag>(EnumSet.of(CreateFlag.CREATE)), true,
        (short) 3, 1024, null);
    // start first addBlock()
    LOG.info("Starting first addBlock for " + src);
    LocatedBlock lb1 = nameNodeRpc.addBlock(src, "clientName", null, null,
        INodeId.GRANDFATHER_INODE_ID, null);
    assertTrue("Block locations should be present",
        lb1.getLocations().length > 0);

    cluster.restartNameNode();
    nameNodeRpc = cluster.getNameNodeRpc();
    LocatedBlock lb2 = nameNodeRpc.addBlock(src, "clientName", null, null,
        INodeId.GRANDFATHER_INODE_ID, null);
    assertEquals("Blocks are not equal", lb1.getBlock(), lb2.getBlock());
    assertTrue("Wrong locations with retry", lb2.getLocations().length > 0);
  }
}
