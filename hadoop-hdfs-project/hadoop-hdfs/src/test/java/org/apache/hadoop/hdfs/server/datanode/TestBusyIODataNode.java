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

package org.apache.hadoop.hdfs.server.datanode;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.NumberReplicas;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBusyIODataNode {

  public static final Logger LOG = LoggerFactory.getLogger(TestBusyIODataNode
      .class);

  private MiniDFSCluster cluster;
  private Configuration conf;
  private FSNamesystem fsn;
  private BlockManager bm;

  static final long SEED = 0xDEADBEEFL;
  static final int BLOCK_SIZE = 8192;
  private static final int HEARTBEAT_INTERVAL = 1;

  private final Path dir = new Path("/" + this.getClass().getSimpleName());

  @Before
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    conf.setTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        0, TimeUnit.MILLISECONDS);
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 1);
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 1000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    bm = fsn.getBlockManager();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  static protected void writeFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    writeFile(fileSys, name, repl, 2);
  }

  static protected void writeFile(FileSystem fileSys, Path name, int repl,
      int numOfBlocks) throws IOException {
    writeFile(fileSys, name, repl, numOfBlocks, true);
  }

  static protected FSDataOutputStream writeFile(FileSystem fileSys, Path name,
      int repl, int numOfBlocks, boolean completeFile)
      throws IOException {
    // create and write a file that contains two blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
            .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, BLOCK_SIZE);
    byte[] buffer = new byte[BLOCK_SIZE * numOfBlocks];
    Random rand = new Random(SEED);
    rand.nextBytes(buffer);
    stm.write(buffer);
    LOG.info("Created file " + name + " with " + repl + " replicas.");
    if (completeFile) {
      stm.close();
      return null;
    } else {
      stm.flush();
      // Do not close stream, return it
      // so that it is not garbage collected
      return stm;
    }
  }

  /*
   * Wait till node is fully decommissioned.
   */
  private void waitBlockMeetReplication(BlockInfo blockInfo, Short repl) {
    boolean done = repl == blockInfo.numNodes();
    while (!done) {
      LOG.info("Waiting for repl change to " + repl + " current repl: "
          + blockInfo.numNodes());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 500);
      } catch (InterruptedException e) {
        // nothing
      }
      done = repl == blockInfo.numNodes();
    }
    LOG.info("block " + blockInfo + " meet the replication " + repl);
  }

  /**
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testIOBusyNode() throws Exception {

    FileSystem fileSys = cluster.getFileSystem(0);
    // 1. create file
    final Path file = new Path(dir, "testFile");
    int repl = 1;
    writeFile(fileSys, file, repl);

    // 2. find the datanode which store this block
    final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(file.toString()).asFile();
    BlockInfo firstBlock = fileNode.getBlocks()[0];
    NumberReplicas replicas = bm.countNodes(firstBlock);
    Assert.assertEquals(1, replicas.liveReplicas());
    Assert.assertEquals(1, firstBlock.numNodes());

    // 3. make datanode io busy. we delay remove operation so that we could
    //   simulate that the datanode's io is busy.
    DatanodeDescriptor datanode = firstBlock.getDatanode(0);
    Logger log = mock(Logger.class);
    for (DataNode dn : cluster.getDataNodes()) {
      if (datanode.getXferPort() != dn.getXferPort()) {
        continue;
      }
      Set<ExtendedBlock> set = Collections
          .synchronizedSet(new HashSet<ExtendedBlock>() {
            @Override
            public boolean add(ExtendedBlock block) {
              boolean ret = super.add(block);
              try {
                Thread.sleep(30000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return ret;
            }
          });
      Field transferringBlock = DataNode.class
          .getDeclaredField("transferringBlock");
      transferringBlock.setAccessible(true);
      Field modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(transferringBlock,
          transferringBlock.getModifiers() & ~Modifier.FINAL);
      transferringBlock.set(dn, set);

      Field logger = DataNode.class.getDeclaredField("LOG");
      logger.setAccessible(true);
      modifiers = Field.class.getDeclaredField("modifiers");
      modifiers.setAccessible(true);
      modifiers.setInt(logger, logger.getModifiers() & ~Modifier.FINAL);
      logger.set(null, log);
    }

    // 4. add block's replication to 2
    bm.setReplication((short) 1, (short) 2, firstBlock);
    waitBlockMeetReplication(firstBlock, (short) 2);
    replicas = bm.countNodes(firstBlock);
    Assert.assertEquals(replicas.liveReplicas(), 2);

    // 5. sleep and verfiy the unnecessary transfer.
    Thread.sleep(3000);
    String blockPoolId = cluster.getNameNode().getNamesystem().getBlockPoolId();
    verify(log, atLeastOnce())
        .warn("Thread for transfer {} was already running，ignore this block.",
            new ExtendedBlock(blockPoolId, firstBlock));
  }

}
