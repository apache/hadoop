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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPipelines {
  public static final Logger LOG = LoggerFactory.getLogger(TestPipelines.class);

  private static final short REPL_FACTOR = 3;
  private static final int RAND_LIMIT = 2000;
  private static final int FILE_SIZE = 10000;

  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static Configuration conf;
  static final Random rand = new Random(RAND_LIMIT);

  static {
    initLoggers();
    setConfiguration();
  }

  @Before
  public void startUpCluster() throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL_FACTOR).build();
    fs = cluster.getFileSystem();
  }

  @After
  public void shutDownCluster() throws IOException {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (cluster != null) {
      cluster.shutdownDataNodes();
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Creates and closes a file of certain length.
   * Calls append to allow next write() operation to add to the end of it
   * After write() invocation, calls hflush() to make sure that data sunk through
   * the pipeline and check the state of the last block's replica.
   * It supposes to be in RBW state
   *
   * @throws IOException in case of an error
   */
  @Test
  public void pipeline_01() throws IOException {
    final String METHOD_NAME = GenericTestUtils.getMethodName();
    if(LOG.isDebugEnabled()) {
      LOG.debug("Running " + METHOD_NAME);
    }
    Path filePath = new Path("/" + METHOD_NAME + ".dat");

    DFSTestUtil.createFile(fs, filePath, FILE_SIZE, REPL_FACTOR, rand.nextLong());
    if(LOG.isDebugEnabled()) {
      LOG.debug("Invoking append but doing nothing otherwise...");
    }
    FSDataOutputStream ofs = fs.append(filePath);
    ofs.writeBytes("Some more stuff to write");
    ((DFSOutputStream) ofs.getWrappedStream()).hflush();

    List<LocatedBlock> lb = cluster.getNameNodeRpc().getBlockLocations(
      filePath.toString(), FILE_SIZE - 1, FILE_SIZE).getLocatedBlocks();

    for (DataNode dn : cluster.getDataNodes()) {
      Replica r =
          cluster.getFsDatasetTestUtils(dn).fetchReplica(lb.get(0).getBlock());

      assertTrue("Replica on DN " + dn + " shouldn't be null", r != null);
      assertEquals("Should be RBW replica on " + dn
          + " after sequence of calls append()/write()/hflush()",
          HdfsServerConstants.ReplicaState.RBW, r.getState());
    }
    ofs.close();
  }

  /**
   * These two test cases are already implemented by
   *
   * @link{TestReadWhileWriting}
   */
  public void pipeline_02_03() {
  }
  
  static byte[] writeData(final FSDataOutputStream out, final int length)
    throws IOException {
    int bytesToWrite = length;
    byte[] ret = new byte[bytesToWrite];
    byte[] toWrite = new byte[1024];
    int written = 0;
    Random rb = new Random(rand.nextLong());
    while (bytesToWrite > 0) {
      rb.nextBytes(toWrite);
      int bytesToWriteNext = (1024 < bytesToWrite) ? 1024 : bytesToWrite;
      out.write(toWrite, 0, bytesToWriteNext);
      System.arraycopy(toWrite, 0, ret, (ret.length - bytesToWrite),
        bytesToWriteNext);
      written += bytesToWriteNext;
      if(LOG.isDebugEnabled()) {
        LOG.debug("Written: " + bytesToWriteNext + "; Total: " + written);
      }
      bytesToWrite -= bytesToWriteNext;
    }
    return ret;
  }
  
  private static void setConfiguration() {
    conf = new Configuration();
    int customPerChecksumSize = 700;
    int customBlockSize = customPerChecksumSize * 3;
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, 100);
    conf.setInt(HdfsClientConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, customPerChecksumSize);
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY, customBlockSize / 2);
    conf.setInt(HdfsClientConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY, 0);
  }

  private static void initLoggers() {
    DFSTestUtil.setNameNodeLogLevel(Level.ALL);
    GenericTestUtils.setLogLevel(DataNode.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
  }
}
