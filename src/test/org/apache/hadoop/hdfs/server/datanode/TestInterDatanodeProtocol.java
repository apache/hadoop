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

import java.io.IOException;
import java.util.List;
import java.net.InetSocketAddress;
 
import java.net.SocketTimeoutException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataBlockScanner;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.BlockMetaDataInfo;
import org.apache.hadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

/**
 * This tests InterDataNodeProtocol for block handling. 
 */
public class TestInterDatanodeProtocol extends junit.framework.TestCase {
 private static final String ADDRESS = "0.0.0.0";
  final static private int PING_INTERVAL = 1000;
  final static private int MIN_SLEEP_TIME = 1000;
  private static Configuration conf = new Configuration();

  private static class TestServer extends Server {
    private boolean sleep;
    private Class<? extends Writable> responseClass;

    public TestServer(int handlerCount, boolean sleep) throws IOException {
      this(handlerCount, sleep, LongWritable.class, null);
    }

    public TestServer(int handlerCount, boolean sleep,
        Class<? extends Writable> paramClass,
        Class<? extends Writable> responseClass)
      throws IOException {
      super(ADDRESS, 0, paramClass, handlerCount, conf);
      this.sleep = sleep;
      this.responseClass = responseClass;
    }

    @Override
    public Writable call(Class<?> protocol, Writable param, long receiveTime)
        throws IOException {
      if (sleep) {
        // sleep a bit
        try {
          Thread.sleep(PING_INTERVAL + MIN_SLEEP_TIME);
        } catch (InterruptedException e) {}
      }
      if (responseClass != null) {
        try {
          return responseClass.newInstance();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        return param;                               // echo param as result
      }
    }
  }

  public static void checkMetaInfo(Block b, InterDatanodeProtocol idp,
      DataBlockScanner scanner) throws IOException {
    BlockMetaDataInfo metainfo = idp.getBlockMetaDataInfo(b);
    assertEquals(b.getBlockId(), metainfo.getBlockId());
    assertEquals(b.getNumBytes(), metainfo.getNumBytes());
    if (scanner != null) {
      assertEquals(scanner.getLastScanTime(b),
          metainfo.getLastScanTime());
    }
  }

  public static LocatedBlock getLastLocatedBlock(
      ClientProtocol namenode, String src
  ) throws IOException {
    //get block info for the last block
    LocatedBlocks locations = namenode.getBlockLocations(src, 0, Long.MAX_VALUE);
    List<LocatedBlock> blocks = locations.getLocatedBlocks();
    DataNode.LOG.info("blocks.size()=" + blocks.size());
    assertTrue(blocks.size() > 0);

    return blocks.get(blocks.size() - 1);
  }

  /** Test block MD access via a DN */
  public void testBlockMetaDataInfo() throws Exception {
    checkBlockMetaDataInfo(false);
  }

  /** The same as above, but use hostnames for DN<->DN communication */
  public void testBlockMetaDataInfoWithHostname() throws Exception {
    checkBlockMetaDataInfo(true);
  }

  /**
   * The following test first creates a file.
   * It verifies the block information from a datanode.
   * Then, it updates the block with new information and verifies again.
   * @param useDnHostname if DNs should access DNs by hostname (vs IP)
   */
  private void checkBlockMetaDataInfo(boolean useDnHostname) throws Exception {    
    MiniDFSCluster cluster = null;

    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME, useDnHostname);
    if (useDnHostname) {
      // Since the mini cluster only listens on the loopback we have to
      // ensure the hostname used to access DNs maps to the loopback. We
      // do this by telling the DN to advertise localhost as its hostname
      // instead of the default hostname.
      conf.set("slave.host.name", "localhost");
    }

    try {
      cluster = new MiniDFSCluster(conf, 3, true, null);
      cluster.waitActive();

      //create a file
      DistributedFileSystem dfs = (DistributedFileSystem)cluster.getFileSystem();
      String filestr = "/foo";
      Path filepath = new Path(filestr);
      DFSTestUtil.createFile(dfs, filepath, 1024L, (short)3, 0L);
      assertTrue(dfs.getClient().exists(filestr));

      //get block info
      LocatedBlock locatedblock = getLastLocatedBlock(dfs.getClient().namenode, filestr);
      DatanodeInfo[] datanodeinfo = locatedblock.getLocations();
      assertTrue(datanodeinfo.length > 0);

      //connect to a data node
      DataNode datanode = cluster.getDataNode(datanodeinfo[0].getIpcPort());
      assertTrue(datanode != null);
      InterDatanodeProtocol idp = DataNode.createInterDataNodeProtocolProxy(
          datanodeinfo[0], conf, datanode.socketTimeout, useDnHostname);
      
      //stop block scanner, so we could compare lastScanTime
      datanode.blockScannerThread.interrupt();

      //verify BlockMetaDataInfo
      Block b = locatedblock.getBlock();
      InterDatanodeProtocol.LOG.info("b=" + b + ", " + b.getClass());
      checkMetaInfo(b, idp, datanode.blockScanner);

      //verify updateBlock
      Block newblock = new Block(
          b.getBlockId(), b.getNumBytes()/2, b.getGenerationStamp()+1);
      idp.updateBlock(b, newblock, false);
      checkMetaInfo(newblock, idp, datanode.blockScanner);
    }
    finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  /** Test to verify that InterDatanode RPC timesout as expected when
   *  the server DN does not respond.
   */
  public void testInterDNProtocolTimeout() throws Exception {
    final Server server = new TestServer(1, true);
    server.start();

    final InetSocketAddress addr = NetUtils.getConnectAddress(server);
    DatanodeID fakeDnId = new DatanodeID(
        "localhost:" + addr.getPort(), "fake-storage", 0, addr.getPort());
    DatanodeInfo dInfo = new DatanodeInfo(fakeDnId);
    InterDatanodeProtocol proxy = null;

    try {
      proxy = DataNode.createInterDataNodeProtocolProxy(
          dInfo, conf, 500, false);
      fail ("Expected SocketTimeoutException exception, but did not get.");
    } catch (SocketTimeoutException e) {
      DataNode.LOG.info("Got expected Exception: SocketTimeoutException");
    } finally {
      if (proxy != null) {
        RPC.stopProxy(proxy);
      }
      server.stop();
    }
  }
}
