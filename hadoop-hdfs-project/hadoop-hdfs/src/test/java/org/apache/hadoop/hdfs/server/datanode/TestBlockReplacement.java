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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Time;
import org.junit.Test;

/**
 * This class tests if block replacement request to data nodes work correctly.
 */
public class TestBlockReplacement {
  private static final Log LOG = LogFactory.getLog(
  "org.apache.hadoop.hdfs.TestBlockReplacement");

  MiniDFSCluster cluster;
  @Test
  public void testThrottler() throws IOException {
    Configuration conf = new HdfsConfiguration();
    FileSystem.setDefaultUri(conf, "hdfs://localhost:0");
    long bandwidthPerSec = 1024*1024L;
    final long TOTAL_BYTES =6*bandwidthPerSec; 
    long bytesToSend = TOTAL_BYTES; 
    long start = Time.now();
    DataTransferThrottler throttler = new DataTransferThrottler(bandwidthPerSec);
    long totalBytes = 0L;
    long bytesSent = 1024*512L; // 0.5MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    bytesSent = 1024*768L; // 0.75MB
    throttler.throttle(bytesSent);
    bytesToSend -= bytesSent;
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {}
    throttler.throttle(bytesToSend);
    long end = Time.now();
    assertTrue(totalBytes*1000/(end-start)<=bandwidthPerSec);
  }
  
  @Test
  public void testBlockReplacement() throws Exception {
    final Configuration CONF = new HdfsConfiguration();
    final String[] INITIAL_RACKS = {"/RACK0", "/RACK1", "/RACK2"};
    final String[] NEW_RACKS = {"/RACK2"};

    final short REPLICATION_FACTOR = (short)3;
    final int DEFAULT_BLOCK_SIZE = 1024;
    final Random r = new Random();
    
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE/2);
    CONF.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,500);
    cluster = new MiniDFSCluster.Builder(CONF).numDataNodes(REPLICATION_FACTOR)
                                              .racks(INITIAL_RACKS).build();

    try {
      cluster.waitActive();
      
      FileSystem fs = cluster.getFileSystem();
      Path fileName = new Path("/tmp.txt");
      
      // create a file with one block
      DFSTestUtil.createFile(fs, fileName,
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, r.nextLong());
      DFSTestUtil.waitReplication(fs,fileName, REPLICATION_FACTOR);
      
      // get all datanodes
      InetSocketAddress addr = new InetSocketAddress("localhost",
          cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, CONF);
      List<LocatedBlock> locatedBlocks = client.getNamenode().
        getBlockLocations("/tmp.txt", 0, DEFAULT_BLOCK_SIZE).getLocatedBlocks();
      assertEquals(1, locatedBlocks.size());
      LocatedBlock block = locatedBlocks.get(0);
      DatanodeInfo[]  oldNodes = block.getLocations();
      assertEquals(oldNodes.length, 3);
      ExtendedBlock b = block.getBlock();
      
      // add a fourth datanode to the cluster
      cluster.startDataNodes(CONF, 1, true, null, NEW_RACKS);
      cluster.waitActive();
      
      DatanodeInfo[] datanodes = client.datanodeReport(DatanodeReportType.ALL);

      // find out the new node
      DatanodeInfo newNode=null;
      for(DatanodeInfo node:datanodes) {
        Boolean isNewNode = true;
        for(DatanodeInfo oldNode:oldNodes) {
          if(node.equals(oldNode)) {
            isNewNode = false;
            break;
          }
        }
        if(isNewNode) {
          newNode = node;
          break;
        }
      }
      
      assertTrue(newNode!=null);
      DatanodeInfo source=null;
      ArrayList<DatanodeInfo> proxies = new ArrayList<DatanodeInfo>(2);
      for(DatanodeInfo node:datanodes) {
        if(node != newNode) {
          if( node.getNetworkLocation().equals(newNode.getNetworkLocation())) {
            source = node;
          } else {
            proxies.add( node );
          }
        }
      }
      //current state: the newNode is on RACK2, and "source" is the other dn on RACK2.
      //the two datanodes on RACK0 and RACK1 are in "proxies".
      //"source" and both "proxies" all contain the block, while newNode doesn't yet.
      assertTrue(source!=null && proxies.size()==2);
      
      // start to replace the block
      // case 1: proxySource does not contain the block
      LOG.info("Testcase 1: Proxy " + newNode
           + " does not contain the block " + b);
      assertFalse(replaceBlock(b, source, newNode, proxies.get(0)));
      // case 2: destination already contains the block
      LOG.info("Testcase 2: Destination " + proxies.get(1)
          + " contains the block " + b);
      assertFalse(replaceBlock(b, source, proxies.get(0), proxies.get(1)));
      // case 3: correct case
      LOG.info("Testcase 3: Source=" + source + " Proxy=" + 
          proxies.get(0) + " Destination=" + newNode );
      assertTrue(replaceBlock(b, source, proxies.get(0), newNode));
      // after cluster has time to resolve the over-replication,
      // block locations should contain two proxies and newNode
      // but not source
      checkBlocks(new DatanodeInfo[]{newNode, proxies.get(0), proxies.get(1)},
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
      // case 4: proxies.get(0) is not a valid del hint
      // expect either source or newNode replica to be deleted instead
      LOG.info("Testcase 4: invalid del hint " + proxies.get(0) );
      assertTrue(replaceBlock(b, proxies.get(0), proxies.get(1), source));
      // after cluster has time to resolve the over-replication,
      // block locations should contain two proxies,
      // and either source or newNode, but not both.
      checkBlocks(proxies.toArray(new DatanodeInfo[proxies.size()]), 
          fileName.toString(), 
          DEFAULT_BLOCK_SIZE, REPLICATION_FACTOR, client);
    } finally {
      cluster.shutdown();
    }
  }
  
  /* check if file's blocks have expected number of replicas,
   * and exist at all of includeNodes
   */
  private void checkBlocks(DatanodeInfo[] includeNodes, String fileName, 
      long fileLen, short replFactor, DFSClient client) 
      throws IOException, TimeoutException {
    boolean notDone;
    final long TIMEOUT = 20000L;
    long starttime = Time.now();
    long failtime = starttime + TIMEOUT;
    do {
      try {
        Thread.sleep(100);
      } catch(InterruptedException e) {
      }
      List<LocatedBlock> blocks = client.getNamenode().
      getBlockLocations(fileName, 0, fileLen).getLocatedBlocks();
      assertEquals(1, blocks.size());
      DatanodeInfo[] nodes = blocks.get(0).getLocations();
      notDone = (nodes.length != replFactor);
      if (notDone) {
        LOG.info("Expected replication factor is " + replFactor +
            " but the real replication factor is " + nodes.length );
      } else {
        List<DatanodeInfo> nodeLocations = Arrays.asList(nodes);
        for (DatanodeInfo node : includeNodes) {
          if (!nodeLocations.contains(node) ) {
            notDone=true; 
            LOG.info("Block is not located at " + node );
            break;
          }
        }
      }
      if (Time.now() > failtime) {
        String expectedNodesList = "";
        String currentNodesList = "";
        for (DatanodeInfo dn : includeNodes) 
          expectedNodesList += dn + ", ";
        for (DatanodeInfo dn : nodes) 
          currentNodesList += dn + ", ";
        LOG.info("Expected replica nodes are: " + expectedNodesList);
        LOG.info("Current actual replica nodes are: " + currentNodesList);
        throw new TimeoutException(
            "Did not achieve expected replication to expected nodes "
            + "after more than " + TIMEOUT + " msec.  See logs for details.");
      }
    } while(notDone);
    LOG.info("Achieved expected replication values in "
        + (Time.now() - starttime) + " msec.");
  }

  /* Copy a block from sourceProxy to destination. If the block becomes
   * over-replicated, preferably remove it from source.
   * 
   * Return true if a block is successfully copied; otherwise false.
   */
  private boolean replaceBlock( ExtendedBlock block, DatanodeInfo source,
      DatanodeInfo sourceProxy, DatanodeInfo destination) throws IOException {
    Socket sock = new Socket();
    sock.connect(NetUtils.createSocketAddr(
        destination.getXferAddr()), HdfsServerConstants.READ_TIMEOUT); 
    sock.setKeepAlive(true);
    // sendRequest
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());
    new Sender(out).replaceBlock(block, StorageType.DEFAULT,
        BlockTokenSecretManager.DUMMY_TOKEN,
        source.getDatanodeUuid(), sourceProxy);
    out.flush();
    // receiveResponse
    DataInputStream reply = new DataInputStream(sock.getInputStream());

    BlockOpResponseProto proto = BlockOpResponseProto.parseDelimitedFrom(reply);
    while (proto.getStatus() == Status.IN_PROGRESS) {
      proto = BlockOpResponseProto.parseDelimitedFrom(reply);
    }
    return proto.getStatus() == Status.SUCCESS;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    (new TestBlockReplacement()).testBlockReplacement();
  }

}
