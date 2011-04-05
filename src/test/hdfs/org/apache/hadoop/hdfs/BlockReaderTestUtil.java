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

import java.net.Socket;
import java.net.InetSocketAddress;
import java.io.DataOutputStream;
import java.util.Random;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.net.NetUtils;

import static org.junit.Assert.*;

/**
 * A helper class to setup the cluster, and get to BlockReader and DataNode for a block.
 */
public class BlockReaderTestUtil {

  private HdfsConfiguration conf = null;
  private MiniDFSCluster cluster = null;

  /**
   * Setup the cluster
   */
  public BlockReaderTestUtil(int replicationFactor) throws Exception {
    conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, replicationFactor);
    cluster = new MiniDFSCluster.Builder(conf).format(true).build();
    cluster.waitActive();
  }

  /**
   * Shutdown cluster
   */
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }

  public HdfsConfiguration getConf() {
    return conf;
  }

  /**
   * Create a file of the given size filled with random data.
   * @return  List of Blocks of the new file.
   */
  public List<LocatedBlock> writeFile(Path filepath, int sizeKB)
      throws IOException {
    FileSystem fs = cluster.getFileSystem();

    // Write a file with the specified amount of data
    DataOutputStream os = fs.create(filepath);
    byte data[] = new byte[1024];
    new Random().nextBytes(data);
    for (int i = 0; i < sizeKB; i++) {
      os.write(data);
    }
    os.close();

    // Return the blocks we just wrote
    DFSClient dfsclient = new DFSClient(
      new InetSocketAddress("localhost", cluster.getNameNodePort()), conf);
    return dfsclient.getNamenode().getBlockLocations(
      filepath.toString(), 0, sizeKB * 1024).getLocatedBlocks();
  }


  /**
   * Exercise the BlockReader and read length bytes.
   *
   * It does not verify the bytes read.
   */
  public void readAndCheckEOS(BlockReader reader, int length, boolean expectEof)
      throws IOException {
    byte buf[] = new byte[1024];
    int nRead = 0;
    while (nRead < length) {
      DFSClient.LOG.info("So far read " + nRead + " - going to read more.");
      int n = reader.read(buf, 0, buf.length);
      assertTrue(n > 0);
      nRead += n;
    }

    if (expectEof) {
      DFSClient.LOG.info("Done reading, expect EOF for next read.");
      assertEquals(-1, reader.read(buf, 0, buf.length));
    }
  }

  /**
   * Get a BlockReader for the given block.
   */
  public BlockReader getBlockReader(LocatedBlock testBlock, int offset, int lenToRead)
      throws IOException {
    InetSocketAddress targetAddr = null;
    Socket sock = null;
    ExtendedBlock block = testBlock.getBlock();
    DatanodeInfo[] nodes = testBlock.getLocations();
    targetAddr = NetUtils.createSocketAddr(nodes[0].getName());
    sock = new Socket();
    sock.connect(targetAddr, HdfsConstants.READ_TIMEOUT);
    sock.setSoTimeout(HdfsConstants.READ_TIMEOUT);

    return BlockReader.newBlockReader(
      sock, targetAddr.toString()+ ":" + block.getBlockId(), block,
      testBlock.getBlockToken(), 
      offset, lenToRead,
      conf.getInt("io.file.buffer.size", 4096));
  }

  /**
   * Get a DataNode that serves our testBlock.
   */
  public DataNode getDataNode(LocatedBlock testBlock) {
    DatanodeInfo[] nodes = testBlock.getLocations();
    int ipcport = nodes[0].ipcPort;
    return cluster.getDataNode(ipcport);
  }

}
