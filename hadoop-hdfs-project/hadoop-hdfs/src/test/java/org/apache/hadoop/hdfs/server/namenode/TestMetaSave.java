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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the creation and validation of metasave
 */
public class TestMetaSave {
  static final int NUM_DATA_NODES = 2;
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  private static MiniDFSCluster cluster = null;
  private static FileSystem fileSys = null;
  private static NamenodeProtocols nnRpc = null;

  @Before
  public void setUp() throws IOException {
    // start a cluster
    Configuration conf = new HdfsConfiguration();

    // High value of replication interval
    // so that blocks remain less redundant
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        1000);
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 1L);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 5L);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES).build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    nnRpc = cluster.getNameNodeRpc();
  }

  /**
   * Tests metasave
   */
  @Test
  public void testMetaSave()
      throws IOException, InterruptedException, TimeoutException {
    for (int i = 0; i < 2; i++) {
      Path file = new Path("/filestatus" + i);
      DFSTestUtil.createFile(fileSys, file, 1024, 1024, blockSize, (short) 2,
          seed);
    }

    // stop datanode and wait for namenode to discover that a datanode is dead
    stopDatanodeAndWait(1);

    nnRpc.setReplication("/filestatus0", (short) 4);

    nnRpc.metaSave("metasave.out.txt");

    // Verification
    FileInputStream fstream = new FileInputStream(getLogFile(
      "metasave.out.txt"));
    DataInputStream in = new DataInputStream(fstream);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(in));
      String line = reader.readLine();
      Assert.assertEquals(
          "3 files and directories, 2 blocks = 5 total filesystem objects",
          line);
      line = reader.readLine();
      assertTrue(line.equals("Live Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Dead Datanodes: 1"));
      reader.readLine();
      line = reader.readLine();
      assertTrue(line.matches("^/filestatus[01]:.*"));
    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * Tests metasave after delete, to make sure there are no orphaned blocks
   */
  @Test
  public void testMetasaveAfterDelete()
      throws IOException, InterruptedException, TimeoutException {
    for (int i = 0; i < 2; i++) {
      Path file = new Path("/filestatus" + i);
      DFSTestUtil.createFile(fileSys, file, 1024, 1024, blockSize, (short) 2,
          seed);
    }

    // stop datanode and wait for namenode to discover that a datanode is dead
    stopDatanodeAndWait(1);

    nnRpc.setReplication("/filestatus0", (short) 4);
    nnRpc.delete("/filestatus0", true);
    nnRpc.delete("/filestatus1", true);

    nnRpc.metaSave("metasaveAfterDelete.out.txt");

    // Verification
    BufferedReader reader = null;
    try {
      FileInputStream fstream = new FileInputStream(getLogFile(
        "metasaveAfterDelete.out.txt"));
      DataInputStream in = new DataInputStream(fstream);
      reader = new BufferedReader(new InputStreamReader(in));
      reader.readLine();
      String line = reader.readLine();
      assertTrue(line.equals("Live Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Dead Datanodes: 1"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks waiting for reconstruction: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks currently missing: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Mis-replicated blocks that have been postponed:"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks being reconstructed: 0"));
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Blocks 2 waiting deletion from 1 datanodes."));
      //skip 2 lines to reach HDFS-9033 scenario.
      line = reader.readLine();
      line = reader.readLine();
      assertTrue(line.contains("blk"));
      // skip 1 line for Corrupt Blocks section.
      line = reader.readLine();
      line = reader.readLine();
      assertTrue(line.equals("Metasave: Number of datanodes: 2"));
      line = reader.readLine();
      assertFalse(line.contains("NaN"));

    } finally {
      if (reader != null)
        reader.close();
    }
  }

  /**
   * Tests that metasave overwrites the output file (not append).
   */
  @Test
  public void testMetaSaveOverwrite() throws Exception {
    // metaSave twice.
    nnRpc.metaSave("metaSaveOverwrite.out.txt");
    nnRpc.metaSave("metaSaveOverwrite.out.txt");

    // Read output file.
    FileInputStream fis = null;
    InputStreamReader isr = null;
    BufferedReader rdr = null;
    try {
      fis = new FileInputStream(getLogFile("metaSaveOverwrite.out.txt"));
      isr = new InputStreamReader(fis);
      rdr = new BufferedReader(isr);

      // Validate that file was overwritten (not appended) by checking for
      // presence of only one "Live Datanodes" line.
      boolean foundLiveDatanodesLine = false;
      String line = rdr.readLine();
      while (line != null) {
        if (line.startsWith("Live Datanodes")) {
          if (foundLiveDatanodesLine) {
            fail("multiple Live Datanodes lines, output file not overwritten");
          }
          foundLiveDatanodesLine = true;
        }
        line = rdr.readLine();
      }
    } finally {
      IOUtils.cleanupWithLogger(null, rdr, isr, fis);
    }
  }

  class MetaSaveThread extends Thread {
    NamenodeProtocols nnRpc;
    String filename;
    public MetaSaveThread(NamenodeProtocols nnRpc, String filename) {
      this.nnRpc = nnRpc;
      this.filename = filename;
    }

    @Override
    public void run() {
      try {
        nnRpc.metaSave(filename);
      } catch (IOException e) {
      }
    }
  }

  /**
   * Tests that metasave concurrent output file (not append).
   */
  @Test
  public void testConcurrentMetaSave() throws Exception {
    ArrayList<MetaSaveThread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      threads.add(new MetaSaveThread(nnRpc, "metaSaveConcurrent.out.txt"));
    }
    for (int i = 0; i < 10; i++) {
      threads.get(i).start();
    }
    for (int i = 0; i < 10; i++) {
      threads.get(i).join();
    }
    // Read output file.
    FileInputStream fis = null;
    InputStreamReader isr = null;
    BufferedReader rdr = null;
    try {
      fis = new FileInputStream(getLogFile("metaSaveConcurrent.out.txt"));
      isr = new InputStreamReader(fis);
      rdr = new BufferedReader(isr);

      // Validate that file was overwritten (not appended) by checking for
      // presence of only one "Live Datanodes" line.
      boolean foundLiveDatanodesLine = false;
      String line = rdr.readLine();
      while (line != null) {
        if (line.startsWith("Live Datanodes")) {
          if (foundLiveDatanodesLine) {
            fail("multiple Live Datanodes lines, output file not overwritten");
          }
          foundLiveDatanodesLine = true;
        }
        line = rdr.readLine();
      }
    } finally {
      IOUtils.cleanupWithLogger(null, rdr, isr, fis);
    }
  }

  @After
  public void tearDown() throws IOException {
    if (fileSys != null)
      fileSys.close();
    if (cluster != null)
      cluster.shutdown();
  }

  /**
   * Returns a File for the given name inside the log directory.
   * 
   * @param name String file name
   * @return File for given name inside log directory
   */
  private static File getLogFile(String name) {
    return new File(System.getProperty("hadoop.log.dir"), name);
  }

  /**
   * Stop a DN, notify NN the death of DN and wait for NN to remove the DN.
   *
   * @param dnIdx Index of the Datanode in MiniDFSCluster
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void stopDatanodeAndWait(final int dnIdx)
      throws TimeoutException, InterruptedException {
    final DataNode dnToStop = cluster.getDataNodes().get(dnIdx);
    cluster.stopDataNode(dnIdx);
    BlockManagerTestUtil.noticeDeadDatanode(
        cluster.getNameNode(), dnToStop.getDatanodeId().getXferAddr());
    // wait for namenode to discover that a datanode is dead
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return BlockManagerTestUtil.isDatanodeRemoved(
            cluster.getNameNode(), dnToStop.getDatanodeUuid());
      }
    }, 1000, 30000);
  }
}
