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

import java.io.OutputStream;
import java.net.InetSocketAddress;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.fs.Path;
import junit.framework.TestCase;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestDatanodeRegistration extends TestCase {

  /**
   * Regression test for HDFS-894 ensures that, when datanodes
   * are restarted, the new IPC port is registered with the
   * namenode.
   */
  public void testChangeIpcPort() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    try {
      cluster = new MiniDFSCluster(conf, 1, true, null);
      fs = cluster.getFileSystem();

      InetSocketAddress addr = new InetSocketAddress(
        "localhost",
        cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, conf);

      // Restart datanodes
      cluster.restartDataNodes();

      // Wait until we get a heartbeat from the new datanode
      DatanodeInfo[] report = client.datanodeReport(DatanodeReportType.ALL);
      long firstUpdateAfterRestart = report[0].getLastUpdate();

      boolean gotHeartbeat = false;
      for (int i = 0; i < 10 && !gotHeartbeat; i++) {
        try {
          Thread.sleep(i*1000);
        } catch (InterruptedException ie) {}

        report = client.datanodeReport(DatanodeReportType.ALL);
        gotHeartbeat = (report[0].getLastUpdate() > firstUpdateAfterRestart);
      }
      if (!gotHeartbeat) {
        fail("Never got a heartbeat from restarted datanode.");
      }

      int realIpcPort = cluster.getDataNodes().get(0)
        .dnRegistration.getIpcPort();
      // Now make sure the reported IPC port is the correct one.
      assertEquals(realIpcPort, report[0].getIpcPort());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
