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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.util.Time.monotonicNow;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY;
import static org.junit.Assert.assertTrue;

/**
 * Tests throttle the data transfers related functions.
 */
public class TestDataTransferThrottler {

  /**
   * Test read data transfer throttler.
   */
  @Test
  public void testReadDataTransferThrottler() throws Exception {
    final HdfsConfiguration conf = new HdfsConfiguration();

    try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build()) {
      cluster.waitActive();
      final DistributedFileSystem fs = cluster.getFileSystem();

      // Create file.
      Path file = new Path("/test");
      long fileLength = 1024 * 1024 * 10 * 8;
      DFSTestUtil.createFile(fs, file, fileLength, (short) 1, 0L);
      DFSTestUtil.waitReplication(fs, file, (short) 1);

      DataNode dataNode = cluster.getDataNodes().get(0);
      // DataXceiverServer#readThrottler is null if
      // dfs.datanode.data.read.bandwidthPerSec default value is 0.
      Assert.assertNull(dataNode.xserver.getReadThrottler());

      // Read file.
      Assert.assertEquals(fileLength, DFSTestUtil.readFileAsBytes(fs, file).length);

      // Set dfs.datanode.data.read.bandwidthPerSec.
      long bandwidthPerSec = 1024 * 1024 * 8;
      conf.setLong(DFS_DATANODE_DATA_READ_BANDWIDTHPERSEC_KEY, bandwidthPerSec);

      // Restart the first datanode.
      cluster.stopDataNode(0);
      cluster.startDataNodes(conf, 1, true, null, null);
      dataNode = cluster.getDataNodes().get(0);
      Assert.assertEquals(bandwidthPerSec, dataNode.xserver.getReadThrottler().getBandwidth());

      // Read file with throttler.
      long start = monotonicNow();
      Assert.assertEquals(fileLength, DFSTestUtil.readFileAsBytes(fs, file).length);
      long elapsedTime = monotonicNow() - start;
      // Ensure throttler is effective, read 1024 * 1024 * 10 * 8 bytes,
      // should take approximately 10 seconds (1024 * 1024 * 8 bytes per second).
      long expectedElapsedTime = fileLength / bandwidthPerSec * 1000; // in milliseconds.
      long acceptableError = 1000; // 1 milliseconds, allowing for a small margin of error.
      assertTrue(elapsedTime >= expectedElapsedTime - acceptableError);
    }
  }
}
