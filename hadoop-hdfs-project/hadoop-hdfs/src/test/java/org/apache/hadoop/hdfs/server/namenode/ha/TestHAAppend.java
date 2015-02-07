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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.TestFileTruncate;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

public class TestHAAppend {
  static final int COUNT = 5;

  static FSDataOutputStream createAndHflush(FileSystem fs, Path file,
      byte[] data, int length) throws IOException{
    FSDataOutputStream out = fs.create(file, false, 4096, (short)3, 1024);
    out.write(data, 0, length);
    out.hflush();
    return out;
  }
  
  /**
   * Test to verify the processing of PendingDataNodeMessageQueue in case of
   * append. One block will marked as corrupt if the OP_ADD, OP_UPDATE_BLOCKS
   * comes in one edit log segment and OP_CLOSE edit comes in next log segment
   * which is loaded during failover. Regression test for HDFS-3605.
   */
  @Test
  public void testMultipleAppendsDuringCatchupTailing() throws Exception {
    Configuration conf = new Configuration();
    
    // Set a length edits tailing period, and explicit rolling, so we can
    // control the ingest of edits by the standby for this test.
    conf.set(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, "5000");
    conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, -1);

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology())
        .numDataNodes(3).build();
    FileSystem fs = null;
    try {
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);

      Path fileToAppend = new Path("/FileToAppend");
      Path fileToTruncate = new Path("/FileToTruncate");
      
      final byte[] data = new byte[1 << 16];
      DFSUtil.getRandom().nextBytes(data);
      final int[] appendPos = AppendTestUtil.randomFilePartition(
          data.length, COUNT);
      final int[] truncatePos = AppendTestUtil.randomFilePartition(
          data.length, 1);

      // Create file, write some data, and hflush so that the first
      // block is in the edit log prior to roll.
      FSDataOutputStream out = createAndHflush(
          fs, fileToAppend, data, appendPos[0]);

      FSDataOutputStream out4Truncate = createAndHflush(
          fs, fileToTruncate, data, data.length);
      
      // Let the StandbyNode catch the creation of the file. 
      cluster.getNameNode(0).getRpcServer().rollEditLog();
      cluster.getNameNode(1).getNamesystem().getEditLogTailer().doTailEdits();
      out.close();
      out4Truncate.close();

      // Append and re-close a few time, so that many block entries are queued.
      for (int i = 0; i < COUNT; i++) {
        int end = i < COUNT - 1? appendPos[i + 1]: data.length;
        out = fs.append(fileToAppend);
        out.write(data, appendPos[i], end - appendPos[i]);
        out.close();
      }
      boolean isTruncateReady = fs.truncate(fileToTruncate, truncatePos[0]);

      // Ensure that blocks have been reported to the SBN ahead of the edits
      // arriving.
      cluster.triggerBlockReports();

      // Failover the current standby to active.
      cluster.shutdownNameNode(0);
      cluster.transitionToActive(1);
      
      // Check the FSCK doesn't detect any bad blocks on the SBN.
      int rc = ToolRunner.run(new DFSck(cluster.getConfiguration(1)),
          new String[] { "/", "-files", "-blocks" });
      assertEquals(0, rc);
      
      assertEquals("CorruptBlocks should be empty.", 0, cluster.getNameNode(1)
          .getNamesystem().getCorruptReplicaBlocks());

      AppendTestUtil.checkFullFile(fs, fileToAppend, data.length, data,
          fileToAppend.toString());

      if (!isTruncateReady) {
        TestFileTruncate.checkBlockRecovery(fileToTruncate,
            cluster.getFileSystem(1));
      }
      AppendTestUtil.checkFullFile(fs, fileToTruncate, truncatePos[0], data,
          fileToTruncate.toString());
    } finally {
      if (null != cluster) {
        cluster.shutdown();
      }
      if (null != fs) {
        fs.close();
      }
    }
  }
}
