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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class TestFSEditLogLoader {
  
  private static final int NUM_DATA_NODES = 0;
  
  @Test
  public void testDisplayRecentEditLogOpCodes() throws IOException {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    FileSystem fileSys = null;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
        .build();
    cluster.waitActive();
    fileSys = cluster.getFileSystem();
    final FSNamesystem namesystem = cluster.getNamesystem();

    FSImage fsimage = namesystem.getFSImage();
    final FSEditLog editLog = fsimage.getEditLog();
    for (int i = 0; i < 20; i++) {
      fileSys.mkdirs(new Path("/tmp/tmp" + i));
    }
    File editFile = editLog.getFsEditName();
    editLog.close();
    cluster.shutdown();
    
    // Corrupt the edits file.
    long fileLen = editFile.length();
    RandomAccessFile rwf = new RandomAccessFile(editFile, "rw");
    rwf.seek(fileLen - 40);
    for (int i = 0; i < 20; i++) {
      rwf.write(FSEditLogOpCodes.OP_DELETE.getOpCode());
    }
    rwf.close();
    
    String expectedErrorMessage = "^Error replaying edit log at offset \\d+\n";
    expectedErrorMessage += "Recent opcode offsets: (\\d+\\s*){4}$";
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATA_NODES)
          .format(false).build();
      fail("should not be able to start");
    } catch (IOException e) {
      assertTrue("error message contains opcodes message",
          e.getMessage().matches(expectedErrorMessage));
    }
  }
  
  /**
   * Test that, if the NN restarts with a new minimum replication,
   * any files created with the old replication count will get
   * automatically bumped up to the new minimum upon restart.
   */
  @Test
  public void testReplicationAdjusted() throws IOException {
    // start a cluster 
    Configuration conf = new HdfsConfiguration();
    // Replicate and heartbeat fast to shave a few seconds off test
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
  
      // Create a file with replication count 1
      Path p = new Path("/testfile");
      DFSTestUtil.createFile(fs, p, 10, /*repl*/ (short)1, 1);
      DFSTestUtil.waitReplication(fs, p, (short)1);
  
      // Shut down and restart cluster with new minimum replication of 2
      cluster.shutdown();
      cluster = null;
      
      conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);
  
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(2)
        .format(false).build();
      cluster.waitActive();
      fs = cluster.getFileSystem();
      
      // The file should get adjusted to replication 2 when
      // the edit log is replayed.
      DFSTestUtil.waitReplication(fs, p, (short)2);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
