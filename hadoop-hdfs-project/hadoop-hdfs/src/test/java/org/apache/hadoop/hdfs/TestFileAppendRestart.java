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

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.hadoop.hdfs.server.namenode.FSImageTestUtil;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.Test;

/**
 * Unit test to make sure that Append properly logs the right
 * things to the edit log, such that files aren't lost or truncated
 * on restart.
 */
public class TestFileAppendRestart {
  private static final int BLOCK_SIZE = 4096;
  private static final String HADOOP_23_BROKEN_APPEND_TGZ =
      "image-with-buggy-append.tgz";
    
  private void writeAndAppend(FileSystem fs, Path p,
      int lengthForCreate, int lengthForAppend) throws IOException {
    // Creating a file with 4096 blockSize to write multiple blocks
    FSDataOutputStream stream = fs.create(
        p, true, BLOCK_SIZE, (short) 1, BLOCK_SIZE);
    try {
      AppendTestUtil.write(stream, 0, lengthForCreate);
      stream.close();
      
      stream = fs.append(p);
      AppendTestUtil.write(stream, lengthForCreate, lengthForAppend);
      stream.close();
    } finally {
      IOUtils.closeStream(stream);
    }
    
    int totalLength = lengthForCreate + lengthForAppend; 
    assertEquals(totalLength, fs.getFileStatus(p).getLen());
  }
    
  /**
   * Regression test for HDFS-2991. Creates and appends to files
   * where blocks start/end on block boundaries.
   */
  @Test
  public void testAppendRestart() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    // Turn off persistent IPC, so that the DFSClient can survive NN restart
    conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
        0);
    MiniDFSCluster cluster = null;

    FSDataOutputStream stream = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      FileSystem fs = cluster.getFileSystem();
      File editLog =
        new File(FSImageTestUtil.getNameNodeCurrentDirs(cluster, 0).get(0),
            NNStorage.getInProgressEditsFileName(1));
      EnumMap<FSEditLogOpCodes, Holder<Integer>> counts;
      
      Path p1 = new Path("/block-boundaries");
      writeAndAppend(fs, p1, BLOCK_SIZE, BLOCK_SIZE);

      counts = FSImageTestUtil.countEditLogOpTypes(editLog);
      // OP_ADD to create file
      // OP_ADD_BLOCK for first block
      // OP_CLOSE to close file
      // OP_ADD to reopen file
      // OP_ADD_BLOCK for second block
      // OP_CLOSE to close file
      assertEquals(2, (int)counts.get(FSEditLogOpCodes.OP_ADD).held);
      assertEquals(2, (int)counts.get(FSEditLogOpCodes.OP_ADD_BLOCK).held);
      assertEquals(2, (int)counts.get(FSEditLogOpCodes.OP_CLOSE).held);

      Path p2 = new Path("/not-block-boundaries");
      writeAndAppend(fs, p2, BLOCK_SIZE/2, BLOCK_SIZE);
      counts = FSImageTestUtil.countEditLogOpTypes(editLog);
      // OP_ADD to create file
      // OP_ADD_BLOCK for first block
      // OP_CLOSE to close file
      // OP_ADD to re-establish the lease
      // OP_UPDATE_BLOCKS from the updatePipeline call (increments genstamp of last block)
      // OP_ADD_BLOCK at the start of the second block
      // OP_CLOSE to close file
      // Total: 2 OP_ADDs, 1 OP_UPDATE_BLOCKS, 2 OP_ADD_BLOCKs, and 2 OP_CLOSEs
       //       in addition to the ones above
      assertEquals(2+2, (int)counts.get(FSEditLogOpCodes.OP_ADD).held);
      assertEquals(1, (int)counts.get(FSEditLogOpCodes.OP_UPDATE_BLOCKS).held);
      assertEquals(2+2, (int)counts.get(FSEditLogOpCodes.OP_ADD_BLOCK).held);
      assertEquals(2+2, (int)counts.get(FSEditLogOpCodes.OP_CLOSE).held);
      
      cluster.restartNameNode();
      
      AppendTestUtil.check(fs, p1, 2*BLOCK_SIZE);
      AppendTestUtil.check(fs, p2, 3*BLOCK_SIZE/2);
    } finally {
      IOUtils.closeStream(stream);
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /**
   * Earlier versions of HDFS had a bug (HDFS-2991) which caused
   * append(), when called exactly at a block boundary,
   * to not log an OP_ADD. This ensures that we can read from
   * such buggy versions correctly, by loading an image created
   * using a namesystem image created with 0.23.1-rc2 exhibiting
   * the issue.
   */
  @Test
  public void testLoadLogsFromBuggyEarlierVersions() throws IOException {
    final Configuration conf = new HdfsConfiguration();

    String tarFile = System.getProperty("test.cache.data", "build/test/cache")
      + "/" + HADOOP_23_BROKEN_APPEND_TGZ;
    String testDir = PathUtils.getTestDirName(getClass());
    File dfsDir = new File(testDir, "image-with-buggy-append");
    if (dfsDir.exists() && !FileUtil.fullyDelete(dfsDir)) {
      throw new IOException("Could not delete dfs directory '" + dfsDir + "'");
    }
    FileUtil.unTar(new File(tarFile), new File(testDir));

    File nameDir = new File(dfsDir, "name");
    GenericTestUtils.assertExists(nameDir);

    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, nameDir.getAbsolutePath());

    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0)
      .format(false)
      .manageDataDfsDirs(false)
      .manageNameDfsDirs(false)
      .numDataNodes(0)
      .waitSafeMode(false)
      .startupOption(StartupOption.UPGRADE)
      .build();
    try {
      FileSystem fs = cluster.getFileSystem();
      Path testPath = new Path("/tmp/io_data/test_io_0");
      assertEquals(2*1024*1024, fs.getFileStatus(testPath).getLen());
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Test to append to the file, when one of datanode in the existing pipeline
   * is down.
   */
  @Test
  public void testAppendWithPipelineRecovery() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    FSDataOutputStream out = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).manageDataDfsDirs(true)
          .manageNameDfsDirs(true).numDataNodes(4)
          .racks(new String[] { "/rack1", "/rack1", "/rack1", "/rack2" })
          .build();
      cluster.waitActive();

      DistributedFileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test1");
      
      out = fs.create(path, true, BLOCK_SIZE, (short) 3, BLOCK_SIZE);
      AppendTestUtil.write(out, 0, 1024);
      out.close();

      cluster.stopDataNode(3);
      out = fs.append(path);
      AppendTestUtil.write(out, 1024, 1024);
      out.close();
      
      cluster.restartNameNode(true);
      AppendTestUtil.check(fs, path, 2048);
    } finally {
      IOUtils.closeStream(out);
      if (null != cluster) {
        cluster.shutdown();
      }
    }
  }
}
