/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.slf4j.event.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.test.GenericTestUtils;

/**
 * This test extends TestFSImageWithSnapshot to test
 * enable both fsimage load parallel and fsimage compress.
 */
public class TestFSImageWithSnapshotParallelAndCompress extends TestFSImageWithSnapshot {
  {
    SnapshotTestHelper.disableLogs();
    GenericTestUtils.setLogLevel(INode.LOG, Level.TRACE);
  }

  @Override
  public void createCluster() throws IOException {

    // turn on both parallelization and compression
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY, true);
    conf.set(DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY, GzipCodec.class.getCanonicalName());
    conf.setBoolean(DFSConfigKeys.DFS_IMAGE_PARALLEL_LOAD_KEY, true);
    conf.setInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_INODE_THRESHOLD_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_TARGET_SECTIONS_KEY, 2);
    conf.setInt(DFSConfigKeys.DFS_IMAGE_PARALLEL_THREADS_KEY, 2);

    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_DATANODES).build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }
}
