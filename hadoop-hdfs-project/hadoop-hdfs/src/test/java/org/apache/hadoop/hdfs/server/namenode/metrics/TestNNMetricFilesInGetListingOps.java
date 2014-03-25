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
 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.metrics;

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for FilesInGetListingOps metric in Namenode
 */
public class TestNNMetricFilesInGetListingOps {
  private static final Configuration CONF = new HdfsConfiguration();
  private static final String NN_METRICS = "NameNodeActivity";
  static {
    CONF.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 100);
    CONF.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, 1);
    CONF.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    CONF.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
  }
     
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private final Random rand = new Random();

  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(CONF).build();
    cluster.waitActive();
    cluster.getNameNode();
    fs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  /** create a file with a length of <code>fileLen</code> */
  private void createFile(String fileName, long fileLen, short replicas) throws IOException {
    Path filePath = new Path(fileName);
    DFSTestUtil.createFile(fs, filePath, fileLen, replicas, rand.nextLong());
  }
     

  @Test
  public void testFilesInGetListingOps() throws Exception {
    createFile("/tmp1/t1", 3200, (short)3);
    createFile("/tmp1/t2", 3200, (short)3);
    createFile("/tmp2/t1", 3200, (short)3);
    createFile("/tmp2/t2", 3200, (short)3);
    cluster.getNameNodeRpc().getListing("/tmp1", HdfsFileStatus.EMPTY_NAME, false);
    assertCounter("FilesInGetListingOps", 2L, getMetrics(NN_METRICS));
    cluster.getNameNodeRpc().getListing("/tmp2", HdfsFileStatus.EMPTY_NAME, false);
    assertCounter("FilesInGetListingOps", 4L, getMetrics(NN_METRICS));
  }
}

