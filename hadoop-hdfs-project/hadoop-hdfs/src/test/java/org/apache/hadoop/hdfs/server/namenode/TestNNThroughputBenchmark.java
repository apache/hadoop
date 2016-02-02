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

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.ExitUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestNNThroughputBenchmark {

  @BeforeClass
  public static void setUp() {
    ExitUtil.disableSystemExit();
  }

  @After
  public void cleanUp() {
    FileUtil.fullyDeleteContents(new File(MiniDFSCluster.getBaseDirectory()));
  }

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark}.
   */
  @Test
  public void testNNThroughput() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());
    DFSTestUtil.formatNameNode(conf);
    NNThroughputBenchmark.runBenchmark(conf, new String[] {"-op", "all"});
  }

  /**
   * This test runs all benchmarks defined in {@link NNThroughputBenchmark},
   * with explicit local -fs option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputWithFsOption() throws Exception {
    Configuration conf = new HdfsConfiguration();
    File nameDir = new File(MiniDFSCluster.getBaseDirectory(), "name");
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nameDir.getAbsolutePath());
    DFSTestUtil.formatNameNode(conf);
    NNThroughputBenchmark.runBenchmark(conf,
        new String[] {"-fs", "file:///", "-op", "all"});
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster.
   */
  @Test(timeout = 120000)
  public void testNNThroughputAgainstRemoteNN() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      final Configuration benchConf = new HdfsConfiguration();
      FileSystem.setDefaultUri(benchConf, cluster.getURI());
      NNThroughputBenchmark.runBenchmark(benchConf, new String[]{"-op", "all"});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * This test runs {@link NNThroughputBenchmark} against a mini DFS cluster
   * with explicit -fs option.
   */
  @Test(timeout = 120000)
  public void testNNThroughputRemoteAgainstNNWithFsOption() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 16);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(0).build();
      cluster.waitActive();

      NNThroughputBenchmark.runBenchmark(new HdfsConfiguration(),
          new String[]{"-fs", cluster.getURI().toString(), "-op", "all"});
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
