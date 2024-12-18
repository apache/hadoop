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

package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

/**
 * To test {@link FSNLockBenchmarkThroughput}.
 */
public class TestFSNLockBenchmarkThroughput {

  @Test
  public void testFineGrainedLockingBenchmarkThroughput1() throws Exception {
    testBenchmarkThroughput(true, 20, 100, 1000);
  }

  @Test
  public void testFineGrainedLockingBenchmarkThroughput2() throws Exception {
    testBenchmarkThroughput(true, 20, 1000, 1000);
  }

  @Test
  public void testFineGrainedLockingBenchmarkThroughput3() throws Exception {
    testBenchmarkThroughput(true, 10, 100, 100);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput1() throws Exception {
    testBenchmarkThroughput(false, 20, 100, 1000);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput2() throws Exception {
    testBenchmarkThroughput(false, 20, 1000, 1000);
  }

  @Test
  public void testGlobalLockingBenchmarkThroughput3() throws Exception {
    testBenchmarkThroughput(false, 10, 100, 100);
  }

  private void testBenchmarkThroughput(boolean enableFGL, int readWriteRatio,
      int testingCount, int numClients) throws Exception {
    MiniQJMHACluster qjmhaCluster = null;

    try {
      Configuration conf = new HdfsConfiguration();
      conf.setBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY, true);
      conf.setInt(DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY, 500);
      if (enableFGL) {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            FineGrainedFSNamesystemLock.class, FSNLockManager.class);
      } else {
        conf.setClass(DFSConfigKeys.DFS_NAMENODE_LOCK_MODEL_PROVIDER_KEY,
            GlobalFSNamesystemLock.class, FSNLockManager.class);
      }

      MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
      builder.getDfsBuilder().numDataNodes(10);
      qjmhaCluster = builder.build();
      MiniDFSCluster cluster = qjmhaCluster.getDfsCluster();

      cluster.transitionToActive(0);
      cluster.waitActive(0);

      FileSystem fileSystem = cluster.getFileSystem(0);

      String[] args = new String[]{"/tmp/fsnlock/benchmark/throughput",
          String.valueOf(readWriteRatio), String.valueOf(testingCount),
          String.valueOf(numClients)};

      Assert.assertEquals(0, ToolRunner.run(conf,
          new FSNLockBenchmarkThroughput(fileSystem), args));
    } finally {
      if (qjmhaCluster != null) {
        qjmhaCluster.shutdown();
      }
    }
  }
}
