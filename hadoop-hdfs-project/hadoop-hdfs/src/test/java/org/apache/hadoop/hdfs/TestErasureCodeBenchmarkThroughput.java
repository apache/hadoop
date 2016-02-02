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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Timeout;

import java.io.IOException;

/**
 * To test {@link org.apache.hadoop.hdfs.ErasureCodeBenchmarkThroughput}.
 */
public class TestErasureCodeBenchmarkThroughput {
  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @BeforeClass
  public static void setup() throws IOException {
    conf = new HdfsConfiguration();
    int numDN = ErasureCodeBenchmarkThroughput.getEcPolicy().getNumDataUnits() +
        ErasureCodeBenchmarkThroughput.getEcPolicy().getNumParityUnits();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDN).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() {
    if (cluster != null) {
      cluster.shutdown(true);
    }
  }

  private static void runBenchmark(String[] args) throws Exception {
    Assert.assertNotNull(conf);
    Assert.assertNotNull(fs);
    Assert.assertEquals(0, ToolRunner.run(conf,
        new ErasureCodeBenchmarkThroughput(fs), args));
  }

  private static void verifyNumFile(final int dataSize, final boolean isEc,
      int numFile) throws IOException {
    Path path = isEc ? new Path(ErasureCodeBenchmarkThroughput.EC_DIR) :
        new Path(ErasureCodeBenchmarkThroughput.REP_DIR);
    FileStatus[] statuses = fs.listStatus(path, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.toString().contains(
            ErasureCodeBenchmarkThroughput.getFilePath(dataSize, isEc));
      }
    });
    Assert.assertEquals(numFile, statuses.length);
  }

  @Test
  public void testReplicaReadWrite() throws Exception {
    Integer dataSize = 10;
    Integer numClient = 3;
    String[] args = new String[]{"write", dataSize.toString(), "rep",
        numClient.toString()};
    runBenchmark(args);
    args[0] = "gen";
    runBenchmark(args);
    args[0] = "read";
    runBenchmark(args);
  }

  @Test
  public void testECReadWrite() throws Exception {
    Integer dataSize = 5;
    Integer numClient = 5;
    String[] args = new String[]{"write", dataSize.toString(), "ec",
        numClient.toString()};
    runBenchmark(args);
    args[0] = "gen";
    runBenchmark(args);
    args[0] = "read";
    runBenchmark(args);
  }

  @Test
  public void testCleanUp() throws Exception {
    Integer dataSize = 5;
    Integer numClient = 5;
    String[] args = new String[]{"gen", dataSize.toString(), "ec",
        numClient.toString()};
    runBenchmark(args);
    args[0] = "clean";
    runBenchmark(args);
    verifyNumFile(dataSize, true, 0);
  }
}
