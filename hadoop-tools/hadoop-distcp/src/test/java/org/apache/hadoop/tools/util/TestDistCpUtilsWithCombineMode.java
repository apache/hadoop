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

package org.apache.hadoop.tools.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Test length and checksums comparison with checksum combine mode.
 * When the combine mode is COMPOSITE_CRC, it should tolerate different file
 * systems and different block sizes.
 */
public class TestDistCpUtilsWithCombineMode {
  private static final Logger LOG = LoggerFactory.getLogger(TestDistCpUtils.class);

  private Configuration config;
  private MiniDFSCluster cluster;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void create() throws IOException {
    config = new Configuration();
    if (testName.getMethodName().contains("WithCombineMode")) {
      config.set("dfs.checksum.combine.mode", "COMPOSITE_CRC");
    }
    config.setLong(
        DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 512);
    cluster = new MiniDFSCluster.Builder(config)
        .numDataNodes(2)
        .format(true)
        .build();
  }

  @After
  public void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testChecksumsComparisonWithCombineMode() throws IOException {
    try {
      compareSameContentButDiffBlockSizes();
    } catch (IOException e) {
      LOG.error("Unexpected exception is found", e);
      throw e;
    }
  }

  @Test
  public void testChecksumsComparisonWithoutCombineMode() {
    try {
      compareSameContentButDiffBlockSizes();
      Assert.fail("Expected comparison to fail");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Checksum mismatch", e);
    }
  }

  private void compareSameContentButDiffBlockSizes() throws IOException {
    String base = "/tmp/verify-checksum-" + testName.getMethodName() + "/";
    long seed = System.currentTimeMillis();
    short rf = 2;

    FileSystem fs = FileSystem.get(config);
    Path basePath = new Path(base);
    fs.mkdirs(basePath);

    // create 2 files of same content but different block-sizes
    Path src = new Path(base + "src");
    Path dst = new Path(base + "dst");
    DFSTestUtil.createFile(fs, src, 256, 1024, 512,
        rf, seed);
    DFSTestUtil.createFile(fs, dst, 256, 1024, 1024,
        rf, seed);
    // then compare
    DistCpUtils.compareFileLengthsAndChecksums(fs, src,
        null, fs, dst, false);
  }
}
