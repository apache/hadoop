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

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.TestDFSClientFailover;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

/**
 * Tests state transition from active->standby, and manual failover
 * and failback between two namenodes.
 */
public class TestHAStateTransitions {
  protected static final Log LOG = LogFactory.getLog(
      TestStandbyIsHot.class);
  private static final Path TEST_DIR = new Path("/test");
  private static final Path TEST_FILE_PATH = new Path(TEST_DIR, "foo");
  private static final String TEST_FILE_DATA =
    "Hello state transitioning world";

  /**
   * Test which takes a single node and flip flops between
   * active and standby mode, making sure it doesn't
   * double-play any edits.
   */
  @Test
  public void testTransitionActiveToStandby() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      FileSystem fs = cluster.getFileSystem(0);
      
      fs.mkdirs(TEST_DIR);
      cluster.transitionToStandby(0);
      try {
        fs.mkdirs(new Path("/x"));
        fail("Didn't throw trying to mutate FS in standby state");
      } catch (Throwable t) {
        GenericTestUtils.assertExceptionContains(
            "Operation category WRITE is not supported", t);
      }
      cluster.transitionToActive(0);
      
      // Create a file, then delete the whole directory recursively.
      DFSTestUtil.createFile(fs, new Path(TEST_DIR, "foo"),
          10, (short)1, 1L);
      fs.delete(TEST_DIR, true);
      
      // Now if the standby tries to replay the last segment that it just
      // wrote as active, it would fail since it's trying to create a file
      // in a non-existent directory.
      cluster.transitionToStandby(0);
      cluster.transitionToActive(0);
      
      assertFalse(fs.exists(TEST_DIR));

    } finally {
      cluster.shutdown();
    }
  }
  
  /**
   * Tests manual failover back and forth between two NameNodes.
   */
  @Test
  public void testManualFailoverAndFailback() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .nnTopology(MiniDFSNNTopology.simpleHATopology())
      .numDataNodes(1)
      .build();
    try {
      cluster.waitActive();
      cluster.transitionToActive(0);
      
      LOG.info("Starting with NN 0 active");
      FileSystem fs = TestDFSClientFailover.configureFailoverFs(cluster, conf);
      fs.mkdirs(TEST_DIR);

      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertTrue(fs.exists(TEST_DIR));
      DFSTestUtil.writeFile(fs, TEST_FILE_PATH, TEST_FILE_DATA);

      LOG.info("Failing over to NN 0");
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);
      assertTrue(fs.exists(TEST_DIR));
      assertEquals(TEST_FILE_DATA, 
          DFSTestUtil.readFile(fs, TEST_FILE_PATH));

      LOG.info("Removing test file");
      fs.delete(TEST_DIR, true);
      assertFalse(fs.exists(TEST_DIR));

      LOG.info("Failing over to NN 1");
      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);
      assertFalse(fs.exists(TEST_DIR));

    } finally {
      cluster.shutdown();
    }
  }
}
