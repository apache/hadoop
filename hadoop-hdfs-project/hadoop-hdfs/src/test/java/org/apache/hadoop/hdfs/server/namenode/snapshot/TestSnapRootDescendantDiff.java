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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.fail;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test snapshot diff report for the snapshot root descendant directory.
 */
public class TestSnapRootDescendantDiff extends TestSnapshotDiffReport {
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_CAPTURE_OPENFILES, true);
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_SKIP_CAPTURE_ACCESSTIME_ONLY_CHANGE,
        true);
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_SNAPSHOT_DIFF_ALLOW_SNAP_ROOT_DESCENDANT,
        false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3)
        .format(true).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testNonSnapRootDiffReport() throws Exception {
    Path subsub1 = new Path(getSnapRootDir(), "subsub1");
    Path subsubsub1 = new Path(subsub1, "subsubsub1");
    hdfs.mkdirs(subsubsub1);
    modifyAndCreateSnapshot(getSnapRootDir(), new Path[]{getSnapRootDir()});
    modifyAndCreateSnapshot(subsubsub1, new Path[]{getSnapRootDir()});

    try {
      hdfs.getSnapshotDiffReport(subsub1, "s1", "s2");
      fail("Expect exception when getting snapshot diff report: " + subsub1
          + " is not a snapshottable directory.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + subsub1, e);
    }
  }

}