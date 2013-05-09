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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the snapshot-related metrics
 */
public class TestSnapshotMetrics {
  
  private static final long seed = 0;
  private static final short REPLICATION = 3;
  private static final String NN_METRICS = "NameNodeActivity";
  private static final String NS_METRICS = "FSNamesystem";
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");

  private final Path file1 = new Path(sub1, "file1");
  private final Path file2 = new Path(sub1, "file2");
  
  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem hdfs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(REPLICATION)
      .build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
    
    DFSTestUtil.createFile(hdfs, file1, 1024, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, 1024, REPLICATION, seed);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Test the metric SnapshottableDirectories, AllowSnapshotOps,
   * DisallowSnapshotOps, and listSnapshottableDirOps
   */
  @Test
  public void testSnapshottableDirs() throws Exception {
    cluster.getNamesystem().getSnapshotManager().setAllowNestedSnapshots(true);

    assertGauge("SnapshottableDirectories", 0, getMetrics(NS_METRICS));
    assertCounter("AllowSnapshotOps", 0L, getMetrics(NN_METRICS));
    assertCounter("DisallowSnapshotOps", 0L, getMetrics(NN_METRICS));
    
    // Allow snapshots for directories, and check the metrics
    hdfs.allowSnapshot(sub1);
    assertGauge("SnapshottableDirectories", 1, getMetrics(NS_METRICS));
    assertCounter("AllowSnapshotOps", 1L, getMetrics(NN_METRICS));
    
    Path sub2 = new Path(dir, "sub2");
    Path file = new Path(sub2, "file");
    DFSTestUtil.createFile(hdfs, file, 1024, REPLICATION, seed);
    hdfs.allowSnapshot(sub2);
    assertGauge("SnapshottableDirectories", 2, getMetrics(NS_METRICS));
    assertCounter("AllowSnapshotOps", 2L, getMetrics(NN_METRICS));
    
    Path subsub1 = new Path(sub1, "sub1sub1");
    Path subfile = new Path(subsub1, "file");
    DFSTestUtil.createFile(hdfs, subfile, 1024, REPLICATION, seed);
    hdfs.allowSnapshot(subsub1);
    assertGauge("SnapshottableDirectories", 3, getMetrics(NS_METRICS));
    assertCounter("AllowSnapshotOps", 3L, getMetrics(NN_METRICS));
    
    // Set an already snapshottable directory to snapshottable, should not
    // change the metrics
    hdfs.allowSnapshot(sub1);
    assertGauge("SnapshottableDirectories", 3, getMetrics(NS_METRICS));
    // But the number of allowSnapshot operations still increases
    assertCounter("AllowSnapshotOps", 4L, getMetrics(NN_METRICS));
    
    // Disallow the snapshot for snapshottable directories, then check the
    // metrics again
    hdfs.disallowSnapshot(sub1);
    assertGauge("SnapshottableDirectories", 2, getMetrics(NS_METRICS));
    assertCounter("DisallowSnapshotOps", 1L, getMetrics(NN_METRICS));
    
    // delete subsub1, snapshottable directories should be 1
    hdfs.delete(subsub1, true);
    assertGauge("SnapshottableDirectories", 1, getMetrics(NS_METRICS));
    
    // list all the snapshottable directories
    SnapshottableDirectoryStatus[] status = hdfs.getSnapshottableDirListing();
    assertEquals(1, status.length);
    assertCounter("ListSnapshottableDirOps", 1L, getMetrics(NN_METRICS));
  }
  
  /**
   * Test the metrics Snapshots, CreateSnapshotOps, DeleteSnapshotOps,
   * RenameSnapshotOps
   */
  @Test
  public void testSnapshots() throws Exception {
    cluster.getNamesystem().getSnapshotManager().setAllowNestedSnapshots(true);

    assertGauge("Snapshots", 0, getMetrics(NS_METRICS));
    assertCounter("CreateSnapshotOps", 0L, getMetrics(NN_METRICS));
    
    // Create a snapshot for a non-snapshottable directory, thus should not
    // change the metrics
    try {
      hdfs.createSnapshot(sub1, "s1");
    } catch (Exception e) {}
    assertGauge("Snapshots", 0, getMetrics(NS_METRICS));
    assertCounter("CreateSnapshotOps", 1L, getMetrics(NN_METRICS));
    
    // Create snapshot for sub1
    hdfs.allowSnapshot(sub1);
    hdfs.createSnapshot(sub1, "s1");
    assertGauge("Snapshots", 1, getMetrics(NS_METRICS));
    assertCounter("CreateSnapshotOps", 2L, getMetrics(NN_METRICS));
    hdfs.createSnapshot(sub1, "s2");
    assertGauge("Snapshots", 2, getMetrics(NS_METRICS));
    assertCounter("CreateSnapshotOps", 3L, getMetrics(NN_METRICS));
    hdfs.getSnapshotDiffReport(sub1, "s1", "s2");
    assertCounter("SnapshotDiffReportOps", 1L, getMetrics(NN_METRICS));
    
    // Create snapshot for a directory under sub1
    Path subsub1 = new Path(sub1, "sub1sub1");
    Path subfile = new Path(subsub1, "file");
    DFSTestUtil.createFile(hdfs, subfile, 1024, REPLICATION, seed);
    hdfs.allowSnapshot(subsub1);
    hdfs.createSnapshot(subsub1, "s11");
    assertGauge("Snapshots", 3, getMetrics(NS_METRICS));
    assertCounter("CreateSnapshotOps", 4L, getMetrics(NN_METRICS));
    
    // delete snapshot
    hdfs.deleteSnapshot(sub1, "s2");
    assertGauge("Snapshots", 2, getMetrics(NS_METRICS));
    assertCounter("DeleteSnapshotOps", 1L, getMetrics(NN_METRICS));
    
    // rename snapshot
    hdfs.renameSnapshot(sub1, "s1", "NewS1");
    assertGauge("Snapshots", 2, getMetrics(NS_METRICS));
    assertCounter("RenameSnapshotOps", 1L, getMetrics(NN_METRICS));
  }
}