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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests snapshot deletion.
 */
public class TestSnapshotDiffReport {
  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem hdfs;
  
  private HashMap<Path, Integer> snapshotNumberMap = new HashMap<Path, Integer>();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private String genSnapshotName(Path snapshotDir) {
    int sNum = -1;
    if (snapshotNumberMap.containsKey(snapshotDir)) {
      sNum = snapshotNumberMap.get(snapshotDir);
    }
    snapshotNumberMap.put(snapshotDir, ++sNum);
    return "s" + sNum;
  }
  
  /**
   * Create/modify/delete files under a given directory, also create snapshots
   * of directories.
   */ 
  private void modifyAndCreateSnapshot(Path modifyDir, Path[] snapshotDirs)
      throws Exception {
    Path file10 = new Path(modifyDir, "file10");
    Path file11 = new Path(modifyDir, "file11");
    Path file12 = new Path(modifyDir, "file12");
    Path file13 = new Path(modifyDir, "file13");
    Path file14 = new Path(modifyDir, "file14");
    Path file15 = new Path(modifyDir, "file15");
    DFSTestUtil.createFile(hdfs, file10, BLOCKSIZE, (short) (REPLICATION - 1),
        seed);
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, (short) (REPLICATION - 1),
        seed);
    DFSTestUtil.createFile(hdfs, file12, BLOCKSIZE, (short) (REPLICATION - 1),
        seed);
    DFSTestUtil.createFile(hdfs, file13, BLOCKSIZE, (short) (REPLICATION - 1),
        seed);
    // create snapshot
    for (Path snapshotDir : snapshotDirs) {
      hdfs.allowSnapshot(snapshotDir.toString());
      hdfs.createSnapshot(snapshotDir, genSnapshotName(snapshotDir));
    }
    
    // delete file11
    hdfs.delete(file11, true);
    // modify file12
    hdfs.setReplication(file12, REPLICATION);
    // modify file13
    hdfs.setReplication(file13, REPLICATION);
    // create file14
    DFSTestUtil.createFile(hdfs, file14, BLOCKSIZE, REPLICATION, seed);
    // create file15
    DFSTestUtil.createFile(hdfs, file15, BLOCKSIZE, REPLICATION, seed);
    
    // create snapshot
    for (Path snapshotDir : snapshotDirs) {
      hdfs.createSnapshot(snapshotDir, genSnapshotName(snapshotDir));
    }
    
    // create file11 again
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION, seed);
    // delete file12
    hdfs.delete(file12, true);
    // modify file13
    hdfs.setReplication(file13, (short) (REPLICATION - 2));
    // delete file14
    hdfs.delete(file14, true);
    // modify file15
    hdfs.setReplication(file15, (short) (REPLICATION - 1));
    
    // create snapshot
    for (Path snapshotDir : snapshotDirs) {
      hdfs.createSnapshot(snapshotDir, genSnapshotName(snapshotDir));
    }
    // modify file10
    hdfs.setReplication(file10, (short) (REPLICATION - 1));
  }
  
  /** check the correctness of the diff reports */
  private void verifyDiffReport(Path dir, String from, String to,
      DiffReportEntry... entries) throws IOException {
    SnapshotDiffReport report = hdfs.getSnapshotDiffReport(dir, from, to);
    // reverse the order of from and to
    SnapshotDiffReport inverseReport = hdfs
        .getSnapshotDiffReport(dir, to, from);
    System.out.println(report.toString());
    System.out.println(inverseReport.toString() + "\n");
    
    assertEquals(entries.length, report.getDiffList().size());
    assertEquals(entries.length, inverseReport.getDiffList().size());
    
    for (DiffReportEntry entry : entries) {
      if (entry.getType() == DiffType.MODIFY) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(entry));
      } else if (entry.getType() == DiffType.DELETE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.CREATE, entry.getFullPath())));
      } else if (entry.getType() == DiffType.CREATE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.DELETE, entry.getFullPath())));
      }
    }
  }
  
  /** Test the computation and representation of diff between snapshots */
//  TODO: fix diff report
//  @Test
  public void testDiffReport() throws Exception {
    Path subsub1 = new Path(sub1, "subsub1");
    Path subsubsub1 = new Path(subsub1, "subsubsub1");
    hdfs.mkdirs(subsubsub1);
    modifyAndCreateSnapshot(sub1, new Path[]{sub1, subsubsub1});
    modifyAndCreateSnapshot(subsubsub1, new Path[]{sub1, subsubsub1});
    
    try {
      hdfs.getSnapshotDiffReport(subsub1, "s1", "s2");
      fail("Expect exception when getting snapshot diff report: " + subsub1
          + " is not a snapshottable directory.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + subsub1, e);
    }
    
    // diff between the same snapshot
    SnapshotDiffReport report = hdfs.getSnapshotDiffReport(sub1, "s0", "s0");
    System.out.println(report);
    assertEquals(0, report.getDiffList().size());
    
    report = hdfs.getSnapshotDiffReport(sub1, "", "");
    System.out.println(report);
    assertEquals(0, report.getDiffList().size());
    
    report = hdfs.getSnapshotDiffReport(subsubsub1, "s0", "s2");
    System.out.println(report);
    assertEquals(0, report.getDiffList().size());
    
    verifyDiffReport(sub1, "s0", "s2", 
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1"),
        new DiffReportEntry(DiffType.CREATE, "/TestSnapshot/sub1/file15"),
        new DiffReportEntry(DiffType.DELETE, "/TestSnapshot/sub1/file12"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file11"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file13"));

    verifyDiffReport(sub1, "s0", "s5", 
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1"),
        new DiffReportEntry(DiffType.CREATE, "/TestSnapshot/sub1/file15"),
        new DiffReportEntry(DiffType.DELETE, "/TestSnapshot/sub1/file12"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file10"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file11"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file13"),
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file10"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file11"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file13"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file15"));
    
    verifyDiffReport(sub1, "s2", "s5",
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1"),
        new DiffReportEntry(DiffType.MODIFY, "/TestSnapshot/sub1/file10"),
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file10"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file11"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file13"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file15"));
    
    verifyDiffReport(sub1, "s3", "",
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1"),
        new DiffReportEntry(DiffType.CREATE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file15"),
        new DiffReportEntry(DiffType.DELETE,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file12"),
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file10"),
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file11"),
        new DiffReportEntry(DiffType.MODIFY,
            "/TestSnapshot/sub1/subsub1/subsubsub1/file13"));
  }
}