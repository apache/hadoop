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
import org.apache.hadoop.hdfs.DFSUtil;
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
  protected static final short REPLICATION_1 = 2;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected DistributedFileSystem hdfs;
  
  private final HashMap<Path, Integer> snapshotNumberMap = new HashMap<Path, Integer>();

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
    Path link13 = new Path(modifyDir, "link13");
    Path file14 = new Path(modifyDir, "file14");
    Path file15 = new Path(modifyDir, "file15");
    DFSTestUtil.createFile(hdfs, file10, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file11, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file12, BLOCKSIZE, REPLICATION_1, seed);
    DFSTestUtil.createFile(hdfs, file13, BLOCKSIZE, REPLICATION_1, seed);
    // create link13
    hdfs.createSymlink(file13, link13, false);
    // create snapshot
    for (Path snapshotDir : snapshotDirs) {
      hdfs.allowSnapshot(snapshotDir);
      hdfs.createSnapshot(snapshotDir, genSnapshotName(snapshotDir));
    }
    
    // delete file11
    hdfs.delete(file11, true);
    // modify file12
    hdfs.setReplication(file12, REPLICATION);
    // modify file13
    hdfs.setReplication(file13, REPLICATION);
    // delete link13
    hdfs.delete(link13, false);
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
    // create link13 again
    hdfs.createSymlink(file13, link13, false);
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
            new DiffReportEntry(DiffType.CREATE, entry.getRelativePath())));
      } else if (entry.getType() == DiffType.CREATE) {
        assertTrue(report.getDiffList().contains(entry));
        assertTrue(inverseReport.getDiffList().contains(
            new DiffReportEntry(DiffType.DELETE, entry.getRelativePath())));
      }
    }
  }
  
  /** Test the computation and representation of diff between snapshots */
  @Test (timeout=60000)
  public void testDiffReport() throws Exception {
    cluster.getNamesystem().getSnapshotManager().setAllowNestedSnapshots(true);

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
    
    final String invalidName = "invalid";
    try {
      hdfs.getSnapshotDiffReport(sub1, invalidName, invalidName);
      fail("Expect exception when providing invalid snapshot name for diff report");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains(
          "Cannot find the snapshot of directory " + sub1 + " with name "
              + invalidName, e);
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

    // test path with scheme also works
    report = hdfs.getSnapshotDiffReport(hdfs.makeQualified(subsubsub1), "s0", "s2");
    System.out.println(report);
    assertEquals(0, report.getDiffList().size());

    verifyDiffReport(sub1, "s0", "s2", 
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("file15")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("file12")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("file11")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("file11")),
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("file13")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("link13")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("link13")));

    verifyDiffReport(sub1, "s0", "s5", 
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("file15")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("file12")),
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("file10")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("file11")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("file11")),
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("file13")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("link13")),
        new DiffReportEntry(DiffType.CREATE, DFSUtil.string2Bytes("link13")),
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file10")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file15")));
    
    verifyDiffReport(sub1, "s2", "s5",
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("file10")),
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file10")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file15")));
    
    verifyDiffReport(sub1, "s3", "",
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file15")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file12")),
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file10")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")));
  }
  
  /**
   * Make changes under a sub-directory, then delete the sub-directory. Make
   * sure the diff report computation correctly retrieve the diff from the
   * deleted sub-directory.
   */
  @Test (timeout=60000)
  public void testDiffReport2() throws Exception {
    Path subsub1 = new Path(sub1, "subsub1");
    Path subsubsub1 = new Path(subsub1, "subsubsub1");
    hdfs.mkdirs(subsubsub1);
    modifyAndCreateSnapshot(subsubsub1, new Path[]{sub1});
    
    // delete subsub1
    hdfs.delete(subsub1, true);
    // check diff report between s0 and s2
    verifyDiffReport(sub1, "s0", "s2", 
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1")), 
        new DiffReportEntry(DiffType.CREATE, 
            DFSUtil.string2Bytes("subsub1/subsubsub1/file15")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file12")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file11")),
        new DiffReportEntry(DiffType.MODIFY,
            DFSUtil.string2Bytes("subsub1/subsubsub1/file13")),
        new DiffReportEntry(DiffType.CREATE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")),
        new DiffReportEntry(DiffType.DELETE,
            DFSUtil.string2Bytes("subsub1/subsubsub1/link13")));
    // check diff report between s0 and the current status
    verifyDiffReport(sub1, "s0", "", 
        new DiffReportEntry(DiffType.MODIFY, DFSUtil.string2Bytes("")),
        new DiffReportEntry(DiffType.DELETE, DFSUtil.string2Bytes("subsub1")));
  }
  
}