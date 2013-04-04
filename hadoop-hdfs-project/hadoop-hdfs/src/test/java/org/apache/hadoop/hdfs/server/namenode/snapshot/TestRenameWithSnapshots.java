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

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffReportEntry;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Testing rename with snapshots. */
public class TestRenameWithSnapshots {
  {
    SnapshotTestHelper.disableLogs();
  }
  private static final Log LOG = LogFactory.getLog(TestRenameWithSnapshots.class);
  
  private static final long SEED = 0;
  private static final short REPL = 3;
  private static final long BLOCKSIZE = 1024;
  
  private static Configuration conf = new Configuration();
  private static MiniDFSCluster cluster;
  private static FSNamesystem fsn;
  private static FSDirectory fsdir;
  private static DistributedFileSystem hdfs;
  
  static private final Path dir = new Path("/testRenameWithSnapshots");
  static private final Path sub1 = new Path(dir, "sub1");
  static private final Path file1 = new Path(sub1, "file1");
  static private final Path file2 = new Path(sub1, "file2");
  static private final Path file3 = new Path(sub1, "file3");
  static private final String snap1 = "snap1";
  static private final String snap2 = "snap2";

  
  @Before
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPL).format(true)
        .build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fsdir = fsn.getFSDirectory();

    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test (timeout=300000)
  public void testRenameWithSnapshot() throws Exception {
    final String dirStr = "/testRenameWithSnapshot";
    final String abcStr = dirStr + "/abc";
    final Path abc = new Path(abcStr);
    hdfs.mkdirs(abc, new FsPermission((short)0777));
    hdfs.allowSnapshot(abcStr);

    final Path foo = new Path(abc, "foo");
    DFSTestUtil.createFile(hdfs, foo, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(abc, "s0");
    final INode fooINode = fsdir.getINode(foo.toString());

    final String xyzStr = dirStr + "/xyz";
    final Path xyz = new Path(xyzStr);
    hdfs.mkdirs(xyz, new FsPermission((short)0777));
    final Path bar = new Path(xyz, "bar");
    hdfs.rename(foo, bar);
    
    final INode fooRef = fsdir.getINode(
        SnapshotTestHelper.getSnapshotPath(abc, "s0", "foo").toString());
    Assert.assertTrue(fooRef.isReference());
    Assert.assertTrue(fooRef.asReference() instanceof INodeReference.WithName);

    final INodeReference.WithCount withCount
        = (INodeReference.WithCount)fooRef.asReference().getReferredINode();
    Assert.assertEquals(2, withCount.getReferenceCount());

    final INode barRef = fsdir.getINode(bar.toString());
    Assert.assertTrue(barRef.isReference());

    Assert.assertSame(withCount, barRef.asReference().getReferredINode());
    Assert.assertSame(fooINode, withCount.asReference().getReferredINode());
    
    hdfs.delete(bar, false);
    Assert.assertEquals(1, withCount.getReferenceCount());
  }
  
  private static boolean existsInDiffReport(List<DiffReportEntry> entries,
      DiffType type, String relativePath) {
    for (DiffReportEntry entry : entries) {
      System.out.println("DiffEntry is:" + entry.getType() + "\""
          + new String(entry.getRelativePath()) + "\"");
      if ((entry.getType() == type)
          && ((new String(entry.getRelativePath())).compareTo(relativePath) == 0)) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Rename a file under a snapshottable directory, file does not exist
   * in a snapshot.
   */
  @Test (timeout=60000)
  public void testRenameFileNotInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    hdfs.createSnapshot(sub1, snap1);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 2);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
  }

  /**
   * Rename a file under a snapshottable directory, file exists
   * in a snapshot.
   */
  @Test (timeout=60000)
  public void testRenameFileInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(sub1, snap1);
    hdfs.rename(file1, file2);

    // Query the diff report and make sure it looks as expected.
    SnapshotDiffReport diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    System.out.println("DiffList is " + diffReport.toString());
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
  }

  @Test (timeout=60000)
  public void testRenameTwiceInSnapshot() throws Exception {
    hdfs.mkdirs(sub1);
    hdfs.allowSnapshot(sub1.toString());
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPL, SEED);
    hdfs.createSnapshot(sub1, snap1);
    hdfs.rename(file1, file2);
    
    hdfs.createSnapshot(sub1, snap2);
    hdfs.rename(file2, file3);

    SnapshotDiffReport diffReport;
    
    // Query the diff report and make sure it looks as expected.
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, snap2);
    LOG.info("DiffList is " + diffReport.toString());
    List<DiffReportEntry> entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file2.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap2, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file3.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file2.getName()));
    
    diffReport = hdfs.getSnapshotDiffReport(sub1, snap1, "");
    LOG.info("DiffList is " + diffReport.toString());
    entries = diffReport.getDiffList();
    assertTrue(entries.size() == 3);
    assertTrue(existsInDiffReport(entries, DiffType.MODIFY, ""));
    assertTrue(existsInDiffReport(entries, DiffType.CREATE, file3.getName()));
    assertTrue(existsInDiffReport(entries, DiffType.DELETE, file1.getName()));
  }
}
