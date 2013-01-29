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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable.SnapshotDiffReport;
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
  protected FSNamesystem fsn;
  protected DistributedFileSystem hdfs;
  
  private int snapshotNum = 0;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .format(true).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /** 
   * Create/modify/delete files and create snapshots under a given directory. 
   */ 
  private void modifyAndCreateSnapshot(Path modifyDir, Path snapshotDir)
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
    // create snapshot s1
    hdfs.allowSnapshot(snapshotDir.toString());
    hdfs.createSnapshot(snapshotDir, "s" + snapshotNum++);
    
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
    
    // create snapshot s2
    hdfs.createSnapshot(snapshotDir, "s" + snapshotNum++);
    
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
    
    // create snapshot s3 for dir
    hdfs.createSnapshot(snapshotDir, "s" + snapshotNum++);
    // modify file10
    hdfs.setReplication(file10, (short) (REPLICATION - 1));
  }
  
  /**
   * Test the functionality of
   * {@link FSNamesystem#getSnapshotDiffReport(String, String, String)}.
   * TODO: without the definision of a DiffReport class, this test temporarily 
   * check the output string of {@link SnapshotDiffReport#dump()} 
   */
  @Test
  public void testDiff() throws Exception {
    modifyAndCreateSnapshot(sub1, sub1);
    modifyAndCreateSnapshot(new Path(sub1, "subsub1/subsubsub1"), sub1);
    
    SnapshotDiffReport diffs = fsn.getSnapshotDiffReport(sub1.toString(), "s0", "s2");
    String diffStr = diffs.dump();
    System.out.println(diffStr);

    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/file15"));
    assertTrue(diffStr.contains("-\t/TestSnapshot/sub1/file12"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file11"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file13"));
    assertFalse(diffStr.contains("file14"));

    diffs = fsn.getSnapshotDiffReport(sub1.toString(), "s0", "s5");
    diffStr = diffs.dump();
    System.out.println(diffStr);
    
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/file15"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/subsub1"));
    assertTrue(diffStr.contains("-\t/TestSnapshot/sub1/file12"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file10"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file11"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file13"));
    assertFalse(diffStr.contains("file14"));
    
    diffs = fsn.getSnapshotDiffReport(sub1.toString(), "s0", "");
    diffStr = diffs.dump();
    System.out.println(diffStr);
    
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/file15"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/subsub1"));
    assertTrue(diffStr.contains("-\t/TestSnapshot/sub1/file12"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file10"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file11"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file13"));
    assertFalse(diffStr.contains("file14"));
    
    diffs = fsn.getSnapshotDiffReport(sub1.toString(), "s2", "s5");
    diffStr = diffs.dump();
    System.out.println(diffStr);
    
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1"));
    assertTrue(diffStr.contains("+\t/TestSnapshot/sub1/subsub1"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/file10"));
    
    diffs = fsn.getSnapshotDiffReport(sub1.toString(), "s3", "");
    diffStr = diffs.dump();
    System.out.println(diffStr);
    
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1"));
    assertTrue(diffStr.contains("M\t/TestSnapshot/sub1/subsub1/subsubsub1"));
    assertTrue(diffStr
        .contains("+\t/TestSnapshot/sub1/subsub1/subsubsub1/file15"));
    assertTrue(diffStr
        .contains("-\t/TestSnapshot/sub1/subsub1/subsubsub1/file12"));
    assertTrue(diffStr
        .contains("M\t/TestSnapshot/sub1/subsub1/subsubsub1/file10"));
    assertTrue(diffStr
        .contains("M\t/TestSnapshot/sub1/subsub1/subsubsub1/file11"));
    assertTrue(diffStr
        .contains("M\t/TestSnapshot/sub1/subsub1/subsubsub1/file13"));
  }
}
