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
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSnapshottableDirListing {

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final long BLOCKSIZE = 1024;

  private final Path dir1 = new Path("/TestSnapshot1");
  private final Path dir2 = new Path("/TestSnapshot2");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();
    fsn = cluster.getNamesystem();
    hdfs = cluster.getFileSystem();
    hdfs.mkdirs(dir1);
    hdfs.mkdirs(dir2);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  /**
   * Test listing all the snapshottable directories
   */
  @Test
  public void testListSnapshottableDir() throws Exception {
    // Initially there is no snapshottable directories in the system
    SnapshottableDirectoryStatus[] dirs = hdfs.getSnapshottableDirListing();
    assertNull(dirs);
    
    // Make dir1 as snapshottable
    hdfs.allowSnapshot(dir1.toString());
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(1, dirs.length);
    assertEquals(dir1.getName(), dirs[0].getDirStatus().getLocalName());
    assertEquals(dir1, dirs[0].getFullPath());
    // There is no snapshot for dir1 yet
    assertEquals(0, dirs[0].getSnapshotNumber());
    
    // Make dir2 as snapshottable
    hdfs.allowSnapshot(dir2.toString());
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(2, dirs.length);
    assertEquals(dir1.getName(), dirs[0].getDirStatus().getLocalName());
    assertEquals(dir1, dirs[0].getFullPath());
    assertEquals(dir2.getName(), dirs[1].getDirStatus().getLocalName());
    assertEquals(dir2, dirs[1].getFullPath());
    // There is no snapshot for dir2 yet
    assertEquals(0, dirs[1].getSnapshotNumber());
    
    // Create dir3
    final Path dir3 = new Path("/TestSnapshot3");
    hdfs.mkdirs(dir3);
    // Rename dir3 to dir2
    hdfs.rename(dir3, dir2, Rename.OVERWRITE);
    // Now we only have one snapshottable dir: dir1
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(1, dirs.length);
    assertEquals(dir1, dirs[0].getFullPath());
    
    // Make dir2 snapshottable again
    hdfs.allowSnapshot(dir2.toString());
    // Create a snapshot for dir2
    hdfs.createSnapshot("s1", dir2.toString());
    hdfs.createSnapshot("s2", dir2.toString());
    dirs = hdfs.getSnapshottableDirListing();
    // There are now 2 snapshots for dir2
    assertEquals(dir2, dirs[1].getFullPath());
    assertEquals(2, dirs[1].getSnapshotNumber());
    
    // Create sub-dirs under dir1
    Path sub1 = new Path(dir1, "sub1");
    Path file1 =  new Path(sub1, "file1");
    Path sub2 = new Path(dir1, "sub2");
    Path file2 =  new Path(sub2, "file2");
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file2, BLOCKSIZE, REPLICATION, seed);
    // Make sub1 and sub2 snapshottable
    hdfs.allowSnapshot(sub1.toString());
    hdfs.allowSnapshot(sub2.toString());
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(4, dirs.length);
    assertEquals(dir1, dirs[0].getFullPath());
    assertEquals(dir2, dirs[1].getFullPath());
    assertEquals(sub1, dirs[2].getFullPath());
    assertEquals(sub2, dirs[3].getFullPath());
    
    // reset sub1
    hdfs.disallowSnapshot(sub1.toString());
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(3, dirs.length);
    assertEquals(dir1, dirs[0].getFullPath());
    assertEquals(dir2, dirs[1].getFullPath());
    assertEquals(sub2, dirs[2].getFullPath());
    
    // Remove dir1, both dir1 and sub2 will be removed
    hdfs.delete(dir1, true);
    dirs = hdfs.getSnapshottableDirListing();
    assertEquals(1, dirs.length);
    assertEquals(dir2.getName(), dirs[0].getDirStatus().getLocalName());
    assertEquals(dir2, dirs[0].getFullPath());
  }
}
