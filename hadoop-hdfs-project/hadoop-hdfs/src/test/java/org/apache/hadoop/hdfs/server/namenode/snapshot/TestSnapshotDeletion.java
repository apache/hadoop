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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.RemoteException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests snapshot deletion.
 */
public class TestSnapshotDeletion {
  protected static final long seed = 0;
  protected static final short REPLICATION = 3;
  protected static final long BLOCKSIZE = 1024;
  public static final int SNAPSHOTNUMBER = 10;
  
  private final Path dir = new Path("/TestSnapshot");
  private final Path sub1 = new Path(dir, "sub1");
  private final Path subsub1 = new Path(sub1, "subsub1");
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected FSNamesystem fsn;
  protected DistributedFileSystem hdfs;
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
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
   * Deleting snapshottable directory with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for sub1, and create snapshot for it
    hdfs.allowSnapshot(sub1.toString());
    hdfs.createSnapshot("s1", sub1.toString());

    // Deleting a snapshottable dir with snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + sub1.toString()
        + " cannot be deleted since " + sub1.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(sub1, true);
  }
  
  /**
   * Deleting directory with snapshottable descendant with snapshots must fail.
   */
  @Test
  public void testDeleteDirectoryWithSnapshot2() throws Exception {
    Path file0 = new Path(sub1, "file0");
    Path file1 = new Path(sub1, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);
    
    Path subfile1 = new Path(subsub1, "file0");
    Path subfile2 = new Path(subsub1, "file1");
    DFSTestUtil.createFile(hdfs, subfile1, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, subfile2, BLOCKSIZE, REPLICATION, seed);

    // Allow snapshot for subsub1, and create snapshot for it
    hdfs.allowSnapshot(subsub1.toString());
    hdfs.createSnapshot("s1", subsub1.toString());

    // Deleting dir while its descedant subsub1 having snapshots should fail
    exception.expect(RemoteException.class);
    String error = "The direcotry " + dir.toString()
        + " cannot be deleted since " + subsub1.toString()
        + " is snapshottable and already has snapshots";
    exception.expectMessage(error);
    hdfs.delete(dir, true);
  }
}
