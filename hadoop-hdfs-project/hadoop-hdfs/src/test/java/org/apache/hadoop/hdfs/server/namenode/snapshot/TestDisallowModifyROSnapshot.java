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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * This class tests snapshot functionality. One or multiple snapshots are
 * created. The snapshotted directory is changed and verification is done to
 * ensure snapshots remain unchanges.
 */
public class TestDisallowModifyROSnapshot {
  private final static Path dir = new Path("/TestSnapshot");
  private final static Path sub1 = new Path(dir, "sub1");
  private final static Path sub2 = new Path(dir, "sub2");

  protected static Configuration conf;
  protected static MiniDFSCluster cluster;
  protected static FSNamesystem fsn;
  protected static DistributedFileSystem fs;

  /**
   * The list recording all previous snapshots. Each element in the array
   * records a snapshot root.
   */
  protected static ArrayList<Path> snapshotList = new ArrayList<Path>();
  static Path objInSnapshot = null;

  @BeforeAll
  public static void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    fsn = cluster.getNamesystem();
    fs = cluster.getFileSystem();

    Path path1 = new Path(sub1, "dir1");
    assertTrue(fs.mkdirs(path1));
    Path path2 = new Path(sub2, "dir2");
    assertTrue(fs.mkdirs(path2));
    SnapshotTestHelper.createSnapshot(fs, sub1, "testSnapshot");
    objInSnapshot = SnapshotTestHelper.getSnapshotPath(sub1, "testSnapshot",
        "dir1");
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  @Timeout(value = 60)
  public void testSetReplication() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.setReplication(objInSnapshot, (short) 1);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testSetPermission() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.setPermission(objInSnapshot, new FsPermission("777"));
    });
  }

  @Test
  @Timeout(value = 60)
  public void testSetOwner() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.setOwner(objInSnapshot, "username", "groupname");
    });
  }

  @Test
  @Timeout(value = 60)
  public void testRename() throws Exception {
    try {
      fs.rename(objInSnapshot, new Path("/invalid/path"));
      fail("Didn't throw SnapshotAccessControlException");
    } catch (SnapshotAccessControlException e) { /* Ignored */ }

    try {
      fs.rename(sub2, objInSnapshot);
      fail("Didn't throw SnapshotAccessControlException");
    } catch (SnapshotAccessControlException e) { /* Ignored */ }

    try {
      fs.rename(sub2, objInSnapshot, (Options.Rename) null);
      fail("Didn't throw SnapshotAccessControlException");
    } catch (SnapshotAccessControlException e) { /* Ignored */ }
  }

  @Test
  @Timeout(value = 60)
  public void testDelete() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.delete(objInSnapshot, true);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testQuota() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.setQuota(objInSnapshot, 100, 100);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testSetTime() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.setTimes(objInSnapshot, 100, 100);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testCreate() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      @SuppressWarnings("deprecation")
      DFSClient dfsclient = new DFSClient(conf);
      dfsclient.create(objInSnapshot.toString(), true);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testAppend() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.append(objInSnapshot, 65535, null);
    });
  }

  @Test
  @Timeout(value = 60)
  public void testMkdir() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      fs.mkdirs(objInSnapshot, new FsPermission("777"));
    });
  }

  @Test
  @Timeout(value = 60)
  public void testCreateSymlink() throws Exception {
    assertThrows(SnapshotAccessControlException.class, () -> {
      @SuppressWarnings("deprecation")
      DFSClient dfsclient = new DFSClient(conf);
      dfsclient.createSymlink(sub2.toString(), "/TestSnapshot/sub1/.snapshot",
          false);
    });
  }
}
