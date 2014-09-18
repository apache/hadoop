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

package org.apache.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class includes end-to-end tests for snapshot related FsShell and
 * DFSAdmin commands.
 */
public class TestSnapshotCommands {

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;
  
  @BeforeClass
  public static void clusterSetUp() throws IOException {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void clusterShutdown() throws IOException{
    if(fs != null){
      fs.close();
    }
    if(cluster != null){
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws IOException {
    fs.mkdirs(new Path("/sub1"));
    fs.allowSnapshot(new Path("/sub1"));
    fs.mkdirs(new Path("/sub1/sub1sub1"));
    fs.mkdirs(new Path("/sub1/sub1sub2"));
  }

  @After
  public void tearDown() throws IOException {
    if (fs.exists(new Path("/sub1"))) {
      if (fs.exists(new Path("/sub1/.snapshot"))) {
        for (FileStatus st : fs.listStatus(new Path("/sub1/.snapshot"))) {
          fs.deleteSnapshot(new Path("/sub1"), st.getPath().getName());
        }
        fs.disallowSnapshot(new Path("/sub1"));
      }
      fs.delete(new Path("/sub1"), true);
    }
  }

  @Test
  public void testAllowSnapshot() throws Exception {
    // Idempotent test
    DFSTestUtil.DFSAdminRun("-allowSnapshot /sub1", 0, "Allowing snaphot on /sub1 succeeded", conf);
    // allow normal dir success 
    DFSTestUtil.FsShellRun("-mkdir /sub2", conf);
    DFSTestUtil.DFSAdminRun("-allowSnapshot /sub2", 0, "Allowing snaphot on /sub2 succeeded", conf);
    // allow non-exists dir failed
    DFSTestUtil.DFSAdminRun("-allowSnapshot /sub3", -1, null, conf);
  }

  @Test
  public void testCreateSnapshot() throws Exception {
    // test createSnapshot
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn0", 0, "Created snapshot /sub1/.snapshot/sn0", conf);
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn0", 1, "there is already a snapshot with the same name \"sn0\"", conf);
    DFSTestUtil.FsShellRun("-rmr /sub1/sub1sub2", conf);
    DFSTestUtil.FsShellRun("-mkdir /sub1/sub1sub3", conf);
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", 0, "Created snapshot /sub1/.snapshot/sn1", conf);
    // check snapshot contents
    DFSTestUtil.FsShellRun("-ls /sub1", 0, "/sub1/sub1sub1", conf);
    DFSTestUtil.FsShellRun("-ls /sub1", 0, "/sub1/sub1sub3", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn0", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn1", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub1", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub2", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub1", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub3", conf);
  }

  @Test
  public void testMkdirUsingReservedName() throws Exception {
    // test can not create dir with reserved name: .snapshot
    DFSTestUtil.FsShellRun("-ls /", conf);
    DFSTestUtil.FsShellRun("-mkdir /.snapshot", 1, "File exists", conf);
    DFSTestUtil.FsShellRun("-mkdir /sub1/.snapshot", 1, "File exists", conf);
    // mkdir -p ignore reserved name check if dir already exists
    DFSTestUtil.FsShellRun("-mkdir -p /sub1/.snapshot", conf);
    DFSTestUtil.FsShellRun("-mkdir -p /sub1/sub1sub1/.snapshot", 1, "mkdir: \".snapshot\" is a reserved name.", conf);
  }

  @Test
  public void testRenameSnapshot() throws Exception {
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn.orig", conf);
    DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.orig sn.rename", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn.rename", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub1", conf);
    DFSTestUtil.FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub2", conf);

    //try renaming from a non-existing snapshot
    DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.nonexist sn.rename", 1,
        "renameSnapshot: The snapshot sn.nonexist does not exist for directory /sub1", conf);

    //try renaming to existing snapshots
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn.new", conf);
    DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.new sn.rename", 1,
        "renameSnapshot: The snapshot sn.rename already exists for directory /sub1", conf);
    DFSTestUtil.FsShellRun("-renameSnapshot /sub1 sn.rename sn.new", 1,
        "renameSnapshot: The snapshot sn.new already exists for directory /sub1", conf);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", conf);
    DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", conf);
    DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", 1,
        "deleteSnapshot: Cannot delete snapshot sn1 from path /sub1: the snapshot does not exist.", conf);
  }

  @Test
  public void testDisallowSnapshot() throws Exception {
    DFSTestUtil.FsShellRun("-createSnapshot /sub1 sn1", conf);
    // cannot delete snapshotable dir
    DFSTestUtil.FsShellRun("-rmr /sub1", 1, "The directory /sub1 cannot be deleted since /sub1 is snapshottable and already has snapshots", conf);
    DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", -1,
        "disallowSnapshot: The directory /sub1 has snapshot(s). Please redo the operation after removing all the snapshots.", conf);
    DFSTestUtil.FsShellRun("-deleteSnapshot /sub1 sn1", conf);
    DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded", conf);
    // Idempotent test
    DFSTestUtil.DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded", conf);
    // now it can be deleted
    DFSTestUtil.FsShellRun("-rmr /sub1", conf);
  }
}
