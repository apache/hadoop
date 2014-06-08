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

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

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

  private void toolRun(Tool tool, String cmd, int retcode, String contain)
      throws Exception {
    String [] cmds = StringUtils.split(cmd, ' ');
    System.out.flush();
    System.err.flush();
    PrintStream origOut = System.out;
    PrintStream origErr = System.err;
    String output = null;
    int ret = 0;
    try {
      ByteArrayOutputStream bs = new ByteArrayOutputStream(1024);
      PrintStream out = new PrintStream(bs);
      System.setOut(out);
      System.setErr(out);
      ret = tool.run(cmds);
      System.out.flush();
      System.err.flush();
      out.close();
      output = bs.toString();
    } finally {
      System.setOut(origOut);
      System.setErr(origErr);
    }
    System.out.println("Output for command: " + cmd + " retcode: " + ret);
    if (output != null) {
      System.out.println(output);
    }
    assertEquals(retcode, ret);
    if (contain != null) {
      assertTrue(output.contains(contain));
    }
  }

  private void FsShellRun(String cmd, int retcode, String contain)
      throws Exception {
    FsShell shell = new FsShell(new Configuration(conf));
    toolRun(shell, cmd, retcode, contain);
  }  

  private void DFSAdminRun(String cmd, int retcode, String contain)
      throws Exception {
    DFSAdmin admin = new DFSAdmin(new Configuration(conf));
    toolRun(admin, cmd, retcode, contain);
  }

  private void FsShellRun(String cmd) throws Exception {
    FsShellRun(cmd, 0, null);
  }

  @Test
  public void testAllowSnapshot() throws Exception {
    // Idempotent test
    DFSAdminRun("-allowSnapshot /sub1", 0, "Allowing snaphot on /sub1 succeeded");
    // allow normal dir success 
    FsShellRun("-mkdir /sub2");
    DFSAdminRun("-allowSnapshot /sub2", 0, "Allowing snaphot on /sub2 succeeded");
    // allow non-exists dir failed
    DFSAdminRun("-allowSnapshot /sub3", -1, null);
  }

  @Test
  public void testCreateSnapshot() throws Exception {
    // test createSnapshot
    FsShellRun("-createSnapshot /sub1 sn0", 0, "Created snapshot /sub1/.snapshot/sn0");
    FsShellRun("-createSnapshot /sub1 sn0", 1, "there is already a snapshot with the same name \"sn0\"");
    FsShellRun("-rmr /sub1/sub1sub2");
    FsShellRun("-mkdir /sub1/sub1sub3");
    FsShellRun("-createSnapshot /sub1 sn1", 0, "Created snapshot /sub1/.snapshot/sn1");
    // check snapshot contents
    FsShellRun("-ls /sub1", 0, "/sub1/sub1sub1");
    FsShellRun("-ls /sub1", 0, "/sub1/sub1sub3");
    FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn0");
    FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn1");
    FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub1");
    FsShellRun("-ls /sub1/.snapshot/sn0", 0, "/sub1/.snapshot/sn0/sub1sub2");
    FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub1");
    FsShellRun("-ls /sub1/.snapshot/sn1", 0, "/sub1/.snapshot/sn1/sub1sub3");
  }

  @Test
  public void testMkdirUsingReservedName() throws Exception {
    // test can not create dir with reserved name: .snapshot
    FsShellRun("-ls /");
    FsShellRun("-mkdir /.snapshot", 1, "File exists");
    FsShellRun("-mkdir /sub1/.snapshot", 1, "File exists");
    // mkdir -p ignore reserved name check if dir already exists
    FsShellRun("-mkdir -p /sub1/.snapshot");
    FsShellRun("-mkdir -p /sub1/sub1sub1/.snapshot", 1, "mkdir: \".snapshot\" is a reserved name.");
  }

  @Test
  public void testRenameSnapshot() throws Exception {
    FsShellRun("-createSnapshot /sub1 sn.orig");
    FsShellRun("-renameSnapshot /sub1 sn.orig sn.rename");
    FsShellRun("-ls /sub1/.snapshot", 0, "/sub1/.snapshot/sn.rename");
    FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub1");
    FsShellRun("-ls /sub1/.snapshot/sn.rename", 0, "/sub1/.snapshot/sn.rename/sub1sub2");

    //try renaming from a non-existing snapshot
    FsShellRun("-renameSnapshot /sub1 sn.nonexist sn.rename", 1,
        "renameSnapshot: The snapshot sn.nonexist does not exist for directory /sub1");

    //try renaming to existing snapshots
    FsShellRun("-createSnapshot /sub1 sn.new");
    FsShellRun("-renameSnapshot /sub1 sn.new sn.rename", 1,
        "renameSnapshot: The snapshot sn.rename already exists for directory /sub1");
    FsShellRun("-renameSnapshot /sub1 sn.rename sn.new", 1,
        "renameSnapshot: The snapshot sn.new already exists for directory /sub1");
  }

  @Test
  public void testDeleteSnapshot() throws Exception {
    FsShellRun("-createSnapshot /sub1 sn1");
    FsShellRun("-deleteSnapshot /sub1 sn1");
    FsShellRun("-deleteSnapshot /sub1 sn1", 1,
        "deleteSnapshot: Cannot delete snapshot sn1 from path /sub1: the snapshot does not exist.");
  }

  @Test
  public void testDisallowSnapshot() throws Exception {
    FsShellRun("-createSnapshot /sub1 sn1");
    // cannot delete snapshotable dir
    FsShellRun("-rmr /sub1", 1, "The directory /sub1 cannot be deleted since /sub1 is snapshottable and already has snapshots");
    DFSAdminRun("-disallowSnapshot /sub1", -1,
        "disallowSnapshot: The directory /sub1 has snapshot(s). Please redo the operation after removing all the snapshots.");
    FsShellRun("-deleteSnapshot /sub1 sn1");
    DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded");
    // Idempotent test
    DFSAdminRun("-disallowSnapshot /sub1", 0, "Disallowing snaphot on /sub1 succeeded");
    // now it can be deleted
    FsShellRun("-rmr /sub1");
  }
}
