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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestErasureCodingPolicyWithSnapshot {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;

  private final static int SUCCESS = 0;
  private ErasureCodingPolicy ecPolicy;
  private short groupSize;

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Before
  public void setupCluster() throws IOException {
    ecPolicy = getEcPolicy();
    groupSize = (short) (ecPolicy.getNumDataUnits()
        + ecPolicy.getNumParityUnits());
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(groupSize).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(ecPolicy.getName());
  }

  @After
  public void shutdownCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Test correctness of successive snapshot creation and deletion with erasure
   * coding policies. Create snapshot of ecDir's parent directory.
   */
  @Test(timeout = 120000)
  public void testSnapshotsOnErasureCodingDirsParentDir() throws Exception {
    final int len = 1024;
    final Path ecDirParent = new Path("/parent");
    final Path ecDir = new Path(ecDirParent, "ecdir");
    final Path ecFile = new Path(ecDir, "ecfile");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDirParent);
    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);
    String contents = DFSTestUtil.readFile(fs, ecFile);
    final Path snap1 = fs.createSnapshot(ecDirParent, "snap1");
    final Path snap1ECDir = new Path(snap1, ecDir.getName());
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap1ECDir));

    // Now delete the dir which has erasure coding policy. Re-create the dir again, and
    // take another snapshot
    fs.delete(ecDir, true);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    final Path snap2 = fs.createSnapshot(ecDirParent, "snap2");
    final Path snap2ECDir = new Path(snap2, ecDir.getName());
    assertNull("Expected null erasure coding policy",
        fs.getErasureCodingPolicy(snap2ECDir));

    // Make dir again with system default ec policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap3 = fs.createSnapshot(ecDirParent, "snap3");
    final Path snap3ECDir = new Path(snap3, ecDir.getName());
    // Check that snap3's ECPolicy has the correct settings
    ErasureCodingPolicy ezSnap3 = fs.getErasureCodingPolicy(snap3ECDir);
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        ezSnap3);

    // Check that older snapshots still have the old ECPolicy settings
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap1ECDir));
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap2ECDir));

    // Verify contents of the snapshotted file
    final Path snapshottedECFile = new Path(snap1.toString() + "/"
        + ecDir.getName() + "/" + ecFile.getName());
    assertEquals("Contents of snapshotted file have changed unexpectedly",
        contents, DFSTestUtil.readFile(fs, snapshottedECFile));

    // Now delete the snapshots out of order and verify the EC policy
    // correctness
    fs.deleteSnapshot(ecDirParent, snap2.getName());
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap1ECDir));
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap3ECDir));
    fs.deleteSnapshot(ecDirParent, snap1.getName());
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap3ECDir));
  }

  /**
   * Test creation of snapshot on directory has erasure coding policy.
   */
  @Test(timeout = 120000)
  public void testSnapshotsOnErasureCodingDir() throws Exception {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap1));
  }

  /**
   * Test verify erasure coding policy is present after restarting the NameNode.
   */
  @Test(timeout = 120000)
  public void testSnapshotsOnErasureCodingDirAfterNNRestart() throws Exception {
    final Path ecDir = new Path("/ecdir");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");
    ErasureCodingPolicy ecSnap = fs.getErasureCodingPolicy(snap1);
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        ecSnap);

    // save namespace, restart namenode, and check ec policy correctness.
    fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
    fs.saveNamespace();
    fs.setSafeMode(SafeModeAction.SAFEMODE_LEAVE);
    cluster.restartNameNode(true);

    ErasureCodingPolicy ecSnap1 = fs.getErasureCodingPolicy(snap1);
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        ecSnap1);
    assertEquals("Got unexpected ecSchema", ecSnap.getSchema(),
        ecSnap1.getSchema());
  }

  /**
   * Test copy a snapshot will not preserve its erasure coding policy info.
   */
  @Test(timeout = 120000)
  public void testCopySnapshotWillNotPreserveErasureCodingPolicy()
      throws Exception {
    final int len = 1024;
    final Path ecDir = new Path("/ecdir");
    final Path ecFile = new Path(ecDir, "ecFile");
    fs.mkdirs(ecDir);
    fs.allowSnapshot(ecDir);

    // set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);
    final Path snap1 = fs.createSnapshot(ecDir, "snap1");

    Path snap1Copy = new Path(ecDir.toString() + "-copy");
    final Path snap1CopyECDir = new Path("/ecdir-copy");
    String[] argv = new String[] { "-cp", "-px", snap1.toUri().toString(),
        snap1Copy.toUri().toString() };
    int ret = ToolRunner.run(new FsShell(conf), argv);
    assertEquals("cp -px is not working on a snapshot", SUCCESS, ret);

    assertNull("Got unexpected erasure coding policy",
        fs.getErasureCodingPolicy(snap1CopyECDir));
    assertEquals("Got unexpected erasure coding policy", ecPolicy,
        fs.getErasureCodingPolicy(snap1));
  }

  @Test (timeout = 300000)
  public void testFileStatusAcrossNNRestart() throws IOException {
    final int len = 1024;
    final Path normalFile = new Path("/", "normalFile");
    DFSTestUtil.createFile(fs, normalFile, len, (short) 1, 0xFEED);

    final Path ecDir = new Path("/ecdir");
    final Path ecFile = new Path(ecDir, "ecFile");
    fs.mkdirs(ecDir);

    // Set erasure coding policy
    fs.setErasureCodingPolicy(ecDir, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFile, len, (short) 1, 0xFEED);

    // Verify FileStatus for normal and EC files
    ContractTestUtils.assertNotErasureCoded(fs, normalFile);
    ContractTestUtils.assertErasureCoded(fs, ecFile);

    cluster.restartNameNode(true);

    // Verify FileStatus for normal and EC files
    ContractTestUtils.assertNotErasureCoded(fs, normalFile);
    ContractTestUtils.assertErasureCoded(fs, ecFile);
  }
}
