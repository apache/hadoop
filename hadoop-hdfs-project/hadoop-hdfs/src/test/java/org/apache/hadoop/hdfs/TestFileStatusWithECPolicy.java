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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * This test ensures the statuses of EC files with the default policy.
 */
public class TestFileStatusWithDefaultECPolicy {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private DFSClient client;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void before() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    client = fs.getClient();
    fs.enableErasureCodingPolicy(getEcPolicy().getName());
  }

  @After
  public void after() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  public ErasureCodingPolicy getEcPolicy() {
    return StripedFileTestUtil.getDefaultECPolicy();
  }

  @Test
  public void testFileStatusWithECPolicy() throws Exception {
    // test directory doesn't have an EC policy
    final Path dir = new Path("/foo");
    assertTrue(fs.mkdir(dir, FsPermission.getDirDefault()));
    ContractTestUtils.assertNotErasureCoded(fs, dir);
    assertNull(client.getFileInfo(dir.toString()).getErasureCodingPolicy());
    // test file doesn't have an EC policy
    final Path file = new Path(dir, "foo");
    fs.create(file).close();
    assertNull(client.getFileInfo(file.toString()).getErasureCodingPolicy());
    ContractTestUtils.assertNotErasureCoded(fs, file);
    fs.delete(file, true);

    final ErasureCodingPolicy ecPolicy1 = getEcPolicy();
    // set EC policy on dir
    fs.setErasureCodingPolicy(dir, ecPolicy1.getName());
    ContractTestUtils.assertErasureCoded(fs, dir);
    final ErasureCodingPolicy ecPolicy2 =
        client.getFileInfo(dir.toUri().getPath()).getErasureCodingPolicy();
    assertNotNull(ecPolicy2);
    assertTrue(ecPolicy1.equals(ecPolicy2));

    // test file with EC policy
    fs.create(file).close();
    final ErasureCodingPolicy ecPolicy3 =
        fs.getClient().getFileInfo(file.toUri().getPath())
            .getErasureCodingPolicy();
    assertNotNull(ecPolicy3);
    assertTrue(ecPolicy1.equals(ecPolicy3));
    ContractTestUtils.assertErasureCoded(fs, file);
    FileStatus status = fs.getFileStatus(file);
    assertTrue(file + " should have erasure coding set in " +
            "FileStatus#toString(): " + status,
        status.toString().contains("isErasureCoded=true"));
  }
}
