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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.ErasureCodingInfo;
import org.apache.hadoop.hdfs.server.namenode.ErasureCodingSchemaManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.*;

public class TestErasureCodingZones {
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private static final int BLOCK_SIZE = 1024;
  private FSNamesystem namesystem;

  @Before
  public void setupCluster() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    cluster = new MiniDFSCluster.Builder(conf).
        numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    namesystem = cluster.getNamesystem();
  }

  @After
  public void shutdownCluster() throws IOException {
    cluster.shutdown();
  }

  @Test
  public void testCreateECZone()
      throws IOException, InterruptedException {
    final Path testDir = new Path("/ec");
    fs.mkdir(testDir, FsPermission.getDirDefault());

    /* Normal creation of an erasure coding zone */
    fs.getClient().createErasureCodingZone(testDir.toString(), null, 0);

    /* Verify files under the zone are striped */
    final Path ECFilePath = new Path(testDir, "foo");
    fs.create(ECFilePath);
    INode inode = namesystem.getFSDirectory().getINode(ECFilePath.toString());
    assertTrue(inode.asFile().isStriped());

    /* Verify that EC zone cannot be created on non-empty dir */
    final Path notEmpty = new Path("/nonEmpty");
    fs.mkdir(notEmpty, FsPermission.getDirDefault());
    fs.create(new Path(notEmpty, "foo"));
    try {
      fs.getClient().createErasureCodingZone(notEmpty.toString(), null, 0);
      fail("Erasure coding zone on non-empty dir");
    } catch (IOException e) {
      assertExceptionContains("erasure coding zone for a non-empty directory", e);
    }

    /* Verify that nested EC zones cannot be created */
    final Path zone1 = new Path("/zone1");
    final Path zone2 = new Path(zone1, "zone2");
    fs.mkdir(zone1, FsPermission.getDirDefault());
    fs.getClient().createErasureCodingZone(zone1.toString(), null, 0);
    fs.mkdir(zone2, FsPermission.getDirDefault());
    try {
      fs.getClient().createErasureCodingZone(zone2.toString(), null, 0);
      fail("Nested erasure coding zones");
    } catch (IOException e) {
      assertExceptionContains("already in an erasure coding zone", e);
    }

    /* Verify that EC zone cannot be created on a file */
    final Path fPath = new Path("/file");
    fs.create(fPath);
    try {
      fs.getClient().createErasureCodingZone(fPath.toString(), null, 0);
      fail("Erasure coding zone on file");
    } catch (IOException e) {
      assertExceptionContains("erasure coding zone for a file", e);
    }
  }

  @Test
  public void testMoveValidity() throws IOException, InterruptedException {
    final Path srcECDir = new Path("/srcEC");
    final Path dstECDir = new Path("/dstEC");
    fs.mkdir(srcECDir, FsPermission.getDirDefault());
    fs.mkdir(dstECDir, FsPermission.getDirDefault());
    fs.getClient().createErasureCodingZone(srcECDir.toString(), null, 0);
    fs.getClient().createErasureCodingZone(dstECDir.toString(), null, 0);
    final Path srcFile = new Path(srcECDir, "foo");
    fs.create(srcFile);

    /* Verify that a file can be moved between 2 EC zones */
    try {
      fs.rename(srcFile, dstECDir);
    } catch (IOException e) {
      fail("A file should be able to move between 2 EC zones " + e);
    }

    // Move the file back
    fs.rename(new Path(dstECDir, "foo"), srcECDir);

    /* Verify that a file cannot be moved from a non-EC dir to an EC zone */
    final Path nonECDir = new Path("/nonEC");
    fs.mkdir(nonECDir, FsPermission.getDirDefault());
    try {
      fs.rename(srcFile, nonECDir);
      fail("A file shouldn't be able to move from a non-EC dir to an EC zone");
    } catch (IOException e) {
      assertExceptionContains("can't be moved because the source and " +
          "destination have different erasure coding policies", e);
    }

    /* Verify that a file cannot be moved from an EC zone to a non-EC dir */
    final Path nonECFile = new Path(nonECDir, "nonECFile");
    fs.create(nonECFile);
    try {
      fs.rename(nonECFile, dstECDir);
    } catch (IOException e) {
      assertExceptionContains("can't be moved because the source and " +
          "destination have different erasure coding policies", e);
    }
  }

  @Test
  public void testGetErasureCodingInfoWithSystemDefaultSchema() throws Exception {
    String src = "/ec";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir ECInfo before creating ec zone
    assertNull(fs.getClient().getErasureCodingInfo(src));
    // dir ECInfo after creating ec zone
    fs.getClient().createErasureCodingZone(src, null, 0); //Default one will be used.
    ECSchema sysDefaultSchema = ErasureCodingSchemaManager.getSystemDefaultSchema();
    verifyErasureCodingInfo(src, sysDefaultSchema);
    fs.create(new Path(ecDir, "/child1")).close();
    // verify for the files in ec zone
    verifyErasureCodingInfo(src + "/child1", sysDefaultSchema);
  }

  @Test
  public void testGetErasureCodingInfo() throws Exception {
    ECSchema[] sysSchemas = ErasureCodingSchemaManager.getSystemSchemas();
    assertTrue("System schemas should be of only 1 for now",
        sysSchemas.length == 1);

    ECSchema usingSchema = sysSchemas[0];
    String src = "/ec2";
    final Path ecDir = new Path(src);
    fs.mkdir(ecDir, FsPermission.getDirDefault());
    // dir ECInfo before creating ec zone
    assertNull(fs.getClient().getErasureCodingInfo(src));
    // dir ECInfo after creating ec zone
    fs.getClient().createErasureCodingZone(src, usingSchema, 0);
    verifyErasureCodingInfo(src, usingSchema);
    fs.create(new Path(ecDir, "/child1")).close();
    // verify for the files in ec zone
    verifyErasureCodingInfo(src + "/child1", usingSchema);
  }

  private void verifyErasureCodingInfo(
      String src, ECSchema usingSchema) throws IOException {
    ErasureCodingInfo ecInfo = fs.getClient().getErasureCodingInfo(src);
    assertNotNull("ECInfo should have been non-null", ecInfo);
    assertEquals(src, ecInfo.getSrc());
    ECSchema schema = ecInfo.getSchema();
    assertNotNull(schema);
    assertEquals("Actually used schema should be equal with target schema",
        usingSchema, schema);
  }
}
