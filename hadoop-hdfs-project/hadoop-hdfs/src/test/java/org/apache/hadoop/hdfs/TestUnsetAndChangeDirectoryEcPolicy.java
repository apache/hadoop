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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.NoECPolicySetException;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.Assert;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.apache.hadoop.test.GenericTestUtils.assertExceptionContains;
import static org.junit.Assert.fail;

/**
 * Test unset and change directory's erasure coding policy.
 */
public class TestUnsetAndChangeDirectoryEcPolicy {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestUnsetAndChangeDirectoryEcPolicy.class);

  private MiniDFSCluster cluster;
  private Configuration conf = new Configuration();
  private DistributedFileSystem fs;
  private ErasureCodingPolicy ecPolicy = StripedFileTestUtil.getDefaultECPolicy();
  private final short dataBlocks = (short) ecPolicy.getNumDataUnits();
  private final short parityBlocks = (short) ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int stripsPerBlock = 2;
  private final int blockSize = stripsPerBlock * cellSize;
  private final int blockGroupSize =  dataBlocks * blockSize;

  @Rule
  public Timeout globalTimeout = new Timeout(300000);

  @Before
  public void setup() throws IOException {
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 0);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(
        dataBlocks + parityBlocks).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    DFSTestUtil.enableAllECPolicies(fs);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /*
   * Test unset EC policy on directory.
   */
  @Test
  public void testUnsetEcPolicy() throws Exception {
    final int numBlocks = 1;
    final int fileLen = blockGroupSize * numBlocks;
    final Path dirPath = new Path("/striped");
    final Path ecFilePath = new Path(dirPath, "ec_file");
    final Path replicateFilePath = new Path(dirPath, "3x_file");

    fs.mkdirs(dirPath);
    // Test unset a directory which has no EC policy
    try {
      fs.unsetErasureCodingPolicy(dirPath);
      fail();
    } catch (NoECPolicySetException e) {
    }
    // Set EC policy on directory
    fs.setErasureCodingPolicy(dirPath, ecPolicy.getName());

    DFSTestUtil.createFile(fs, ecFilePath, fileLen, (short) 1, 0L);
    fs.unsetErasureCodingPolicy(dirPath);
    DFSTestUtil.createFile(fs, replicateFilePath, fileLen, (short) 1, 0L);

    // ec_file should has EC policy
    ErasureCodingPolicy tempEcPolicy =
        fs.getErasureCodingPolicy(ecFilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // rep_file should not have EC policy
    tempEcPolicy = fs.getErasureCodingPolicy(replicateFilePath);
    Assert.assertNull("Replicate file should not have erasure coding policy!",
        tempEcPolicy);

    // Directory should not return erasure coding policy
    tempEcPolicy = fs.getErasureCodingPolicy(dirPath);
    Assert.assertNull("Directory should no have erasure coding policy set!",
        tempEcPolicy);

    fs.delete(dirPath, true);
  }

  /*
   * Test nested directory with different EC policy.
   */
  @Test
  public void testNestedEcPolicy() throws Exception {
    final int numBlocks = 1;
    final int fileLen = blockGroupSize * numBlocks;
    final Path parentDir = new Path("/ec-6-3");
    final Path childDir = new Path("/ec-6-3/ec-3-2");
    final Path ec63FilePath = new Path(childDir, "ec_6_3_file");
    final Path ec32FilePath = new Path(childDir, "ec_3_2_file");
    final Path ec63FilePath2 = new Path(childDir, "ec_6_3_file_2");
    final ErasureCodingPolicy ec32Policy = SystemErasureCodingPolicies
        .getByID(SystemErasureCodingPolicies.RS_3_2_POLICY_ID);

    fs.mkdirs(parentDir);
    fs.setErasureCodingPolicy(parentDir, ecPolicy.getName());
    fs.mkdirs(childDir);
    // Create RS(6,3) EC policy file
    DFSTestUtil.createFile(fs, ec63FilePath, fileLen, (short) 1, 0L);
    // Set RS(3,2) EC policy on child directory
    fs.setErasureCodingPolicy(childDir, ec32Policy.getName());
    // Create RS(3,2) EC policy file
    DFSTestUtil.createFile(fs, ec32FilePath, fileLen, (short) 1, 0L);

    // Start to check
    // ec_6_3_file should has RS-6-3 EC policy
    ErasureCodingPolicy tempEcPolicy =
        fs.getErasureCodingPolicy(ec63FilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // ec_3_2_file should have RS-3-2 policy
    tempEcPolicy = fs.getErasureCodingPolicy(ec32FilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ec32Policy.getName()));

    // Child directory should have RS-3-2 policy
    tempEcPolicy = fs.getErasureCodingPolicy(childDir);
    Assert.assertTrue(
        "Directory should have erasure coding policy set!",
        tempEcPolicy.getName().equals(ec32Policy.getName()));

    // Unset EC policy on child directory
    fs.unsetErasureCodingPolicy(childDir);
    DFSTestUtil.createFile(fs, ec63FilePath2, fileLen, (short) 1, 0L);

    // ec_6_3_file_2 should have RS-6-3 policy
    tempEcPolicy = fs.getErasureCodingPolicy(ec63FilePath2);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // Child directory should have RS-6-3 policy now
    tempEcPolicy = fs.getErasureCodingPolicy(childDir);
    Assert.assertTrue(
        "Directory should have erasure coding policy set!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    fs.delete(parentDir, true);
  }


  /*
   * Test unset EC policy on root directory.
   */
  @Test
  public void testUnsetRootDirEcPolicy() throws Exception {
    final int numBlocks = 1;
    final int fileLen = blockGroupSize * numBlocks;
    final Path rootPath = new Path("/");
    final Path ecFilePath = new Path(rootPath, "ec_file");
    final Path replicateFilePath = new Path(rootPath, "rep_file");

    // Test unset root path which has no EC policy
    try {
      fs.unsetErasureCodingPolicy(rootPath);
      fail();
    } catch (NoECPolicySetException e) {
    }
    // Set EC policy on root path
    fs.setErasureCodingPolicy(rootPath, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFilePath, fileLen, (short) 1, 0L);
    fs.unsetErasureCodingPolicy(rootPath);
    DFSTestUtil.createFile(fs, replicateFilePath, fileLen, (short) 1, 0L);

    // ec_file should has EC policy set
    ErasureCodingPolicy tempEcPolicy =
        fs.getErasureCodingPolicy(ecFilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // rep_file should not have EC policy set
    tempEcPolicy = fs.getErasureCodingPolicy(replicateFilePath);
    Assert.assertNull("Replicate file should not have erasure coding policy!",
        tempEcPolicy);

    // Directory should not return erasure coding policy
    tempEcPolicy = fs.getErasureCodingPolicy(rootPath);
    Assert.assertNull("Directory should not have erasure coding policy set!",
        tempEcPolicy);

    fs.delete(rootPath, true);
  }

  /*
  * Test change EC policy on root directory.
  */
  @Test
  public void testChangeRootDirEcPolicy() throws Exception {
    final int numBlocks = 1;
    final int fileLen = blockGroupSize * numBlocks;
    final Path rootPath = new Path("/");
    final Path ec63FilePath = new Path(rootPath, "ec_6_3_file");
    final Path ec32FilePath = new Path(rootPath, "ec_3_2_file");
    final ErasureCodingPolicy ec32Policy = SystemErasureCodingPolicies
        .getByID(SystemErasureCodingPolicies.RS_3_2_POLICY_ID);

    try {
      fs.unsetErasureCodingPolicy(rootPath);
      fail();
    } catch (NoECPolicySetException e) {
    }
    fs.setErasureCodingPolicy(rootPath, ecPolicy.getName());
    // Create RS(6,3) EC policy file
    DFSTestUtil.createFile(fs, ec63FilePath, fileLen, (short) 1, 0L);
    // Change EC policy from RS(6,3) to RS(3,2)
    fs.setErasureCodingPolicy(rootPath, ec32Policy.getName());
    DFSTestUtil.createFile(fs, ec32FilePath, fileLen, (short) 1, 0L);

    // start to check
    // ec_6_3_file should has RS-6-3 ec policy set
    ErasureCodingPolicy tempEcPolicy =
        fs.getErasureCodingPolicy(ec63FilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // ec_3_2_file should have RS-3-2 policy
    tempEcPolicy = fs.getErasureCodingPolicy(ec32FilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ec32Policy.getName()));

    // Root directory should have RS-3-2 policy
    tempEcPolicy = fs.getErasureCodingPolicy(rootPath);
    Assert.assertTrue(
        "Directory should have erasure coding policy!",
        tempEcPolicy.getName().equals(ec32Policy.getName()));

    fs.delete(rootPath, true);
  }

  /*
   * Test different replica factor files.
   */
  @Test
  public void testDifferentReplicaFactor() throws Exception {
    final int numBlocks = 1;
    final int fileLen = blockGroupSize * numBlocks;
    final Path ecDirPath = new Path("/striped");
    final Path ecFilePath = new Path(ecDirPath, "ec_file");
    final Path replicateFilePath = new Path(ecDirPath, "rep_file");
    final Path replicateFilePath2 = new Path(ecDirPath, "rep_file2");

    fs.mkdirs(ecDirPath);
    fs.setErasureCodingPolicy(ecDirPath, ecPolicy.getName());
    DFSTestUtil.createFile(fs, ecFilePath, fileLen, (short) 1, 0L);
    fs.unsetErasureCodingPolicy(ecDirPath);
    DFSTestUtil.createFile(fs, replicateFilePath, fileLen, (short) 3, 0L);
    DFSTestUtil.createFile(fs, replicateFilePath2, fileLen, (short) 2, 0L);

    // ec_file should has EC policy set
    ErasureCodingPolicy tempEcPolicy =
        fs.getErasureCodingPolicy(ecFilePath);
    Assert.assertTrue("Erasure coding policy mismatch!",
        tempEcPolicy.getName().equals(ecPolicy.getName()));

    // rep_file should not have EC policy set
    tempEcPolicy = fs.getErasureCodingPolicy(replicateFilePath);
    Assert.assertNull("Replicate file should not have erasure coding policy!",
        tempEcPolicy);
    tempEcPolicy = fs.getErasureCodingPolicy(replicateFilePath2);
    Assert.assertNull("Replicate file should not have erasure coding policy!",
        tempEcPolicy);

    // Directory should not return erasure coding policy
    tempEcPolicy = fs.getErasureCodingPolicy(ecDirPath);
    Assert.assertNull("Directory should not have erasure coding policy set!",
        tempEcPolicy);

    fs.delete(ecDirPath, true);
  }


  /*
   * Test set and unset EC policy on directory doesn't exist.
   */
  @Test
  public void testNonExistentDir() throws Exception {
    final Path dirPath = new Path("/striped");

    // Unset EC policy on non-existent directory
    try {
      fs.unsetErasureCodingPolicy(dirPath);
      fail("FileNotFoundException should be thrown for a non-existent"
          + " file path");
    } catch (FileNotFoundException e) {
      assertExceptionContains("Path not found: " + dirPath, e);
    }

    // Set EC policy on non-existent directory
    try {
      fs.setErasureCodingPolicy(dirPath, ecPolicy.getName());
      fail("FileNotFoundException should be thrown for a non-existent"
          + " file path");
    } catch (FileNotFoundException e) {
      assertExceptionContains("Path not found: " + dirPath, e);
    }
  }

  /*
   * Test set and unset EC policy on file.
   */
  @Test
  public void testEcPolicyOnFile() throws Exception {
    final Path ecFilePath = new Path("/striped_file");
    final int fileLen = blockGroupSize * 2;
    DFSTestUtil.createFile(fs, ecFilePath, fileLen, (short) 1, 0L);

    // Set EC policy on file
    try {
      fs.setErasureCodingPolicy(ecFilePath, ecPolicy.getName());
      fail("IOException should be thrown for setting EC policy on file");
    } catch (IOException e) {
      assertExceptionContains("Attempt to set an erasure coding policy " +
          "for a file " + ecFilePath, e);
    }

    // Unset EC policy on file
    try {
      fs.unsetErasureCodingPolicy(ecFilePath);
      fail("IOException should be thrown for unsetting EC policy on file");
    } catch (IOException e) {
      assertExceptionContains("Cannot unset an erasure coding policy on a file "
          + ecFilePath, e);
    }
  }

  /**
   * Test unsetEcPolicy is persisted correctly in edit log.
   */
  @Test
  public void testUnsetEcPolicyInEditLog() throws IOException {
    fs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());
    Assert.assertEquals(ecPolicy, fs.getErasureCodingPolicy(new Path("/")));
    fs.getClient().unsetErasureCodingPolicy("/");

    cluster.restartNameNode(true);
    Assert.assertNull(fs.getErasureCodingPolicy(new Path("/")));
  }
}