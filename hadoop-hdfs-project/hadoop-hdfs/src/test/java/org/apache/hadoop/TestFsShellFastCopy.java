/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.FastCopy;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestFsShellFastCopy {
  private Configuration conf = null;
  private MiniDFSCluster cluster = null;
  private DistributedFileSystem srcDFS = null;
  private DistributedFileSystem dstDFS = null;
  private FsShell fsShell;

  private static final long BLOCKSIZE = 1024 * 1024;

  private final String prefix = "/fastcopy/";
  private final String replicatedRootDir = prefix + "replicated/";
  private final String ecFileRootDir = prefix + "ec/";
  private final String suffix = "BlocksFile";

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);

    cluster =
        new MiniDFSCluster.Builder(conf).nnTopology(MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(6).build();
    cluster.waitActive();

    srcDFS = cluster.getFileSystem(0);
    dstDFS = cluster.getFileSystem(1);

    srcDFS.mkdirs(new Path(replicatedRootDir), FsPermission.getFileDefault());
    dstDFS.mkdirs(new Path(replicatedRootDir), FsPermission.getFileDefault());

    enableECPolicies();
    fsShell = new FsShell(conf);
  }

  @After
  public void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void initFiles(String dir, int numFiles) throws IOException {
    for (int i = 0; i < numFiles; i++) {
      Path path = new Path(dir + i + suffix);
      DFSTestUtil.createFile(srcDFS, path, i * BLOCKSIZE, (short) 3, 0L);
    }
  }

  private void cleanFiles(DistributedFileSystem dfs, String dir) throws IOException {
    dfs.delete(new Path(dir), true);
  }

  public void enableECPolicies() throws IOException {
    DFSTestUtil.enableAllECPolicies(srcDFS);
    ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getPolicies().get(1);
    srcDFS.mkdirs(new Path(ecFileRootDir), FsPermission.getFileDefault());
    srcDFS.getClient().setErasureCodingPolicy(ecFileRootDir, ecPolicy.getName());

    DFSTestUtil.enableAllECPolicies(dstDFS);
    ecPolicy = SystemErasureCodingPolicies.getPolicies().get(1);

    dstDFS.mkdirs(new Path(ecFileRootDir), FsPermission.getFileDefault());
    dstDFS.getClient().setErasureCodingPolicy(ecFileRootDir, ecPolicy.getName());
  }

  @Test
  public void testFastCopy() throws Exception {
    int numFiles = 4;
    initFiles(replicatedRootDir, numFiles);

    for (int i = 0; i < numFiles; i++) {
      FastCopy fcp = new FastCopy(conf, new Path(srcDFS.getUri() + replicatedRootDir + i + suffix),
          new Path(dstDFS.getUri() + replicatedRootDir + i + suffix), false);
      fcp.copyFile();
    }

    for (int i = 0; i < numFiles; i++) {
      Path path = new Path(replicatedRootDir + i + suffix);
      assertTrue(dstDFS.getFileStatus(path).isFile());
      assertEquals(dstDFS.getFileChecksum(path), srcDFS.getFileChecksum(path));
    }

    cleanFiles(srcDFS, replicatedRootDir);
    cleanFiles(dstDFS, replicatedRootDir);
  }

  @Test
  public void testFastCopyWithEC() throws Exception {
    int numFiles = 4;
    initFiles(ecFileRootDir, numFiles);

    for (int i = 0; i < numFiles; i++) {
      FastCopy fcp = new FastCopy(conf, new Path(srcDFS.getUri() + ecFileRootDir + i + suffix),
          new Path(dstDFS.getUri() + ecFileRootDir + i + suffix), false);
      fcp.copyFile();
    }

    for (int i = 0; i < numFiles; i++) {
      Path path = new Path(ecFileRootDir + i + suffix);
      assertTrue(dstDFS.getFileStatus(path).isFile());
      assertEquals(dstDFS.getFileChecksum(path), srcDFS.getFileChecksum(path));
    }

    cleanFiles(srcDFS, ecFileRootDir);
    cleanFiles(dstDFS, ecFileRootDir);
  }

  @Test
  public void testDFSFastCp() throws Exception {
    int numFiles = 4;
    initFiles(replicatedRootDir, numFiles);

    int ret = fsShell.run(new String[] {"-fastcp", "-t", "3", srcDFS.getUri() + replicatedRootDir,
        dstDFS.getUri() + prefix});
    assertEquals(0, ret);

    for (int i = 0; i < numFiles; i++) {
      Path path = new Path(replicatedRootDir + i + suffix);
      assertTrue(dstDFS.getFileStatus(path).isFile());
      assertEquals(dstDFS.getFileChecksum(path), srcDFS.getFileChecksum(path));
    }
  }

  @Test
  public void testDFSFastCpWithECFiles() throws Exception {
    int numFiles = 12;
    initFiles(ecFileRootDir, numFiles);

    int ret = fsShell.run(new String[] {"-fastcp", "-t", "3", srcDFS.getUri() + ecFileRootDir,
        dstDFS.getUri() + prefix});
    assertEquals(0, ret);

    for (int i = 0; i < numFiles; i++) {
      Path path = new Path(ecFileRootDir + i + suffix);
      assertTrue(dstDFS.getFileStatus(path).isFile());
      assertEquals(dstDFS.getFileChecksum(path), srcDFS.getFileChecksum(path));
    }
  }

}
