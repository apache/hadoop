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
package org.apache.hadoop.hdfs.tools;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.viewfs.ConfigUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Test StoragePolicyAdmin commands with ViewFileSystem.
 */
public class TestViewFSStoragePolicyCommands extends TestStoragePolicyCommands {

  @Before
  public void clusterSetUp() throws IOException {
    conf = new HdfsConfiguration();
    String clusterName = "cluster";
    cluster =
        new MiniDFSCluster.Builder(conf).nnTopology(
            MiniDFSNNTopology.simpleFederatedTopology(2))
            .numDataNodes(2)
            .build();
    cluster.waitActive();
    DistributedFileSystem hdfs1 = cluster.getFileSystem(0);
    DistributedFileSystem hdfs2 = cluster.getFileSystem(1);

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        FsConstants.VIEWFS_SCHEME +"://" + clusterName);

    Path base1 = new Path("/user1");
    Path base2 = new Path("/user2");
    hdfs1.delete(base1, true);
    hdfs2.delete(base2, true);
    hdfs1.mkdirs(base1);
    hdfs2.mkdirs(base2);
    ConfigUtil.addLink(conf, clusterName, "/foo",
        hdfs1.makeQualified(base1).toUri());
    ConfigUtil.addLink(conf, clusterName, "/hdfs2",
        hdfs2.makeQualified(base2).toUri());
    fs = FileSystem.get(conf);
  }

  /**
   * Storage policy operation on the viewfs root should fail.
   */
  @Test
  public void testStoragePolicyRoot() throws Exception {
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(conf);
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path /", 2,
        "is not supported for filesystem viewfs on path /");
  }

  @Test
  public void testStoragePolicyCommandPathWithSchema() throws Exception {
    Path base1 = new Path("/user1");
    final Path bar = new Path(base1, "bar");
    DFSTestUtil.createFile(cluster.getFileSystem(0), bar, 1024, (short) 1, 0);

    // Test with hdfs:// schema
    String pathHdfsSchema = "hdfs://"
        + cluster.getNameNode(0).getClientNamenodeAddress() + "/"
        + bar.toString();
    checkCommandsWithUriPath(pathHdfsSchema);

    // Test with webhdfs:// schema
    InetSocketAddress httpAddress = cluster.getNameNode(0).getHttpAddress();
    String pathWebhdfsSchema = "webhdfs://" + httpAddress.getHostName() + ":"
        + httpAddress.getPort() + "/" + bar.toString();
    checkCommandsWithUriPath(pathWebhdfsSchema);
  }

  private void checkCommandsWithUriPath(String pathWithSchema) throws Exception{
    final StoragePolicyAdmin admin = new StoragePolicyAdmin(conf);
    DFSTestUtil.toolRun(admin, "-setStoragePolicy -path " + pathWithSchema
        + " -policy WARM", 0, "Set storage policy WARM on " + pathWithSchema);
    final BlockStoragePolicySuite suite = BlockStoragePolicySuite
        .createDefaultSuite();
    final BlockStoragePolicy warm = suite.getPolicy("WARM");
    DFSTestUtil.toolRun(admin, "-getStoragePolicy -path " + pathWithSchema, 0,
        "The storage policy of " + pathWithSchema + ":\n" + warm);
    DFSTestUtil.toolRun(admin, "-unsetStoragePolicy -path " + pathWithSchema, 0,
        "Unset storage policy from " + pathWithSchema);
  }
}
