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
package org.apache.hadoop.fs.viewfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.security.token.Token;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for viewfs with LinkFallback mount table entries.
 */
public class TestViewFsLinkFallback {
  private static FileSystem fsDefault;
  private FileSystem fsTarget;
  private static MiniDFSCluster cluster;
  private static URI viewFsDefaultClusterUri;
  private Path targetTestRoot;

  @BeforeClass
  public static void clusterSetupAtBeginning()
      throws IOException, URISyntaxException {
    int nameSpacesCount = 3;
    int dataNodesCount = 3;
    int fsIndexDefault = 0;
    Configuration conf = new Configuration();
    FileSystem[] fsHdfs = new FileSystem[nameSpacesCount];
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_DELEGATION_TOKEN_ALWAYS_USE_KEY,
        true);
    cluster = new MiniDFSCluster.Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleFederatedTopology(
            nameSpacesCount))
        .numDataNodes(dataNodesCount)
        .build();
    cluster.waitClusterUp();

    for (int i = 0; i < nameSpacesCount; i++) {
      fsHdfs[i] = cluster.getFileSystem(i);
    }
    fsDefault = fsHdfs[fsIndexDefault];
    viewFsDefaultClusterUri = new URI(FsConstants.VIEWFS_SCHEME,
        Constants.CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE, "/", null, null);

  }

  @AfterClass
  public static void clusterShutdownAtEnd() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    fsTarget = fsDefault;
    initializeTargetTestRoot();
  }

  private void initializeTargetTestRoot() throws IOException {
    targetTestRoot = fsDefault.makeQualified(new Path("/"));
    for (FileStatus status : fsDefault.listStatus(targetTestRoot)) {
      fsDefault.delete(status.getPath(), true);
    }
  }

  /**
   * Test getDelegationToken when fallback is configured.
   */
  @Test
  public void testGetDelegationToken() throws IOException {
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, "/user",
        new Path(targetTestRoot.toString(), "user").toUri());
    ConfigUtil.addLink(conf, "/data",
        new Path(targetTestRoot.toString(), "data").toUri());
    ConfigUtil.addLinkFallback(conf, targetTestRoot.toUri());

    FileContext fcView =
        FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
    List<Token<?>> tokens = fcView.getDelegationTokens(new Path("/"), "tester");
    // Two tokens from the two mount points and one token from fallback
    assertEquals(3, tokens.size());
  }
}
