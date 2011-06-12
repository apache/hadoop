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

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests if a data-node can startup depending on configuration parameters.
 */
public class TestDatanodeConfig {
  private static final File BASE_DIR =
                                new File(MiniDFSCluster.getBaseDirectory());

  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    clearBaseDir();
    Configuration conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster(conf, 0, true, null);
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(cluster != null)
      cluster.shutdown();
    clearBaseDir();
  }

  private static void clearBaseDir() throws IOException {
    if(BASE_DIR.exists() && ! FileUtil.fullyDelete(BASE_DIR))
      throw new IOException("Cannot clear BASE_DIR " + BASE_DIR);
  }

  /**
   * Test that a data-node does not start if configuration specifies
   * incorrect URI scheme in data directory.
   * Test that a data-node starts if data directory is specified as
   * URI = "file:///path" or as a non URI path.
   */
  @Test
  public void testDataDirectories() throws IOException {
    File dataDir = new File(BASE_DIR, "data").getCanonicalFile();
    Configuration conf = cluster.getConfiguration();
    // 1. Test unsupported schema. Only "file:" is supported.
    String dnDir = makeURI("shv", null, fileAsURI(dataDir).getPath());
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, dnDir);
    DataNode dn = DataNode.createDataNode(new String[]{}, conf);
    assertNull("Data-node startup should have failed.", dn);

    // 2. Test "file:" schema and no schema (path-only). Both should work.
    String dnDir1 = fileAsURI(dataDir).toString() + "1";
    String dnDir2 = makeURI("file", "localhost",
                    fileAsURI(dataDir).getPath() + "2");
    String dnDir3 = dataDir.getAbsolutePath() + "3";
    conf.set(DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY,
                dnDir1 + "," + dnDir2 + "," + dnDir3);
    cluster.startDataNodes(conf, 1, false, StartupOption.REGULAR, null);
    assertTrue("Data-node should startup.", cluster.isDataNodeUp());
  }

  private static String makeURI(String scheme, String host, String path)
  throws IOException {
    try {
      URI uDir = new URI(scheme, host, path, null);
      return uDir.toString();
    } catch(URISyntaxException e) {
      throw new IOException("Bad URI", e);
    }
  }
}
