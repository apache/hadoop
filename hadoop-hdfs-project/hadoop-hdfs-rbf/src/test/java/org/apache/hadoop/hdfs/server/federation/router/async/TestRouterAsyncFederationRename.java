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
package org.apache.hadoop.hdfs.server.federation.router.async;

import static org.apache.hadoop.hdfs.server.federation.FederationTestUtils.verifyFileExists;
import static org.apache.hadoop.test.GenericTestUtils.getMethodName;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster;
import org.apache.hadoop.hdfs.server.federation.MiniRouterDFSCluster.RouterContext;
import org.apache.hadoop.hdfs.server.federation.router.TestRouterFederationRenameBase;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests router federation rename with async RPC enabled.
 */
public class TestRouterAsyncFederationRename
    extends TestRouterFederationRenameBase {

  private RouterContext router;
  private FileSystem routerFS;
  private MiniRouterDFSCluster cluster;

  @BeforeAll
  public static void before() throws Exception {
    globalSetUp(true);
  }

  @AfterAll
  public static void after() {
    tearDown();
  }

  @BeforeEach
  public void testSetup() throws Exception {
    setup();
    router = getRouterContext();
    routerFS = getRouterFileSystem();
    cluster = getCluster();
  }

  @Test
  @Timeout(value = 60)
  public void testAsyncRbfRename2() throws Exception {
    List<String> nss = cluster.getNameservices();
    String ns0 = nss.get(0);
    String ns1 = nss.get(1);

    String dir = cluster.getFederatedTestDirectoryForNS(ns0) + "/"
        + getMethodName();
    String renamedDir = cluster.getFederatedTestDirectoryForNS(ns1) + "/"
        + getMethodName();

    createDir(routerFS, dir);
    try {
      DFSClient client = router.getClient();
      ClientProtocol clientProtocol = client.getNamenode();
      clientProtocol.rename2(dir, renamedDir);

      assertFalse(verifyFileExists(routerFS, dir));
      assertTrue(verifyFileExists(routerFS, renamedDir + "/file"));
    } finally {
      FileContext fileContext = router.getFileContext();
      fileContext.delete(new Path(dir), true);
      fileContext.delete(new Path(renamedDir), true);
    }
  }
}
