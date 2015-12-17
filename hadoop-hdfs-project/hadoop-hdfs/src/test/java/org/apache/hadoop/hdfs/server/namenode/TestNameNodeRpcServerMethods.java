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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNameNodeRpcServerMethods {
  private static NamenodeProtocols nnRpc;
  private static Configuration conf;
  private static MiniDFSCluster cluster;

  /** Start a cluster */
  @Before
  public void setup() throws Exception {
    conf = new HdfsConfiguration();
    cluster = new MiniDFSCluster.Builder(conf).build();
    cluster.waitActive();
    nnRpc = cluster.getNameNode().getRpcServer();
  }

  /**
   * Cleanup after the test
   *
   * @throws IOException
   * @throws UnresolvedLinkException
   * @throws SafeModeException
   * @throws AccessControlException
   */
  @After
  public void cleanup() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testDeleteSnapshotWhenSnapshotNameIsEmpty() throws Exception {
    String dir = "/testNamenodeRetryCache/testDelete";
    try {
      nnRpc.deleteSnapshot(dir, null);
      Assert.fail("testdeleteSnapshot is not thrown expected exception ");
    } catch (IOException e) {
      // expected
      GenericTestUtils.assertExceptionContains(
          "The snapshot name is null or empty.", e);
    }
    try {
      nnRpc.deleteSnapshot(dir, "");
      Assert.fail("testdeleteSnapshot is not thrown expected exception");
    } catch (IOException e) {
      // expected
      GenericTestUtils.assertExceptionContains(
          "The snapshot name is null or empty.", e);
    }

  }

}
