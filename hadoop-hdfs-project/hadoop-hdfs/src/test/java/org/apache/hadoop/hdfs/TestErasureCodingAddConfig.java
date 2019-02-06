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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.junit.Test;

/**
 * Test that ensures addition of user defined EC policies is allowed only when
 * dfs.namenode.ec.userdefined.policy.allowed is set to true.
 */
public class TestErasureCodingAddConfig {

  @Test
  public void testECAddPolicyConfigDisable() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY,
        false);
    try (MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(0).build()) {
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();

      ErasureCodingPolicy newPolicy1 =
          new ErasureCodingPolicy(new ECSchema("rs", 5, 3), 1024 * 1024);

      AddErasureCodingPolicyResponse[] response =
          fs.addErasureCodingPolicies(new ErasureCodingPolicy[] {newPolicy1});

      assertFalse(response[0].isSucceed());
      assertEquals(
          "Addition of user defined erasure coding policy is disabled.",
          response[0].getErrorMsg());
    }
  }

  @Test
  public void testECAddPolicyConfigEnable() throws IOException {
    Configuration conf = new HdfsConfiguration();
    conf.setBoolean(
        DFSConfigKeys.DFS_NAMENODE_EC_POLICIES_USERPOLICIES_ALLOWED_KEY, true);
    try (MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(0).build()) {
      DistributedFileSystem fs = cluster.getFileSystem();
      ErasureCodingPolicy newPolicy1 =
          new ErasureCodingPolicy(new ECSchema("rs", 5, 3), 1024 * 1024);
      AddErasureCodingPolicyResponse[] response =
          fs.addErasureCodingPolicies(new ErasureCodingPolicy[] {newPolicy1});
      assertTrue(response[0].isSucceed());
      assertNull(response[0].getErrorMsg());
    }
  }
}