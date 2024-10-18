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

package org.apache.hadoop.tools;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;

/**
 * A JUnit test for copying files recursively.
 */

public class TestDistCpSystemWithECFiles extends TestDistCpSystem {
  static DistributedFileSystem dfs;

  @BeforeClass
  public static void beforeClass() throws IOException {
    BLOCK_SIZE = 1024 * 1024;
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
    //conf.set("dfs.checksum.combine.mode", COMPOSITE_CRC.toString());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(6).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    enableECPolicies();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static void enableECPolicies() throws IOException {
    DFSTestUtil.enableAllECPolicies(dfs);
    ErasureCodingPolicy ecPolicy = SystemErasureCodingPolicies.getPolicies().get(1);
    dfs.getClient().setErasureCodingPolicy("/", ecPolicy.getName());
  }
}