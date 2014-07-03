/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.contract.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.ContractOptions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;

import java.io.IOException;

/**
 * The contract of HDFS
 * This changes its feature set from platform for platform -the default
 * set is updated during initialization.
 */
public class HDFSContract extends AbstractFSContract {

  public static final String CONTRACT_HDFS_XML = "contract/hdfs.xml";
  public static final int BLOCK_SIZE = AbstractFSContractTestBase.TEST_FILE_LEN;
  private static MiniDFSCluster cluster;

  public HDFSContract(Configuration conf) {
    super(conf);
    //insert the base features
    addConfResource(CONTRACT_HDFS_XML);
  }

  public static void createCluster() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.addResource(CONTRACT_HDFS_XML);
    //hack in a 256 byte block size
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);

    cluster =
      new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    cluster.waitClusterUp();
  }

  public static void destroyCluster() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public static MiniDFSCluster getCluster() {
    return cluster;
  }

  @Override
  public void init() throws IOException {
    super.init();
    Assert.assertTrue("contract options not loaded",
                      isSupported(ContractOptions.IS_CASE_SENSITIVE, false));
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", cluster);
    return cluster.getFileSystem();
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public Path getTestPath() {
    Path path = new Path("/test");
    return path;
  }
}
