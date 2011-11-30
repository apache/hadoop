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

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

/**
 * Tests MiniDFS cluster setup/teardown and isolation.
 * Every instance is brought up with a new data dir, to ensure that
 * shutdown work in background threads don't interfere with bringing up
 * the new cluster.
 */
public class TestMiniDFSCluster {

  private static final String CLUSTER_1 = "cluster1";
  private static final String CLUSTER_2 = "cluster2";
  private static final String CLUSTER_3 = "cluster3";
  protected String testDataPath;
  protected File testDataDir;
  @Before
  public void setUp() {
    testDataPath = System.getProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA,
        "build/test/data");
    testDataDir = new File(new File(testDataPath).getParentFile(),
                           "miniclusters");


  }
  @After
  public void tearDown() {
    System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA, testDataPath);
  }

  /**
   * Verify that without system properties the cluster still comes up, provided
   * the configuration is set
   *
   * @throws Throwable on a failure
   */
  @Test
  public void testClusterWithoutSystemProperties() throws Throwable {
    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
    Configuration conf = new HdfsConfiguration();
    File testDataCluster1 = new File(testDataPath, CLUSTER_1);
    String c1Path = testDataCluster1.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    try {
      Assert.assertEquals(c1Path+"/data", cluster.getDataDirectory());
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * Bring up two clusters and assert that they are in different directories.
   * @throws Throwable on a failure
   */
  @Test
  public void testDualClusters() throws Throwable {
    File testDataCluster2 = new File(testDataPath, CLUSTER_2);
    File testDataCluster3 = new File(testDataPath, CLUSTER_3);
    Configuration conf = new HdfsConfiguration();
    String c2Path = testDataCluster2.getAbsolutePath();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c2Path);
    MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf).build();
    MiniDFSCluster cluster3 = null;
    try {
      String dataDir2 = cluster2.getDataDirectory();
      Assert.assertEquals(c2Path + "/data", dataDir2);
      //change the data dir
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR,
               testDataCluster3.getAbsolutePath());
      MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
      cluster3 = builder.build();
      String dataDir3 = cluster3.getDataDirectory();
      Assert.assertTrue("Clusters are bound to the same directory: " + dataDir2,
                        !dataDir2.equals(dataDir3));
    } finally {
      MiniDFSCluster.shutdownCluster(cluster3);
      MiniDFSCluster.shutdownCluster(cluster2);
    }
  }


}
