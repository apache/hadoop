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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestDatanodeLayoutUpgrade {
  private static final String HADOOP_DATANODE_DIR_TXT =
      "hadoop-datanode-dir.txt";
  private static final String HADOOP24_DATANODE = "hadoop-24-datanode-dir.tgz";
  private static final String HADOOP_56_DN_LAYOUT_TXT =
      "hadoop-to-57-dn-layout-dir.txt";
  private static final String HADOOP_56_DN_LAYOUT =
      "hadoop-56-layout-datanode-dir.tgz";

  /**
   * Upgrade from LDir-based layout to 32x32 block ID-based layout (-57) --
   * change described in HDFS-6482 and HDFS-8791
   */
  @Test
  public void testUpgradeToIdBasedLayout() throws IOException {
    TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
    upgrade.unpackStorage(HADOOP24_DATANODE, HADOOP_DATANODE_DIR_TXT);
    Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
    conf.set(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, GenericTestUtils.getTestDir(
            "dfs" + File.separator + "data").toURI().toString());
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, GenericTestUtils.getTestDir(
            "dfs" + File.separator + "name").toURI().toString());
    upgrade.upgradeAndVerify(new MiniDFSCluster.Builder(conf).numDataNodes(1)
    .manageDataDfsDirs(false).manageNameDfsDirs(false), null);
  }

  /**
   * Test upgrade from block ID-based layout 256x256 (-56) to block ID-based
   * layout 32x32 (-57)
   */
  @Test
  public void testUpgradeFrom256To32Layout() throws IOException {
    TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
    upgrade.unpackStorage(HADOOP_56_DN_LAYOUT, HADOOP_56_DN_LAYOUT_TXT);
    Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
    conf.set(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, GenericTestUtils.getTestDir(
            "dfs" + File.separator + "data").toURI().toString());
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, GenericTestUtils.getTestDir(
            "dfs" + File.separator + "name").toURI().toString());
    upgrade.upgradeAndVerify(new MiniDFSCluster.Builder(conf).numDataNodes(1)
        .manageDataDfsDirs(false).manageNameDfsDirs(false), null);
  }
}
