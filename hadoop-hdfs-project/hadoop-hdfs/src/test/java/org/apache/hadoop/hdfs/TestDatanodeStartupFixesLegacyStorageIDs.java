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
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.TestDFSUpgradeFromImage.ClusterVerifier;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * The test verifies that legacy storage IDs in older DataNode
 * images are replaced with UUID-based storage IDs. The startup may
 * or may not involve a Datanode Layout upgrade. Each test case uses
 * the following resource files.
 *
 *    1. testCaseName.tgz - NN and DN directories corresponding
 *                          to a specific layout version.
 *    2. testCaseName.txt - Text file listing the checksum of each file
 *                          in the cluster and overall checksum. See
 *                          TestUpgradeFromImage for the file format.
 *
 * If any test case is renamed then the corresponding resource files must
 * also be renamed.
 */
public class TestDatanodeStartupFixesLegacyStorageIDs {

  /**
   * Perform a upgrade using the test image corresponding to
   * testCaseName.
   *
   * @param testCaseName
   * @param expectedStorageId if null, then the upgrade generates a new
   *                          unique storage ID.
   * @throws IOException
   */
  private static void runLayoutUpgradeTest(final String testCaseName,
                                           final String expectedStorageId)
      throws IOException {
    TestDFSUpgradeFromImage upgrade = new TestDFSUpgradeFromImage();
    upgrade.unpackStorage(testCaseName + ".tgz", testCaseName + ".txt");
    Configuration conf = new Configuration(TestDFSUpgradeFromImage.upgradeConf);
    initStorageDirs(conf, testCaseName);
    upgradeAndVerify(upgrade, conf, new ClusterVerifier() {
      @Override
      public void verifyClusterPostUpgrade(MiniDFSCluster cluster) throws IOException {
        // Verify that a GUID-based storage ID was generated.
        final String bpid = cluster.getNamesystem().getBlockPoolId();
        StorageReport[] reports =
            cluster.getDataNodes().get(0).getFSDataset().getStorageReports(bpid);
        assertThat(reports.length, is(1));
        final String storageID = reports[0].getStorage().getStorageID();
        assertTrue(DatanodeStorage.isValidStorageId(storageID));

        if (expectedStorageId != null) {
          assertThat(storageID, is(expectedStorageId));
        }
      }
    });
  }

  private static void initStorageDirs(final Configuration conf,
                                      final String testName) {
    conf.set(
        DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY, GenericTestUtils.getTempPath(
            testName + File.separator + "dfs" + File.separator + "data"));
    conf.set(
        DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, GenericTestUtils.getTempPath(
            testName + File.separator + "dfs" + File.separator + "name"));

  }

  private static void upgradeAndVerify(final TestDFSUpgradeFromImage upgrade,
                                       final Configuration conf,
                                       final ClusterVerifier verifier)
      throws IOException{
    upgrade.upgradeAndVerify(new MiniDFSCluster.Builder(conf)
                                 .numDataNodes(1)
                                 .manageDataDfsDirs(false)
                                 .manageNameDfsDirs(false), verifier);
  }

  /**
   * Upgrade from 2.2 (no storage IDs per volume) correctly generates
   * GUID-based storage IDs. Test case for HDFS-7575.
   */
  @Test (timeout=300000)
  public void testUpgradeFrom22FixesStorageIDs() throws IOException {
    runLayoutUpgradeTest(GenericTestUtils.getMethodName(), null);
  }

  /**
   * Startup from a 2.6-layout that has legacy storage IDs correctly
   * generates new storage IDs.
   * Test case for HDFS-7575.
   */
  @Test (timeout=300000)
  public void testUpgradeFrom22via26FixesStorageIDs() throws IOException {
    runLayoutUpgradeTest(GenericTestUtils.getMethodName(), null);
  }

  /**
   * Startup from a 2.6-layout that already has unique storage IDs does
   * not regenerate the storage IDs.
   * Test case for HDFS-7575.
   */
  @Test (timeout=300000)
  public void testUpgradeFrom26PreservesStorageIDs() throws IOException {
    // StorageId present in the image testUpgradeFrom26PreservesStorageId.tgz
    runLayoutUpgradeTest(GenericTestUtils.getMethodName(),
                         "DS-a0e39cfa-930f-4abd-813c-e22b59223774");
  }
}
