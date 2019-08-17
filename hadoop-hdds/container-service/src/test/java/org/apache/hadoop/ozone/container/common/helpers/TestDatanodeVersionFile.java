/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.HddsVolumeUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * This class tests {@link DatanodeVersionFile}.
 */
public class TestDatanodeVersionFile {

  private File versionFile;
  private DatanodeVersionFile dnVersionFile;
  private Properties properties;

  private String storageID;
  private String clusterID;
  private String datanodeUUID;
  private long cTime;
  private int lv;

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    versionFile = folder.newFile("Version");
    storageID = UUID.randomUUID().toString();
    clusterID = UUID.randomUUID().toString();
    datanodeUUID = UUID.randomUUID().toString();
    cTime = Time.now();
    lv = DataNodeLayoutVersion.getLatestVersion().getVersion();

    dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, cTime, lv);

    dnVersionFile.createVersionFile(versionFile);

    properties = dnVersionFile.readFrom(versionFile);
  }

  @Test
  public void testCreateAndReadVersionFile() throws IOException{

    //Check VersionFile exists
    assertTrue(versionFile.exists());

    assertEquals(storageID, HddsVolumeUtil.getStorageID(
        properties, versionFile));
    assertEquals(clusterID, HddsVolumeUtil.getClusterID(
        properties, versionFile, clusterID));
    assertEquals(datanodeUUID, HddsVolumeUtil.getDatanodeUUID(
        properties, versionFile, datanodeUUID));
    assertEquals(cTime, HddsVolumeUtil.getCreationTime(
        properties, versionFile));
    assertEquals(lv, HddsVolumeUtil.getLayOutVersion(
        properties, versionFile));
  }

  @Test
  public void testIncorrectClusterId() throws IOException{
    try {
      String randomClusterID = UUID.randomUUID().toString();
      HddsVolumeUtil.getClusterID(properties, versionFile,
          randomClusterID);
      fail("Test failure in testIncorrectClusterId");
    } catch (InconsistentStorageStateException ex) {
      GenericTestUtils.assertExceptionContains("Mismatched ClusterIDs", ex);
    }
  }

  @Test
  public void testVerifyCTime() throws IOException{
    long invalidCTime = -10;
    dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, invalidCTime, lv);
    dnVersionFile.createVersionFile(versionFile);
    properties = dnVersionFile.readFrom(versionFile);

    try {
      HddsVolumeUtil.getCreationTime(properties, versionFile);
      fail("Test failure in testVerifyCTime");
    } catch (InconsistentStorageStateException ex) {
      GenericTestUtils.assertExceptionContains("Invalid Creation time in " +
          "Version File : " + versionFile, ex);
    }
  }

  @Test
  public void testVerifyLayOut() throws IOException{
    int invalidLayOutVersion = 100;
    dnVersionFile = new DatanodeVersionFile(
        storageID, clusterID, datanodeUUID, cTime, invalidLayOutVersion);
    dnVersionFile.createVersionFile(versionFile);
    Properties props = dnVersionFile.readFrom(versionFile);

    try {
      HddsVolumeUtil.getLayOutVersion(props, versionFile);
      fail("Test failure in testVerifyLayOut");
    } catch (InconsistentStorageStateException ex) {
      GenericTestUtils.assertExceptionContains("Invalid layOutVersion.", ex);
    }
  }
}
