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

import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.InconsistentStorageStateException;
import org.apache.hadoop.ozone.container.common.DataNodeLayoutVersion;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * This class tests DatanodeVersionFile.
 */
public class TestDatanodeVersionFile {

  @Rule
  public TemporaryFolder folder= new TemporaryFolder();

  @Test
  public void testCreateAndReadVersionFile() throws IOException{
    File versionFile = folder.newFile("Version");
    String uuid = UUID.randomUUID().toString();
    long now = Time.now();
    int lv = DataNodeLayoutVersion.getLatestVersion().getVersion();

    DatanodeVersionFile dnVersionFile = new DatanodeVersionFile(uuid, now, lv);

    dnVersionFile.createVersionFile(versionFile);

    //Check VersionFile exists
    assertTrue(versionFile.exists());

    Properties properties = dnVersionFile.readFrom(versionFile);

    assertEquals(uuid, properties.getProperty(OzoneConsts.SCM_ID));
    assertEquals(String.valueOf(now), properties.get(OzoneConsts.CTIME));
    assertEquals(String.valueOf(lv), properties.get(OzoneConsts.LAYOUTVERSION));

    DatanodeVersionFile.verifyCreationTime(String.valueOf(properties.get(
        OzoneConsts.CTIME)));
    DatanodeVersionFile.verifyLayOutVersion(String.valueOf(properties
        .getProperty(OzoneConsts.LAYOUTVERSION)));
    DatanodeVersionFile.verifyScmUuid(uuid, String.valueOf(properties
        .getProperty(OzoneConsts.SCM_ID)));


  }

  @Test
  public void testVerifyUuid() throws IOException{
    String uuid = UUID.randomUUID().toString();
    try {
      DatanodeVersionFile.verifyScmUuid(uuid, uuid);
      DatanodeVersionFile.verifyScmUuid(uuid, UUID.randomUUID().toString());
      fail("Test failure in testVerifyUuid");
    } catch (InconsistentStorageStateException ex) {
      GenericTestUtils.assertExceptionContains("MisMatch of ScmUuid", ex);
    }
  }

  @Test
  public void testVerifyCTime() throws IOException{
    try {
      DatanodeVersionFile.verifyCreationTime(String.valueOf(Time.now()));
      DatanodeVersionFile.verifyCreationTime(null);
      fail("Test failure in testVerifyCTime");
    } catch (IllegalStateException ex) {
      GenericTestUtils.assertExceptionContains("Invalid creation Time.", ex);
    }
  }

  @Test
  public void testVerifyLayOut() throws IOException{
    String lv = String.valueOf(DataNodeLayoutVersion.getLatestVersion()
        .getVersion());
    try {
      DatanodeVersionFile.verifyLayOutVersion(lv);
      DatanodeVersionFile.verifyLayOutVersion(null);
      fail("Test failure in testVerifyLayOut");
    } catch (IllegalStateException ex) {
      GenericTestUtils.assertExceptionContains("Invalid layOutVersion.", ex);
    }
  }

  @Test
  public void testGetVersionFile() throws IOException {
    StorageLocation location = StorageLocation.parse("/tmp/disk1");
    String scmId = UUID.randomUUID().toString();
    assertEquals(new File("/tmp/disk1/hdds/" + scmId + "/current/VERSION"),
        DatanodeVersionFile.getVersionFile(location, scmId));
    assertEquals(null, DatanodeVersionFile.getVersionFile(null, scmId));
  }

}
