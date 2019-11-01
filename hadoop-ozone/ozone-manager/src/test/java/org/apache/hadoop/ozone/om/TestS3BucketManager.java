/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Tests for S3 Bucket Manager.
 */
public class TestS3BucketManager {
  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OzoneConfiguration conf;
  private OmMetadataManagerImpl metaMgr;
  private BucketManager bucketManager;
  private VolumeManager volumeManager;

  @Before
  public void init() throws IOException {
    conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    metaMgr = new OmMetadataManagerImpl(conf);
    volumeManager = new VolumeManagerImpl(metaMgr, conf);
    bucketManager = new BucketManagerImpl(metaMgr);
  }

  @Test
  public void testOzoneVolumeNameForUser() throws IOException {
    S3BucketManager s3BucketManager = new S3BucketManagerImpl(conf, metaMgr,
        volumeManager, bucketManager);
    String userName = "ozone";
    String volumeName = s3BucketManager.getOzoneVolumeNameForUser(userName);
    assertEquals(OzoneConsts.OM_S3_VOLUME_PREFIX + userName, volumeName);
  }

  @Test
  public void testOzoneVolumeNameForUserFails() throws IOException {
    S3BucketManager s3BucketManager = new S3BucketManagerImpl(conf, metaMgr,
        volumeManager, bucketManager);
    String userName = null;
    try {
      String volumeName = s3BucketManager.getOzoneVolumeNameForUser(userName);
      fail("testOzoneVolumeNameForUserFails failed");
    } catch (NullPointerException ex) {
      GenericTestUtils.assertExceptionContains("UserName cannot be null", ex);
    }

  }

  @Test
  public void testGetS3BucketMapping() throws IOException {
    S3BucketManager s3BucketManager = new S3BucketManagerImpl(conf, metaMgr,
        volumeManager, bucketManager);
    String userName = "bilbo";
    metaMgr.getS3Table().put("newBucket",
        s3BucketManager.formatOzoneVolumeName(userName) + "/newBucket");
    String mapping = s3BucketManager.getOzoneBucketMapping("newBucket");
    Assert.assertTrue(mapping.startsWith("s3bilbo/"));
    Assert.assertTrue(mapping.endsWith("/newBucket"));
  }

  @Test
  public void testGetOzoneNames() throws IOException {
    S3BucketManager s3BucketManager = new S3BucketManagerImpl(conf, metaMgr,
        volumeManager, bucketManager);
    String userName = "batman";
    String s3BucketName = "gotham";
    metaMgr.getS3Table().put(s3BucketName,
        s3BucketManager.formatOzoneVolumeName(userName) + "/" + s3BucketName);
    String volumeName = s3BucketManager.getOzoneVolumeName(s3BucketName);
    Assert.assertTrue(volumeName.equalsIgnoreCase("s3"+userName));
    String bucketName =s3BucketManager.getOzoneBucketName(s3BucketName);
    Assert.assertTrue(bucketName.equalsIgnoreCase(s3BucketName));
    // try to get a bucket that does not exist.
    thrown.expectMessage("No such S3 bucket.");
    s3BucketManager.getOzoneBucketMapping("raven");

  }
}