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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.web.response.BucketInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test Ozone Bucket Info operation.
 */
public class TestBucketInfo {
  @Test
  public void testBucketInfoJson() throws IOException {
    BucketInfo bucketInfo = new BucketInfo("volumeName", "bucketName");
    String bucketInfoString = bucketInfo.toJsonString();
    BucketInfo newBucketInfo = BucketInfo.parse(bucketInfoString);
    assertEquals(bucketInfo, newBucketInfo);
  }

  @Test
  public void testBucketInfoDBString() throws IOException {
    BucketInfo bucketInfo = new BucketInfo("volumeName", "bucketName");
    String bucketInfoString = bucketInfo.toDBString();
    BucketInfo newBucketInfo = BucketInfo.parse(bucketInfoString);
    assertEquals(bucketInfo, newBucketInfo);
  }

  @Test
  public void testBucketInfoAddAcls() throws IOException {
    BucketInfo bucketInfo = new BucketInfo("volumeName", "bucketName");
    String bucketInfoString = bucketInfo.toDBString();
    BucketInfo newBucketInfo = BucketInfo.parse(bucketInfoString);
    assertEquals(bucketInfo, newBucketInfo);
    List<OzoneAcl> aclList = new ArrayList<>();

    aclList.add(OzoneAcl.parseAcl("user:bilbo:r"));
    aclList.add(OzoneAcl.parseAcl("user:samwise:rw"));
    newBucketInfo.setAcls(aclList);

    assertNotNull(newBucketInfo.getAcls());
    assertEquals(2, newBucketInfo.getAcls().size());
  }

  @Test
  public void testBucketInfoVersionAndType() throws IOException {
    BucketInfo bucketInfo = new BucketInfo("volumeName", "bucketName");
    bucketInfo.setVersioning(OzoneConsts.Versioning.ENABLED);
    bucketInfo.setStorageType(StorageType.DISK);

    String bucketInfoString = bucketInfo.toDBString();

    BucketInfo newBucketInfo = BucketInfo.parse(bucketInfoString);
    assertEquals(bucketInfo, newBucketInfo);
  }

}
