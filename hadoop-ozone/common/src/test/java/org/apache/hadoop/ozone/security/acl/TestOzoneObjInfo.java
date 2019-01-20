/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType;

/**
 * Test class for {@link OzoneObjInfo}.
 * */
public class TestOzoneObjInfo {

  private OzoneObjInfo objInfo;
  private OzoneObjInfo.Builder builder;
  private String volume = "vol1";
  private String bucket = "bucket1";
  private String key = "key1";
  private static final OzoneObj.StoreType STORE = OzoneObj.StoreType.OZONE;


  @Test
  public void testGetVolumeName() {

    builder = getBuilder(volume, bucket, key);
    objInfo = builder.build();
    assertEquals(objInfo.getVolumeName(), volume);

    objInfo = getBuilder(null, null, null).build();
    assertEquals(objInfo.getVolumeName(), null);

    objInfo = getBuilder(volume, null, null).build();
    assertEquals(objInfo.getVolumeName(), volume);
  }

  private OzoneObjInfo.Builder getBuilder(String withVolume,
      String withBucket,
      String withKey) {
    return OzoneObjInfo.Builder.newBuilder()
        .setResType(ResourceType.VOLUME)
        .setStoreType(STORE)
        .setVolumeName(withVolume)
        .setBucketName(withBucket)
        .setKeyName(withKey);
  }

  @Test
  public void testGetBucketName() {
    objInfo = getBuilder(volume, bucket, key).build();
    assertEquals(objInfo.getBucketName(), bucket);

    objInfo =getBuilder(volume, null, null).build();
    assertEquals(objInfo.getBucketName(), null);

    objInfo =getBuilder(null, bucket, null).build();
    assertEquals(objInfo.getBucketName(), bucket);
  }

  @Test
  public void testGetKeyName() {
    objInfo = getBuilder(volume, bucket, key).build();
    assertEquals(objInfo.getKeyName(), key);

    objInfo =getBuilder(volume, null, null).build();
    assertEquals(objInfo.getKeyName(), null);

    objInfo =getBuilder(null, bucket, null).build();
    assertEquals(objInfo.getKeyName(), null);

    objInfo =getBuilder(null, null, key).build();
    assertEquals(objInfo.getKeyName(), key);
  }
}