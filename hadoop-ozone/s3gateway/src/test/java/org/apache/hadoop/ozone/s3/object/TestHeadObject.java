/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.s3.object;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import java.io.IOException;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

/**
 * Test head object.
 */
public class TestHeadObject {
  private String volName = "vol1";
  private String bucketName = "b1";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private HeadObject headObject;
  private OzoneBucket bucket;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    // Create volume and bucket
    objectStoreStub.createVolume(volName);
    OzoneVolume volumeStub = objectStoreStub.getVolume(volName);
    volumeStub.createBucket(bucketName);
    bucket = objectStoreStub.getVolume(volName).getBucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    headObject = new HeadObject();
    headObject.setClient(clientStub);
  }

  @Test
  public void testHeadObject() throws Exception {
    //GIVEN
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes().length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE);
    out.write(value.getBytes());
    out.close();

    //WHEN
    Response response = headObject.head(volName, bucketName, "key1");

    //THEN
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(value.getBytes().length,
        Long.parseLong(response.getHeaderString("Content-Length")));
  }

  @Test
  public void testHeadFailByBadName() throws Exception {
    //Head an object that doesn't exist.
    try {
      headObject.head(volName, bucketName, "badKeyName");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchObject"));
      Assert.assertTrue(ex.getErrorMessage().contains("object does not exist"));
      Assert.assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }
}
