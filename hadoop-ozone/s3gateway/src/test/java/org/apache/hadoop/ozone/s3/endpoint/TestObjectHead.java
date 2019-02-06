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
package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test head object.
 */
public class TestObjectHead {
  private String bucketName = "b1";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    // Create volume and bucket
    objectStoreStub.createS3Bucket("bilbo", bucketName);
    String volName = objectStoreStub.getOzoneVolumeName(bucketName);

    bucket = objectStoreStub.getVolume(volName).getBucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    keyEndpoint = new ObjectEndpoint();
    keyEndpoint.setClient(clientStub);
  }

  @Test
  public void testHeadObject() throws Exception {
    //GIVEN
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length, ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(value.getBytes(UTF_8).length,
        Long.parseLong(response.getHeaderString("Content-Length")));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

  }

  @Test
  public void testHeadFailByBadName() throws Exception {
    //Head an object that doesn't exist.
    try {
      Response response =  keyEndpoint.head(bucketName, "badKeyName");
      Assert.assertEquals(404, response.getStatus());
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchObject"));
      Assert.assertTrue(ex.getErrorMessage().contains("object does not exist"));
      Assert.assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }
}
