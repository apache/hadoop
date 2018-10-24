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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Test put object.
 */
public class TestPutObject {
  public static final String CONTENT = "0123456789";
  private String userName = "ozone";
  private String bucketName = "b1";
  private String keyName = "key1";
  private String destBucket = "b2";
  private String destkey = "key2";
  private String nonexist = "nonexist";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private ObjectEndpoint objectEndpoint;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    // Create bucket
    objectStoreStub.createS3Bucket(userName, bucketName);
    objectStoreStub.createS3Bucket("ozone1", destBucket);

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(clientStub);
  }

  @Test
  public void testPutObject() throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body = new ByteArrayInputStream(CONTENT.getBytes());
    objectEndpoint.setHeaders(headers);

    //WHEN
    Response response = objectEndpoint.put(bucketName, keyName,
        ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
        body);

    //THEN
    String volumeName = clientStub.getObjectStore()
        .getOzoneVolumeName(bucketName);
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getVolume(volumeName).getBucket(bucketName)
            .readKey(keyName);
    String keyContent =
        IOUtils.toString(ozoneInputStream, Charset.forName("UTF-8"));

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);
  }

  @Test
  public void testPutObjectWithSignedChunks() throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);

    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    //WHEN
    Response response = objectEndpoint.put(bucketName, keyName,
        ReplicationType.STAND_ALONE,
        ReplicationFactor.ONE,
        chunkedContent.length(),
        new ByteArrayInputStream(chunkedContent.getBytes()));

    //THEN
    String volumeName = clientStub.getObjectStore()
        .getOzoneVolumeName(bucketName);
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getVolume(volumeName).getBucket(bucketName)
            .readKey(keyName);
    String keyContent =
        IOUtils.toString(ozoneInputStream, Charset.forName("UTF-8"));

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("1234567890abcde", keyContent);
  }

  @Test
  public void testCopyObject() throws IOException, OS3Exception {
    // Put object in to source bucket
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body = new ByteArrayInputStream(CONTENT.getBytes());
    objectEndpoint.setHeaders(headers);
    keyName = "sourceKey";

    Response response = objectEndpoint.put(bucketName, keyName,
        ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
        body);

    String volumeName = clientStub.getObjectStore().getOzoneVolumeName(
        bucketName);

    OzoneInputStream ozoneInputStream = clientStub.getObjectStore().getVolume(
        volumeName).getBucket(bucketName).readKey(keyName);

    String keyContent = IOUtils.toString(ozoneInputStream, Charset.forName(
        "UTF-8"));

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);


    // Add copy header, and then call put
    when(headers.getHeaderString("x-amz-copy-source")).thenReturn(
        bucketName  + "/" + keyName);

    response = objectEndpoint.put(destBucket, destkey,
        ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
        body);

    // Check destination key and response
    volumeName = clientStub.getObjectStore().getOzoneVolumeName(destBucket);
    ozoneInputStream = clientStub.getObjectStore().getVolume(volumeName)
        .getBucket(destBucket).readKey(destkey);

    keyContent = IOUtils.toString(ozoneInputStream, Charset.forName("UTF-8"));

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);

    // source and dest same
    try {
      objectEndpoint.put(bucketName, keyName, ReplicationType.STAND_ALONE,
          ReplicationFactor.ONE, CONTENT.length(), body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getErrorMessage().contains("This copy request is " +
          "illegal"));
    }

    // source bucket not found
    try {
      when(headers.getHeaderString("x-amz-copy-source")).thenReturn(
          nonexist + "/"  + keyName);
      response = objectEndpoint.put(destBucket, destkey,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
          body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    // dest bucket not found
    try {
      when(headers.getHeaderString("x-amz-copy-source")).thenReturn(
          bucketName + "/" + keyName);
      response = objectEndpoint.put(nonexist, destkey,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
          body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    //Both source and dest bucket not found
    try {
      when(headers.getHeaderString("x-amz-copy-source")).thenReturn(
          nonexist + "/" + keyName);
      response = objectEndpoint.put(nonexist, destkey,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
          body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    // source key not found
    try {
      when(headers.getHeaderString("x-amz-copy-source")).thenReturn(
          bucketName + "/" + nonexist);
      response = objectEndpoint.put("nonexistent", keyName,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE, CONTENT.length(),
          body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

  }
}