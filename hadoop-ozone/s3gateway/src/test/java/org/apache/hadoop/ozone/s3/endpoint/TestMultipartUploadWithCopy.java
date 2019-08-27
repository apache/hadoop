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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

/**
 * Class to test Multipart upload where parts are created with copy header.
 */

public class TestMultipartUploadWithCopy {

  private final static ObjectEndpoint REST = new ObjectEndpoint();

  private final static String BUCKET = "s3bucket";
  private final static String KEY = "key2";
  private final static String EXISTING_KEY = "key1";
  private static final String EXISTING_KEY_CONTENT = "testkey";
  private final static OzoneClientStub CLIENT = new OzoneClientStub();
  private static final int RANGE_FROM = 2;
  private static final int RANGE_TO = 4;

  @BeforeClass
  public static void setUp() throws Exception {

    ObjectStore objectStore = CLIENT.getObjectStore();
    objectStore.createS3Bucket("ozone", BUCKET);

    OzoneBucket bucket = getOzoneBucket(objectStore, BUCKET);

    byte[] keyContent = EXISTING_KEY_CONTENT.getBytes();
    try (OutputStream stream = bucket
        .createKey(EXISTING_KEY, keyContent.length, ReplicationType.RATIS,
            ReplicationFactor.THREE, new HashMap<>())) {
      stream.write(keyContent);
    }

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(CLIENT);
  }

  @Test
  public void testMultipart() throws Exception {

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(KEY);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(KEY, uploadID, partNumber, content);
    partsList.add(part1);

    partNumber = 2;
    Part part2 =
        uploadPartWithCopy(KEY, uploadID, partNumber,
            BUCKET + "/" + EXISTING_KEY, null);
    partsList.add(part2);

    partNumber = 3;
    Part part3 =
        uploadPartWithCopy(KEY, uploadID, partNumber,
            BUCKET + "/" + EXISTING_KEY,
            "bytes=" + RANGE_FROM + "-" + RANGE_TO);
    partsList.add(part3);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);

    completeMultipartUpload(KEY, completeMultipartUploadRequest,
        uploadID);

    OzoneBucket bucket = getOzoneBucket(CLIENT.getObjectStore(), BUCKET);
    try (InputStream is = bucket.readKey(KEY)) {
      String keyContent = new Scanner(is).useDelimiter("\\A").next();
      Assert.assertEquals(content + EXISTING_KEY_CONTENT + EXISTING_KEY_CONTENT
          .substring(RANGE_FROM, RANGE_TO), keyContent);
    }
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    setHeaders();
    Response response = REST.initializeMultipartUpload(BUCKET, key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(response.getStatus(), 200);

    return uploadID;

  }

  private Part uploadPart(String key, String uploadID, int partNumber, String
      content) throws IOException, OS3Exception {
    setHeaders();
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());
    Response response = REST.put(BUCKET, key, content.length(), partNumber,
        uploadID, body);
    assertEquals(response.getStatus(), 200);
    assertNotNull(response.getHeaderString("ETag"));
    Part part = new Part();
    part.seteTag(response.getHeaderString("ETag"));
    part.setPartNumber(partNumber);

    return part;
  }

  private Part uploadPartWithCopy(String key, String uploadID, int partNumber,
      String keyOrigin, String range) throws IOException, OS3Exception {
    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders.put(COPY_SOURCE_HEADER, keyOrigin);
    if (range != null) {
      additionalHeaders.put(COPY_SOURCE_HEADER_RANGE, range);

    }
    setHeaders(additionalHeaders);

    ByteArrayInputStream body = new ByteArrayInputStream("".getBytes());
    Response response = REST.put(BUCKET, key, 0, partNumber,
        uploadID, body);
    assertEquals(response.getStatus(), 200);

    CopyPartResult result = (CopyPartResult) response.getEntity();
    assertNotNull(result.getETag());
    assertNotNull(result.getLastModified());
    Part part = new Part();
    part.seteTag(result.getETag());
    part.setPartNumber(partNumber);

    return part;
  }

  private void completeMultipartUpload(String key,
      CompleteMultipartUploadRequest completeMultipartUploadRequest,
      String uploadID) throws IOException, OS3Exception {
    setHeaders();
    Response response = REST.completeMultipartUpload(BUCKET, key, uploadID,
        completeMultipartUploadRequest);

    assertEquals(response.getStatus(), 200);

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();

    assertEquals(completeMultipartUploadResponse.getBucket(), BUCKET);
    assertEquals(completeMultipartUploadResponse.getKey(), KEY);
    assertEquals(completeMultipartUploadResponse.getLocation(), BUCKET);
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  private void setHeaders(Map<String, String> additionalHeaders) {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    additionalHeaders
        .forEach((k, v) -> when(headers.getHeaderString(k)).thenReturn(v));
    REST.setHeaders(headers);
  }

  private void setHeaders() {
    setHeaders(new HashMap<>());
  }

  private static OzoneBucket getOzoneBucket(ObjectStore objectStore,
      String bucketName)
      throws IOException {

    String ozoneBucketName = objectStore.getOzoneBucketName(bucketName);
    String ozoneVolumeName = objectStore.getOzoneVolumeName(bucketName);

    return objectStore.getVolume(ozoneVolumeName).getBucket(ozoneBucketName);
  }
}
