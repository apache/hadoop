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

import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * This class tests Upload part request.
 */
public class TestPartUpload {

  private final static ObjectEndpoint REST = new ObjectEndpoint();
  private final static String BUCKET = "s3bucket";
  private final static String KEY = "key1";

  @BeforeClass
  public static void setUp() throws Exception {

    OzoneClientStub client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("ozone", BUCKET);


    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(client);
  }


  @Test
  public void testPartUpload() throws Exception {

    Response response = REST.multipartUpload(BUCKET, KEY, "", "", null);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(response.getStatus(), 200);

    String content = "Multipart Upload";
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());
    response = REST.put(BUCKET, KEY, content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

  }

  @Test
  public void testPartUploadWithOverride() throws Exception {

    Response response = REST.multipartUpload(BUCKET, KEY, "", "", null);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(response.getStatus(), 200);

    String content = "Multipart Upload";
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());
    response = REST.put(BUCKET, KEY, content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    String eTag = response.getHeaderString("ETag");

    // Upload part again with same part Number, the ETag should be changed.
    content = "Multipart Upload Changed";
    response = REST.put(BUCKET, KEY, content.length(), 1, uploadID, body);
    assertNotNull(response.getHeaderString("ETag"));
    assertNotEquals(eTag, response.getHeaderString("ETag"));

  }


  @Test
  public void testPartUploadWithIncorrectUploadID() throws Exception {
    try {
      String content = "Multipart Upload With Incorrect uploadID";
      ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());
      REST.put(BUCKET, KEY, content.length(), 1, "random", body);
      fail("testPartUploadWithIncorrectUploadID failed");
    } catch (OS3Exception ex) {
      assertEquals("NoSuchUpload", ex.getCode());
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }
}
