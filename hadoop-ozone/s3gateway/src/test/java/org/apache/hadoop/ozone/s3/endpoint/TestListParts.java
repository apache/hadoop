/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;

import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * This class test list parts request.
 */
public class TestListParts {


  private final static ObjectEndpoint REST = new ObjectEndpoint();
  private final static String BUCKET = "s3bucket";
  private final static String KEY = "key1";
  private static String uploadID;

  @BeforeClass
  public static void setUp() throws Exception {

    OzoneClientStub client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("ozone", BUCKET);


    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(client);

    Response response = REST.multipartUpload(BUCKET, KEY, "", "", null);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(response.getStatus(), 200);

    String content = "Multipart Upload";
    ByteArrayInputStream body = new ByteArrayInputStream(content.getBytes());
    response = REST.put(BUCKET, KEY, content.length(), 1, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    response = REST.put(BUCKET, KEY, content.length(), 2, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));

    response = REST.put(BUCKET, KEY, content.length(), 3, uploadID, body);

    assertNotNull(response.getHeaderString("ETag"));
  }

  @Test
  public void testListParts() throws Exception {
    Response response = REST.get(BUCKET, KEY, uploadID, 3, "0", null);

    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    Assert.assertFalse(listPartsResponse.getTruncated());
    Assert.assertTrue(listPartsResponse.getPartList().size() == 3);

  }

  @Test
  public void testListPartsContinuation() throws Exception {
    Response response = REST.get(BUCKET, KEY, uploadID, 2, "0", null);
    ListPartsResponse listPartsResponse =
        (ListPartsResponse) response.getEntity();

    Assert.assertTrue(listPartsResponse.getTruncated());
    Assert.assertTrue(listPartsResponse.getPartList().size() == 2);

    // Continue
    response = REST.get(BUCKET, KEY, uploadID, 2,
        Integer.toString(listPartsResponse.getNextPartNumberMarker()), null);
    listPartsResponse = (ListPartsResponse) response.getEntity();

    Assert.assertFalse(listPartsResponse.getTruncated());
    Assert.assertTrue(listPartsResponse.getPartList().size() == 1);

  }

  @Test
  public void testListPartsWithUnknownUploadID() throws Exception {
    try {
      Response response = REST.get(BUCKET, KEY, uploadID, 2, "0", null);
    } catch (OS3Exception ex) {
      Assert.assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getErrorMessage(),
          ex.getErrorMessage());
    }
  }


}
