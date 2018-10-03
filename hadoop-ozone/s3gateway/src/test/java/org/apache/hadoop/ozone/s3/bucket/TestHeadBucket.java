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

package org.apache.hadoop.ozone.s3.bucket;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class test HeadBucket functionality.
 */
public class TestHeadBucket {

  private String volumeName = "myVolume";
  private String bucketName = "myBucket";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private HeadBucket headBucket;

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    // Create volume and bucket
    objectStoreStub.createVolume(volumeName);

    OzoneVolume volumeStub = objectStoreStub.getVolume(volumeName);
    volumeStub.createBucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    headBucket = new HeadBucket();
    headBucket.setClient(clientStub);


  }

  @Test
  public void testHeadBucket() throws Exception {

    Response response = headBucket.head(volumeName, bucketName);
    assertEquals(200, response.getStatus());
    assertEquals(headBucket.getRequestId(), response.getHeaderString(
        "x-amz-request-id"));
  }

  @Test
  public void testHeadFail() {
    try {
      headBucket.head(volumeName, "unknownbucket");
    } catch (Exception ex) {
      if (ex instanceof OS3Exception) {
        assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(),
            ((OS3Exception) ex).getCode());
        assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(), (
            (OS3Exception) ex).getErrorMessage());
        assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getResource(), (
            (OS3Exception) ex).getResource());
        assertEquals(headBucket.getRequestId(), (
            (OS3Exception) ex).getRequestId());
      } else {
        fail("testHeadFail failed");
      }
    }
  }
}
