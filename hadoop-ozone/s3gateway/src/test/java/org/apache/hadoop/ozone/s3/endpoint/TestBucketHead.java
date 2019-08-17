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

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientStub;

import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * This class test HeadBucket functionality.
 */
public class TestBucketHead {

  private String bucketName = "myBucket";
  private String userName = "ozone";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private BucketEndpoint bucketEndpoint;

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    objectStoreStub.createS3Bucket(userName, bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);
  }

  @Test
  public void testHeadBucket() throws Exception {

    Response response = bucketEndpoint.head(bucketName);
    assertEquals(200, response.getStatus());

  }

  @Test
  public void testHeadFail() throws Exception {
    Response response = bucketEndpoint.head("unknownbucket");
    Assert.assertEquals(400, response.getStatus());
  }
}
