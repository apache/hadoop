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

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.header.AuthenticationHeaderParser;

import org.apache.commons.lang3.RandomStringUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * This class test HeadBucket functionality.
 */
public class TestRootList {

  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private RootEndpoint rootEndpoint;
  private String userName = "ozone";

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();

    // Create HeadBucket and setClient to OzoneClientStub
    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);

    AuthenticationHeaderParser parser = new AuthenticationHeaderParser();
    parser.setAuthHeader("AWS " + userName +":secret");
    rootEndpoint.setAuthenticationHeaderParser(parser);
  }

  @Test
  public void testListBucket() throws Exception {

    // List operation should success even there is no bucket.
    ListBucketResponse response = rootEndpoint.get();
    assertEquals(0, response.getBucketsNum());

    String bucketBaseName = "bucket-";
    for(int i = 0; i < 10; i++) {
      objectStoreStub.createS3Bucket(userName,
          bucketBaseName + RandomStringUtils.randomNumeric(3));
    }
    response = rootEndpoint.get();
    assertEquals(10, response.getBucketsNum());
  }

}
