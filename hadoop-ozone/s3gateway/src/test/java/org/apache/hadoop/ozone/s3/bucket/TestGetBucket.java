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

import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.object.ListObject;
import org.apache.hadoop.ozone.s3.object.ListObjectResponse;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing basic object list browsing.
 */
public class TestGetBucket {

  @Test
  public void listRoot() throws OS3Exception, IOException {

    ListObject getBucket = new ListObject();

    OzoneClient client = createClientWithKeys("file1", "dir1/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        getBucket.get("vol1", "b1", "/", null, null, 100, "", null);

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(1, getBucketResponse.getContents().size());
    Assert.assertEquals("file1",
        getBucketResponse.getContents().get(0).getKey());

  }

  @Test
  public void listDir() throws OS3Exception, IOException {

    ListObject getBucket = new ListObject();

    OzoneClient client = createClientWithKeys("dir1/file2", "dir1/dir2/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        getBucket.get("vol1", "b1", "/", null, null, 100, "dir1", null);

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(0, getBucketResponse.getContents().size());

  }

  @Test
  public void listSubDir() throws OS3Exception, IOException {

    ListObject getBucket = new ListObject();
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        getBucket.get("vol1", "b1", "/", null, null, 100, "dir1/", null);

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/dir2/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(1, getBucketResponse.getContents().size());
    Assert.assertEquals("dir1/file2",
        getBucketResponse.getContents().get(0).getKey());

  }

  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createVolume("vol1");
    client.getObjectStore().getVolume("vol1").createBucket("b1");
    OzoneBucket bucket =
        client.getObjectStore().getVolume("vol1").getBucket("b1");
    for (String key : keys) {
      bucket.createKey(key, 0).close();
    }
    return client;
  }
}