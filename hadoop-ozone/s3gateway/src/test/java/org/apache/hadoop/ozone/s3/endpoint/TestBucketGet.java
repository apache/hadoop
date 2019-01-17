/**
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

import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Testing basic object list browsing.
 */
public class TestBucketGet {

  @Test
  public void listRoot() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient client = createClientWithKeys("file1", "dir1/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket
            .list("b1", "/", null, null, 100, "", null, null, null, null)
            .getEntity();

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(1, getBucketResponse.getContents().size());
    Assert.assertEquals("file1",
        getBucketResponse.getContents().get(0).getKey());

  }

  @Test
  public void listDir() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient client = createClientWithKeys("dir1/file2", "dir1/dir2/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, 100,
            "dir1", null, null, null, null).getEntity();

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(0, getBucketResponse.getContents().size());

  }

  @Test
  public void listSubDir() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket
            .list("b1", "/", null, null, 100, "dir1/", null, null,
                null, null)
            .getEntity();

    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("dir1/dir2/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());

    Assert.assertEquals(1, getBucketResponse.getContents().size());
    Assert.assertEquals("dir1/file2",
        getBucketResponse.getContents().get(0).getKey());

  }


  @Test
  public void listWithPrefixAndDelimiter() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, 100,
            "dir1", null, null, null, null).getEntity();

    Assert.assertEquals(3, getBucketResponse.getCommonPrefixes().size());

  }

  @Test
  public void listWithPrefixAndDelimiter1() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, 100,
            "", null, null, null, null).getEntity();

    Assert.assertEquals(3, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("file2", getBucketResponse.getContents().get(0)
        .getKey());

  }

  @Test
  public void listWithPrefixAndDelimiter2() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, 100,
            "dir1bh", null, null, "dir1/dir2/file2", null).getEntity();

    Assert.assertEquals(2, getBucketResponse.getCommonPrefixes().size());

  }

  @Test
  public void listWithContinuationToken() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null, maxKeys,
            "", null, null, null, null).getEntity();

    Assert.assertTrue(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 2);

    // 2nd time
    String continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null, maxKeys,
            "", null, continueToken, null, null).getEntity();
    Assert.assertTrue(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 2);


    continueToken = getBucketResponse.getNextToken();

    //3rd time
    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null, maxKeys,
            "", null, continueToken, null, null).getEntity();

    Assert.assertFalse(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 1);

  }

  @Test
  public void listWithContinuationTokenDirBreak()
      throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys(
            "test/dir1/file1",
            "test/dir1/file2",
            "test/dir1/file3",
            "test/dir2/file4",
            "test/dir2/file5",
            "test/dir2/file6",
            "test/dir3/file7",
            "test/file8");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;

    ListObjectResponse getBucketResponse;

    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, maxKeys,
            "test/", null, null, null, null).getEntity();

    Assert.assertEquals(0, getBucketResponse.getContents().size());
    Assert.assertEquals(2, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("test/dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());
    Assert.assertEquals("test/dir2/",
        getBucketResponse.getCommonPrefixes().get(1).getPrefix());

    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, maxKeys,
            "test/", null, getBucketResponse.getNextToken(), null, null)
            .getEntity();
    Assert.assertEquals(1, getBucketResponse.getContents().size());
    Assert.assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    Assert.assertEquals("test/dir3/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix());
    Assert.assertEquals("test/file8",
        getBucketResponse.getContents().get(0).getKey());

  }

  @Test
  /**
   * This test is with prefix and delimiter and verify continuation-token
   * behavior.
   */
  public void listWithContinuationToken1() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, maxKeys,
            "dir", null, null, null, null).getEntity();

    Assert.assertTrue(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getCommonPrefixes().size() == 2);

    // 2nd time
    String continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, maxKeys,
            "dir", null, continueToken, null, null).getEntity();
    Assert.assertTrue(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getCommonPrefixes().size() == 2);

    //3rd time
    continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", "/", null, null, maxKeys,
            "dir", null, continueToken, null, null).getEntity();

    Assert.assertFalse(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getCommonPrefixes().size() == 1);

  }

  @Test
  public void listWithContinuationTokenFail() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "dir1", "dir2", "dir3");

    getBucket.setClient(ozoneClient);

    try {
      ListObjectResponse getBucketResponse =
          (ListObjectResponse) getBucket.list("b1", "/", null, null, 2,
              "dir", null, "random", null, null).getEntity();
      fail("listWithContinuationTokenFail");
    } catch (OS3Exception ex) {
      Assert.assertEquals("random", ex.getResource());
      Assert.assertEquals("Invalid Argument", ex.getErrorMessage());
    }

  }


  @Test
  public void testStartAfter() throws IOException, OS3Exception {
    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null, 1000,
            null, null, null, null, null).getEntity();

    Assert.assertFalse(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 5);

    //As our list output is sorted, after seeking to startAfter, we shall
    // have 4 keys.
    String startAfter = "dir0/file1";

    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null,
            1000, null, null, null, startAfter, null).getEntity();

    Assert.assertFalse(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 4);

    getBucketResponse =
        (ListObjectResponse) getBucket.list("b1", null, null, null,
            1000, null, null, null, "random", null).getEntity();

    Assert.assertFalse(getBucketResponse.isTruncated());
    Assert.assertTrue(getBucketResponse.getContents().size() == 0);


  }

  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneClient client = new OzoneClientStub();

    client.getObjectStore().createS3Bucket("bilbo", "b1");
    String volume = client.getObjectStore().getOzoneVolumeName("b1");
    client.getObjectStore().getVolume(volume).createBucket("b1");
    OzoneBucket bucket =
        client.getObjectStore().getVolume(volume).getBucket("b1");
    for (String key : keys) {
      bucket.createKey(key, 0).close();
    }
    return client;
  }
}