/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.aliyun.oss.Constants.MAX_PAGING_KEYS_DEFAULT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

/**
 * Test the bridging logic between Hadoop's abstract filesystem and
 * Aliyun OSS.
 */
public class TestAliyunOSSFileSystemStore {
  private Configuration conf;
  private AliyunOSSFileSystemStore store;
  private AliyunOSSFileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = new AliyunOSSFileSystem();
    fs.initialize(URI.create(conf.get("test.fs.oss.name")), conf);
    store = fs.getStore();
  }

  @After
  public void tearDown() throws Exception {
    try {
      store.purge("test");
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  @BeforeClass
  public static void checkSettings() throws Exception {
    Configuration conf = new Configuration();
    assumeNotNull(conf.get(Constants.ACCESS_KEY_ID));
    assumeNotNull(conf.get(Constants.ACCESS_KEY_SECRET));
    assumeNotNull(conf.get("test.fs.oss.name"));
  }

  protected void writeRenameReadCompare(Path path, long len)
      throws IOException, NoSuchAlgorithmException {
    MessageDigest digest = MessageDigest.getInstance("MD5");
    OutputStream out = new BufferedOutputStream(
        new DigestOutputStream(fs.create(path, false), digest));
    for (long i = 0; i < len; i++) {
      out.write('Q');
    }
    out.flush();
    out.close();

    assertTrue("Exists", fs.exists(path));

    ObjectMetadata srcMeta = fs.getStore().getObjectMetadata(
        path.toUri().getPath().substring(1));

    Path copyPath = path.suffix(".copy");
    fs.rename(path, copyPath);

    assertTrue("Copy exists", fs.exists(copyPath));
    // file type should not change
    ObjectMetadata dstMeta = fs.getStore().getObjectMetadata(
        copyPath.toUri().getPath().substring(1));
    assertEquals(srcMeta.getObjectType(), dstMeta.getObjectType());
    // Download file from Aliyun OSS and compare the digest against the original
    MessageDigest digest2 = MessageDigest.getInstance("MD5");
    InputStream in = new BufferedInputStream(
        new DigestInputStream(fs.open(copyPath), digest2));
    long copyLen = 0;
    while (in.read() != -1) {
      copyLen++;
    }
    in.close();

    assertEquals("Copy length matches original", len, copyLen);
    assertArrayEquals("Digests match", digest.digest(), digest2.digest());
  }

  @Test
  public void testSmallUpload() throws IOException, NoSuchAlgorithmException {
    // Regular upload, regular copy
    writeRenameReadCompare(new Path("/test/small"), 16384);
  }

  @Test
  public void testLargeUpload()
      throws IOException, NoSuchAlgorithmException {
    // Multipart upload, shallow copy
    writeRenameReadCompare(new Path("/test/xlarge"),
        Constants.MULTIPART_UPLOAD_PART_SIZE_DEFAULT + 1);
  }

  @Test
  public void testDeleteObjects() throws IOException, NoSuchAlgorithmException {
    // generate test files
    final int files = 10;
    final long size = 5 * 1024 * 1024;
    final String prefix = "dir";
    for (int i = 0; i < files; i++) {
      Path path = new Path(String.format("/%s/testFile-%d.txt", prefix, i));
      ContractTestUtils.generateTestFile(this.fs, path, size, 256, 255);
    }
    OSSListRequest listRequest =
        store.createListObjectsRequest(prefix, MAX_PAGING_KEYS_DEFAULT, null, null, true);
    List<String> keysToDelete = new ArrayList<>();
    OSSListResult objects = store.listObjects(listRequest);
    assertEquals(files, objects.getObjectSummaries().size());

    // test delete files
    for (OSSObjectSummary objectSummary : objects.getObjectSummaries()) {
      keysToDelete.add(objectSummary.getKey());
    }
    store.deleteObjects(keysToDelete);
    objects = store.listObjects(listRequest);
    assertEquals(0, objects.getObjectSummaries().size());
  }
}
