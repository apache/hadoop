/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3native;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.junit.Before;
import org.junit.After;
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


public class TestJets3tNativeFileSystemStore {
  private Configuration conf;
  private Jets3tNativeFileSystemStore store;
  private NativeS3FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    store = new Jets3tNativeFileSystemStore();
    fs = new NativeS3FileSystem(store);
    conf.setBoolean("fs.s3n.multipart.uploads.enabled", true);
    conf.setLong("fs.s3n.multipart.uploads.block.size", 64 * 1024 * 1024);
    fs.initialize(URI.create(conf.get("test.fs.s3n.name")), conf);
  }

  @After
  public void tearDown() throws Exception {
    try {
      store.purge("test");
    } catch (Exception e) {}
  }

  @BeforeClass
  public static void checkSettings() throws Exception {
    Configuration conf = new Configuration();
    assumeNotNull(conf.get("fs.s3n.awsAccessKeyId"));
    assumeNotNull(conf.get("fs.s3n.awsSecretAccessKey"));
    assumeNotNull(conf.get("test.fs.s3n.name"));
  }

  protected void writeRenameReadCompare(Path path, long len)
      throws IOException, NoSuchAlgorithmException {
    // If len > fs.s3n.multipart.uploads.block.size,
    // we'll use a multipart upload copy
    MessageDigest digest = MessageDigest.getInstance("MD5");
    OutputStream out = new BufferedOutputStream(
        new DigestOutputStream(fs.create(path, false), digest));
    for (long i = 0; i < len; i++) {
      out.write('Q');
    }
    out.flush();
    out.close();

    assertTrue("Exists", fs.exists(path));

    // Depending on if this file is over 5 GB or not,
    // rename will cause a multipart upload copy
    Path copyPath = path.suffix(".copy");
    fs.rename(path, copyPath);

    assertTrue("Copy exists", fs.exists(copyPath));

    // Download file from S3 and compare the digest against the original
    MessageDigest digest2 = MessageDigest.getInstance("MD5");
    InputStream in = new BufferedInputStream(
        new DigestInputStream(fs.open(copyPath), digest2));
    long copyLen = 0;
    while (in.read() != -1) {copyLen++;}
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
  public void testMediumUpload() throws IOException, NoSuchAlgorithmException {
    // Multipart upload, regular copy
    writeRenameReadCompare(new Path("/test/medium"), 33554432);    // 100 MB
  }

  /*
  Enable Multipart upload to run this test
  @Test
  public void testExtraLargeUpload()
      throws IOException, NoSuchAlgorithmException {
    // Multipart upload, multipart copy
    writeRenameReadCompare(new Path("/test/xlarge"), 5368709121L); // 5GB+1byte
  }
  */
}
