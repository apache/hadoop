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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.aliyun.oss.Constants.REGION_KEY;
import static org.apache.hadoop.fs.aliyun.oss.Constants.SIGNATURE_VERSION_KEY;
import static org.apache.hadoop.fs.contract.ContractTestUtils.createFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests Aliyun OSS system.
 */
public class ITAliyunOSSSignatureV4 {
  private static final Logger LOG = LoggerFactory.getLogger(ITAliyunOSSSignatureV4.class);
  private Configuration conf;
  private URI testURI;
  private Path testFile = new Path("ITAliyunOSSSignatureV4/atestr");

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    String bucketUri = conf.get("test.fs.oss.name");
    LOG.debug("bucketUri={}", bucketUri);
    testURI = URI.create(bucketUri);
  }

  @Test
  public void testV4() throws IOException {
    conf.set(SIGNATURE_VERSION_KEY, "V4");
    conf.set(REGION_KEY, "cn-hongkong");
    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
    fs.initialize(testURI, conf);
    assumeTrue(fs != null);

    createFile(fs, testFile, true, dataset(256, 0, 255));
    FileStatus status = fs.getFileStatus(testFile);
    fs.delete(testFile);
    fs.close();
  }

  @Test
  public void testDefaultSignatureVersion() throws IOException {
    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
    fs.initialize(testURI, conf);
    assumeTrue(fs != null);

    Path testFile2 = new Path("/test/atestr");
    createFile(fs, testFile2, true, dataset(256, 0, 255));
    FileStatus status = fs.getFileStatus(testFile2);
    fs.delete(testFile2);
    fs.close();
  }

  @Test
  public void testV4WithoutRegion() throws IOException {
    conf.set(SIGNATURE_VERSION_KEY, "V4");
    AliyunOSSFileSystem fs = new AliyunOSSFileSystem();
    IOException expectedException = null;
    try {
      fs.initialize(testURI, conf);
    } catch (IOException e) {
      LOG.warn("use V4 , but do not set region, get exception={}", e);
      expectedException = e;
      assertEquals("use V4 , but do not set region", e.getMessage(),
              "SignVersion is V4 but region is empty");
    }
    assertNotNull(expectedException);
  }
}
