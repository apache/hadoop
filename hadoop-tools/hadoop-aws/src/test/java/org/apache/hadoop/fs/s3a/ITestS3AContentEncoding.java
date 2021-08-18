/*
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

package org.apache.hadoop.fs.s3a;

import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.audit.S3AAuditConstants;
import org.apache.hadoop.fs.s3a.impl.StoreContext;

import static org.apache.hadoop.fs.s3a.Constants.CONTENT_ENCODING;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Tests of content encoding object meta data.
 */
public class ITestS3AContentEncoding extends AbstractS3ATestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestS3ACannedACLs.class);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(CONTENT_ENCODING, "gzip");

    return conf;
  }

  @Test
  public void testCreatedObjectsHaveEncoding() throws Throwable {
    S3AFileSystem fs = getFileSystem();
    Path dir = methodPath();
    fs.mkdirs(dir);
    Path path = new Path(dir, "1");
    ContractTestUtils.touch(fs, path);
    assertObjectHasEncoding(path);
  }

  /**
   * Assert that a given object has gzip encoding specified
   * @param path path
   */
  private void assertObjectHasEncoding(Path path) {
    S3AFileSystem fs = getFileSystem();

    StoreContext storeContext = fs.createStoreContext();
    AmazonS3 s3 = fs.getAmazonS3ClientForTesting("encoding");
    String key = storeContext.pathToKey(path);
    ObjectMetadata meta = s3.getObjectMetadata(storeContext.getBucket(),
        key);
    String encoding = meta.getContentEncoding();
    Assertions.assertThat(encoding)
        .describedAs("Encoding of object %s is gzip", path)
        .isEqualTo("gzip!");
  }
}
