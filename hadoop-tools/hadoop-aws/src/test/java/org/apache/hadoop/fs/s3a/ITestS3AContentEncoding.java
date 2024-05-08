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

import java.io.IOException;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;

import static org.apache.hadoop.fs.s3a.Constants.CONTENT_ENCODING;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.raiseAsAssumption;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfNotEnabled;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.XA_CONTENT_ENCODING;
import static org.apache.hadoop.fs.s3a.impl.HeaderProcessing.decodeBytes;

/**
 * Tests of content encoding object meta data.
 * Some stores don't support gzip; rejection of the content encoding
 * is downgraded to a skipped test.
 */
public class ITestS3AContentEncoding extends AbstractS3ATestBase {

  private static final String GZIP = "gzip";

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    skipIfNotEnabled(conf, KEY_CONTENT_ENCODING_ENABLED,
        "Skipping storage class ACL tests");
    removeBaseAndBucketOverrides(conf, CONTENT_ENCODING);
    conf.set(CONTENT_ENCODING, GZIP);

    return conf;
  }

  @Test
  public void testCreatedObjectsHaveEncoding() throws Throwable {
    try {
      S3AFileSystem fs = getFileSystem();
      Path dir = methodPath();
      fs.mkdirs(dir);
      // even with content encoding enabled, directories do not have
      // encoding.
      Assertions.assertThat(getEncoding(dir))
          .describedAs("Encoding of object %s", dir)
          .isNull();
      Path path = new Path(dir, "1");
      ContractTestUtils.touch(fs, path);
      assertObjectHasEncoding(path);
      Path path2 = new Path(dir, "2");
      fs.rename(path, path2);
      assertObjectHasEncoding(path2);
    } catch (AWSUnsupportedFeatureException e) {
      LOG.warn("Object store does not support {} content encoding", GZIP, e);
      raiseAsAssumption(e);
    }
  }

  /**
   * Assert that a given object has gzip encoding specified.
   * @param path path
   *
   */
  private void assertObjectHasEncoding(Path path) throws Throwable {
    Assertions.assertThat(getEncoding(path))
        .describedAs("Encoding of object %s", path)
        .isEqualTo(GZIP);
  }

  /**
   * Get the encoding of a path.
   * @param path path
   * @return encoding string or null
   * @throws IOException IO Failure.
   */
  private String getEncoding(Path path) throws IOException {
    S3AFileSystem fs = getFileSystem();

    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    return decodeBytes(xAttrs.get(XA_CONTENT_ENCODING));
  }
}
