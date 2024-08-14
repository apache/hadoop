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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.junit.internal.AssumptionViolatedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.aliyun.oss.TestAliyunOSSFileSystemContract.TEST_FS_OSS_NAME;
import static org.apache.hadoop.util.Preconditions.checkNotNull;

/**
 * Utility class for Aliyun OSS Tests.
 */
public final class AliyunOSSTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      AliyunOSSTestUtils.class);

  private AliyunOSSTestUtils() {
  }

  /**
   * Create the test filesystem.
   *
   * If the test.fs.oss.name property is not set,
   * tests will fail.
   *
   * @param conf configuration
   * @return the FS
   * @throws IOException
   */
  public static AliyunOSSFileSystem createTestFileSystem(Configuration conf)
      throws IOException {
    AliyunOSSFileSystem ossfs = new AliyunOSSFileSystem();
    ossfs.initialize(getURI(conf), conf);
    return ossfs;
  }

  public static FileContext createTestFileContext(Configuration conf) throws
      IOException {
    return FileContext.getFileContext(getURI(conf), conf);
  }

  private static URI getURI(Configuration conf) {
    String fsname = conf.getTrimmed(
        TEST_FS_OSS_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_OSS);
    }

    if (!liveTest) {
      throw new AssumptionViolatedException("No test filesystem in "
          + TEST_FS_OSS_NAME);
    }
    return testURI;
  }
  /**
   * Generate unique test path for multiple user tests.
   *
   * @return root test path
   */
  public static String generateUniqueTestPath() {
    String testUniqueForkId = System.getProperty("test.unique.fork.id");
    return testUniqueForkId == null ? "/test" :
        "/" + testUniqueForkId + "/test";
  }

  /**
   * Turn off FS Caching: use if a filesystem with different options from
   * the default is required.
   * @param conf configuration to patch
   */
  public static void disableFilesystemCaching(Configuration conf) {
    conf.setBoolean(TestAliyunOSSFileSystemContract.FS_OSS_IMPL_DISABLE_CACHE,
        true);
  }

  /**
   * Get the name of the test bucket.
   * @param conf configuration to scan.
   * @return the bucket name from the config.
   * @throws NullPointerException: no test bucket
   */
  public static String getTestBucketName(final Configuration conf) {
    String bucket = checkNotNull(conf.get(TEST_FS_OSS_NAME),
        "No test bucket");
    return URI.create(bucket).getHost();
  }

  /**
   * Remove any values from the test bucket and the base values too.
   * @param conf config
   * @param options list of fs.oss options to remove
   */
  public static void removeBaseAndBucketOverrides(
      final Configuration conf,
      final String... options) {
    for (String option : options) {
      conf.unset(option);
    }
  }
}
