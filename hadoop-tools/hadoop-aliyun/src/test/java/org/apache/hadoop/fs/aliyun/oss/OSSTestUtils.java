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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Random;

/**
 * Utility class for OSS Tests.
 */
public final class OSSTestUtils {

  private OSSTestUtils() {
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
    String fsname = conf.getTrimmed(
        TestOSSFileSystemContract.TEST_FS_OSS_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(Constants.FS_OSS);
    }

    if (!liveTest) {
      throw new AssumptionViolatedException("No test filesystem in "
          + TestOSSFileSystemContract.TEST_FS_OSS_NAME);
    }
    AliyunOSSFileSystem ossfs = new AliyunOSSFileSystem();
    ossfs.initialize(testURI, conf);
    return ossfs;
  }

  /**
   * Generate unique test path for multiple user tests.
   *
   * @return root test path
   */
  public static String generateUniqueTestPath() {
    Long time = new Date().getTime();
    Random rand = new Random();
    return "/test_" + Long.toString(time) + "_"
        + Long.toString(Math.abs(rand.nextLong()));
  }
}
