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

package org.apache.hadoop.fs.obs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.internal.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.obs.OBSTestConstants.*;
import static org.apache.hadoop.fs.obs.OBSConstants.*;

/**
 * Utilities for the OBS tests.
 */
public final class OBSTestUtils {

  /**
   * Create the test filesystem.
   * <p>
   * If the test.fs.obs.name property is not set, this will trigger a JUnit
   * failure.
   * <p>
   * Multipart purging is enabled.
   *
   * @param conf configuration
   * @return the FS
   * @throws IOException                 IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  public static OBSFileSystem createTestFileSystem(Configuration conf)
      throws IOException {
    return createTestFileSystem(conf, false);
  }

  /**
   * Create the test filesystem with or without multipart purging
   * <p>
   * If the test.fs.obs.name property is not set, this will trigger a JUnit
   * failure.
   *
   * @param conf  configuration
   * @param purge flag to enable Multipart purging
   * @return the FS
   * @throws IOException                 IO Problems
   * @throws AssumptionViolatedException if the FS is not named
   */
  @SuppressWarnings("deprecation")
  public static OBSFileSystem createTestFileSystem(Configuration conf,
      boolean purge)
      throws IOException {

    String fsname = conf.getTrimmed(TEST_FS_OBS_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals(OBSConstants.OBS_SCHEME);
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TEST_FS_OBS_NAME);
    }
    OBSFileSystem fs1 = new OBSFileSystem();
    //enable purging in tests
    if (purge) {
      conf.setBoolean(PURGE_EXISTING_MULTIPART, true);
      // but a long delay so that parallel multipart tests don't
      // suddenly start timing out
      conf.setInt(PURGE_EXISTING_MULTIPART_AGE, 30 * 60);
    }
    fs1.initialize(testURI, conf);
    return fs1;
  }

  /**
   * Create a test path, using the value of
   * {@link OBSTestConstants#TEST_UNIQUE_FORK_ID}
   * if it is set.
   *
   * @param defVal default value
   * @return a path
   */
  public static Path createTestPath(Path defVal) {
    String testUniqueForkId = System.getProperty(
        OBSTestConstants.TEST_UNIQUE_FORK_ID);
    return testUniqueForkId == null ? defVal :
        new Path("/" + testUniqueForkId, "test");
  }

  /**
   * This class should not be instantiated.
   */
  private OBSTestUtils() {
  }

}
