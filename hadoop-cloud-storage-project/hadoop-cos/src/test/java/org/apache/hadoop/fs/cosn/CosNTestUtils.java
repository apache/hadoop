/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.cosn;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.internal.AssumptionViolatedException;

/**
 * Utilities for the CosN tests.
 */
public final class CosNTestUtils {

  private CosNTestUtils() {
  }

  /**
   * Create the file system for test.
   *
   * @param configuration hadoop's configuration
   * @return The file system for test
   * @throws IOException If fail to create or initialize the file system.
   */
  public static CosNFileSystem createTestFileSystem(
      Configuration configuration) throws IOException {
    String fsName = configuration.getTrimmed(
        CosNTestConfigKey.TEST_COS_FILESYSTEM_CONF_KEY,
        CosNTestConfigKey.DEFAULT_TEST_COS_FILESYSTEM_CONF_VALUE);

    boolean liveTest = StringUtils.isNotEmpty(fsName);
    URI testUri;
    if (liveTest) {
      testUri = URI.create(fsName);
      liveTest = testUri.getScheme().equals(CosNFileSystem.SCHEME);
    } else {
      throw new AssumptionViolatedException("no test file system in " +
          fsName);
    }

    CosNFileSystem cosFs = new CosNFileSystem();
    cosFs.initialize(testUri, configuration);
    return cosFs;
  }

  /**
   * Create a dir path for test.
   * The value of {@link CosNTestConfigKey#TEST_UNIQUE_FORK_ID_KEY}
   * will be used if it is set.
   *
   * @param defVal default value
   * @return The test path
   */
  public static Path createTestPath(Path defVal) {
    String testUniqueForkId = System.getProperty(
        CosNTestConfigKey.TEST_UNIQUE_FORK_ID_KEY);
    return testUniqueForkId ==
        null ? defVal : new Path("/" + testUniqueForkId, "test");
  }
}
