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
package org.apache.hadoop.fs.azurebfs.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.FILE;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_SECURE_SCHEME;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_SCALE_TEST_ENABLED;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Some Utils for ABFS tests.
 */
public final class AbfsTestUtils extends AbstractAbfsIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsTestUtils.class);

  private static final int TOTAL_THREADS_IN_POOL = 5;
  private static final String UNSET_PROPERTY = "unset";
  private static final int SCALE_TEST_TIMEOUT_SECONDS = 30 * 60;
  private static final boolean DEFAULT_SCALE_TESTS_ENABLED = false;
  public static final int SCALE_TEST_TIMEOUT_MILLIS = SCALE_TEST_TIMEOUT_SECONDS
      * 1000;

  public AbfsTestUtils() throws Exception {
    super();
  }

  /**
   * Turn off FS Caching: use if a filesystem with different options from
   * the default is required.
   * @param conf configuration to patch
   */
  public static void disableFilesystemCaching(Configuration conf) {
    // Disabling cache to make sure new configs are picked up.
    conf.setBoolean(String.format("fs.%s.impl.disable.cache", ABFS_SCHEME),
        true);
    conf.setBoolean(
        String.format("fs.%s.impl.disable.cache", ABFS_SECURE_SCHEME), true);
  }

  /**
   * Helper method to create files in the given directory.
   *
   * @param fs The AzureBlobFileSystem instance to use for file creation.
   * @param path The source path (directory).
   * @param numFiles The number of files to create.
   * @throws ExecutionException, InterruptedException If an error occurs during file creation.
   */
  public static void createFiles(AzureBlobFileSystem fs,
      Path path,
      int numFiles)
      throws ExecutionException, InterruptedException {
    ExecutorService executorService =
        Executors.newFixedThreadPool(TOTAL_THREADS_IN_POOL);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      final int iter = i;
      Future future = executorService.submit(() ->
          fs.create(new Path(path, FILE + iter + ".txt")));
      futures.add(future);
    }
    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();
  }

  /**
   * Assume that a condition is met. If not: log at WARN and
   * then throw an {@link TestAbortedException}.
   * @param message message in an assumption
   * @param condition condition to probe
   */
  public static void assume(String message, boolean condition) {
    if (!condition) {
      LOG.warn(message);
    }
    assumeThat(condition).as(message).isTrue();
  }

  /**
   * Get a string test property.
   * <ol>
   *   <li>Look up configuration value (which can pick up core-default.xml),
   *       using {@code defVal} as the default value (if conf != null).
   *   </li>
   *   <li>Fetch the system property.</li>
   *   <li>If the system property is not empty or "(unset)":
   *   it overrides the conf value.
   *   </li>
   * </ol>
   * This puts the build properties in charge of everything. It's not a
   * perfect design; having maven set properties based on a file, as ant let
   * you do, is better for customization.
   *
   * As to why there's a special (unset) value, see
   * @see <a href="http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven">
   *   Stack Overflow</a>
   * @param conf config: may be null
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */

  public static String getTestProperty(Configuration conf,
      String key,
      String defVal) {
    String confVal = conf != null
        ? conf.getTrimmed(key, defVal)
        : defVal;
    String propval = System.getProperty(key);
    return StringUtils.isNotEmpty(propval) && !UNSET_PROPERTY.equals(propval)
        ? propval : confVal;
  }

  /**
   * Get a long test property.
   * <ol>
   *   <li>Look up configuration value (which can pick up core-default.xml),
   *       using {@code defVal} as the default value (if conf != null).
   *   </li>
   *   <li>Fetch the system property.</li>
   *   <li>If the system property is not empty or "(unset)":
   *   it overrides the conf value.
   *   </li>
   * </ol>
   * This puts the build properties in charge of everything. It's not a
   * perfect design; having maven set properties based on a file, as ant let
   * you do, is better for customization.
   *
   * As to why there's a special (unset) value, see
   * {@link <a href="http://stackoverflow.com/questions/7773134/null-versus-empty-arguments-in-maven">...</a>}
   * @param conf config: may be null
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static long getTestPropertyLong(Configuration conf,
      String key, long defVal) {
    return Long.valueOf(
        getTestProperty(conf, key, Long.toString(defVal)));
  }

  /**
   * Get an integer test property; algorithm described in
   * {@link #getTestPropertyLong(Configuration, String, long)}.
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static int getTestPropertyInt(Configuration conf,
      String key, int defVal) {
    return (int) getTestPropertyLong(conf, key, defVal);
  }

  /**
   * Get a boolean test property; algorithm described in
   * {@link #getTestPropertyLong(Configuration, String, long)}.
   * @param key key to look up
   * @param defVal default value
   * @return the evaluated test property.
   */
  public static boolean getTestPropertyBool(Configuration conf,
      String key,
      boolean defVal) {
    return Boolean.valueOf(
        getTestProperty(conf, key, Boolean.toString(defVal)));
  }

  /**
   * Assume that the scale tests are enabled by the relevant system property.
   */
  public static void assumeScaleTestsEnabled(Configuration conf) {
    boolean enabled = getTestPropertyBool(
        conf,
        FS_AZURE_SCALE_TEST_ENABLED,
        DEFAULT_SCALE_TESTS_ENABLED);
    assume("Scale test disabled: to enable set property "
            + FS_AZURE_SCALE_TEST_ENABLED,
        enabled);
  }
}
