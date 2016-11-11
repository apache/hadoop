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
 *
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configure Adl storage file system.
 */
public final class AdlStorageConfiguration {
  private static final String CONTRACT_ENABLE_KEY =
      "dfs.adl.test.contract.enable";

  private static final String TEST_CONFIGURATION_FILE_NAME =
      "contract-test-options.xml";
  private static final String TEST_SUPPORTED_TEST_CONFIGURATION_FILE_NAME =
      "adls.xml";
  private static final String KEY_FILE_SYSTEM_IMPL = "fs.contract.test.fs";
  private static final String KEY_FILE_SYSTEM = "test.fs.adl.name";

  private static boolean isContractTestEnabled = false;
  private static Configuration conf = null;

  private AdlStorageConfiguration() {
  }

  public synchronized static Configuration getConfiguration() {
    Configuration newConf = new Configuration();
    newConf.addResource(TEST_CONFIGURATION_FILE_NAME);
    newConf.addResource(TEST_SUPPORTED_TEST_CONFIGURATION_FILE_NAME);
    return newConf;
  }

  public synchronized static boolean isContractTestEnabled() {
    if (conf == null) {
      conf = getConfiguration();
    }

    isContractTestEnabled = conf.getBoolean(CONTRACT_ENABLE_KEY, false);
    return isContractTestEnabled;
  }

  public synchronized static FileSystem createStorageConnector()
      throws URISyntaxException, IOException {
    if (conf == null) {
      conf = getConfiguration();
    }

    if (!isContractTestEnabled()) {
      return null;
    }

    String fileSystem = conf.get(KEY_FILE_SYSTEM);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new IOException("Default file system not configured.");
    }
    String fileSystemImpl = conf.get(KEY_FILE_SYSTEM_IMPL);
    if (fileSystemImpl == null || fileSystemImpl.trim().length() == 0) {
      throw new IOException(
          "Configuration " + KEY_FILE_SYSTEM_IMPL + "does not exist.");
    }
    FileSystem fs = null;
    try {
      fs = (FileSystem) Class.forName(fileSystemImpl).newInstance();
    } catch (Exception e) {
      throw new IOException("Could not instantiate the filesystem.");
    }

    fs.initialize(new URI(conf.get(KEY_FILE_SYSTEM)), conf);
    return fs;
  }
}
