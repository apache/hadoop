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
import org.apache.hadoop.fs.adl.AdlFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class to configure real Adls storage to run Live test suite against.
 */
public final class AdlStorageConfiguration {
  private AdlStorageConfiguration() {}

  private static final String CONTRACT_ENABLE_KEY =
      "dfs.adl.test.contract" + ".enable";
  private static final String TEST_CONFIGURATION_FILE_NAME =
      "contract-test-options.xml";
  private static final String TEST_SUPPORTED_TEST_CONFIGURATION_FILE_NAME =
      "adls.xml";

  private static boolean isContractTestEnabled = false;
  private static Configuration conf = null;

  public static Configuration getConfiguration() {
    Configuration localConf = new Configuration();
    localConf.addResource(TEST_CONFIGURATION_FILE_NAME);
    localConf.addResource(TEST_SUPPORTED_TEST_CONFIGURATION_FILE_NAME);
    return localConf;
  }

  public static boolean isContractTestEnabled() {
    if (conf == null) {
      conf = getConfiguration();
    }

    isContractTestEnabled = conf.getBoolean(CONTRACT_ENABLE_KEY, false);
    return isContractTestEnabled;
  }

  public static FileSystem createAdlStorageConnector()
      throws URISyntaxException, IOException {
    if (conf == null) {
      conf = getConfiguration();
    }

    if(!isContractTestEnabled()) {
      return null;
    }

    AdlFileSystem fileSystem = new AdlFileSystem();
    fileSystem.initialize(new URI(conf.get("fs.defaultFS")), conf);
    return fileSystem;
  }
}
