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
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Configure Adl storage file system.
 */
public final class AdlStorageConfiguration {
  static final String CONTRACT_XML = "adls.xml";

  private static final String CONTRACT_ENABLE_KEY =
      "fs.adl.test.contract.enable";
  private static final boolean CONTRACT_ENABLE_DEFAULT = false;

  private static final String FILE_SYSTEM_KEY =
      String.format("test.fs.%s.name", AdlFileSystem.SCHEME);

  private static final String FILE_SYSTEM_IMPL_KEY =
      String.format("fs.%s.impl", AdlFileSystem.SCHEME);
  private static final Class<?> FILE_SYSTEM_IMPL_DEFAULT =
      AdlFileSystem.class;

  private static boolean isContractTestEnabled = false;
  private static Configuration conf = null;

  static {
    Configuration.addDeprecation("dfs.adl.test.contract.enable",
        CONTRACT_ENABLE_KEY);
    Configuration.reloadExistingConfigurations();
  }

  private AdlStorageConfiguration() {
  }

  public synchronized static Configuration getConfiguration() {
    Configuration newConf = new Configuration();
    newConf.addResource(CONTRACT_XML);
    return newConf;
  }

  public synchronized static boolean isContractTestEnabled() {
    if (conf == null) {
      conf = getConfiguration();
    }

    isContractTestEnabled = conf.getBoolean(CONTRACT_ENABLE_KEY,
        CONTRACT_ENABLE_DEFAULT);
    return isContractTestEnabled;
  }

  public synchronized static FileSystem createStorageConnector()
      throws URISyntaxException, IOException {
    if (conf == null) {
      conf = getConfiguration();
    }
    return createStorageConnector(conf);
  }

  public synchronized static FileSystem createStorageConnector(
      Configuration fsConfig) throws URISyntaxException, IOException {
    if (!isContractTestEnabled()) {
      return null;
    }

    String fileSystem = fsConfig.get(FILE_SYSTEM_KEY);
    if (fileSystem == null || fileSystem.trim().length() == 0) {
      throw new IOException("Default file system not configured.");
    }

    Class<?> clazz = fsConfig.getClass(FILE_SYSTEM_IMPL_KEY,
        FILE_SYSTEM_IMPL_DEFAULT);
    FileSystem fs = (FileSystem) ReflectionUtils.newInstance(clazz, fsConfig);
    fs.initialize(new URI(fileSystem), fsConfig);
    return fs;
  }
}
