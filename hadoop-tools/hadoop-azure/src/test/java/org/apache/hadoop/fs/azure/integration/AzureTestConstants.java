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

package org.apache.hadoop.fs.azure.integration;

import org.apache.hadoop.fs.Path;

/**
 * Constants for the Azure tests.
 */
public interface AzureTestConstants {

  /**
   * Prefix for any cross-filesystem scale test options.
   */
  String SCALE_TEST = "scale.test.";

  /**
   * Prefix for wasb-specific scale tests.
   */
  String AZURE_SCALE_TEST = "fs.azure.scale.test.";

  /**
   * Prefix for FS wasb tests.
   */
  String TEST_FS_WASB = "test.fs.azure.";

  /**
   * Name of the test filesystem.
   */
  String TEST_FS_WASB_NAME = TEST_FS_WASB + "name";

  /**
   * Tell tests that they are being executed in parallel: {@value}.
   */
  String KEY_PARALLEL_TEST_EXECUTION = "test.parallel.execution";

  /**
   * A property set to true in maven if scale tests are enabled: {@value}.
   */
  String KEY_SCALE_TESTS_ENABLED = AZURE_SCALE_TEST + "enabled";

  /**
   * The number of operations to perform: {@value}.
   */
  String KEY_OPERATION_COUNT = SCALE_TEST + "operation.count";

  /**
   * The number of directory operations to perform: {@value}.
   */
  String KEY_DIRECTORY_COUNT = SCALE_TEST + "directory.count";

  /**
   * The readahead buffer: {@value}.
   */
  String KEY_READ_BUFFER_SIZE = AZURE_SCALE_TEST + "read.buffer.size";

  int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Key for a multi MB test file: {@value}.
   */
  String KEY_CSVTEST_FILE = AZURE_SCALE_TEST + "csvfile";

  /**
   * Default path for the multi MB test file: {@value}.
   */
  String DEFAULT_CSVTEST_FILE = "wasb://datasets@azuremlsampleexperiments.blob.core.windows.net/network_intrusion_detection.csv";

  /**
   * Name of the property to define the timeout for scale tests: {@value}.
   * Measured in seconds.
   */
  String KEY_TEST_TIMEOUT = AZURE_SCALE_TEST + "timeout";

  /**
   * Name of the property to define the file size for the huge file
   * tests: {@value}.
   * Measured in KB; a suffix like "M", or "G" will change the unit.
   */
  String KEY_HUGE_FILESIZE = AZURE_SCALE_TEST + "huge.filesize";

  /**
   * Name of the property to define the partition size for the huge file
   * tests: {@value}.
   * Measured in KB; a suffix like "M", or "G" will change the unit.
   */
  String KEY_HUGE_PARTITION_SIZE = AZURE_SCALE_TEST + "huge.partitionsize";

  /**
   * The default huge size is small —full 5GB+ scale tests are something
   * to run in long test runs on EC2 VMs. {@value}.
   */
  String DEFAULT_HUGE_FILESIZE = "10M";

  /**
   * The default number of operations to perform: {@value}.
   */
  long DEFAULT_OPERATION_COUNT = 2005;

  /**
   * Default number of directories to create when performing
   * directory performance/scale tests.
   */
  int DEFAULT_DIRECTORY_COUNT = 2;

  /**
   * Default policy on scale tests: {@value}.
   */
  boolean DEFAULT_SCALE_TESTS_ENABLED = false;

  /**
   * Fork ID passed down from maven if the test is running in parallel.
   */
  String TEST_UNIQUE_FORK_ID = "test.unique.fork.id";

  /**
   * Timeout in Milliseconds for standard tests: {@value}.
   */
  int AZURE_TEST_TIMEOUT = 10 * 60 * 1000;

  /**
   * Timeout in Seconds for Scale Tests: {@value}.
   */
  int SCALE_TEST_TIMEOUT_SECONDS = 30 * 60;

  int SCALE_TEST_TIMEOUT_MILLIS = SCALE_TEST_TIMEOUT_SECONDS * 1000;



  String ACCOUNT_KEY_PROPERTY_NAME
      = "fs.azure.account.key.";
  String SAS_PROPERTY_NAME = "fs.azure.sas.";
  String TEST_CONFIGURATION_FILE_NAME = "azure-test.xml";
  String TEST_ACCOUNT_NAME_PROPERTY_NAME
      = "fs.azure.test.account.name";
  String MOCK_ACCOUNT_NAME
      = "mockAccount.blob.core.windows.net";
  String MOCK_CONTAINER_NAME = "mockContainer";
  String WASB_AUTHORITY_DELIMITER = "@";
  String WASB_SCHEME = "wasb";
  String PATH_DELIMITER = "/";
  String AZURE_ROOT_CONTAINER = "$root";
  String MOCK_WASB_URI = "wasb://" + MOCK_CONTAINER_NAME
      + WASB_AUTHORITY_DELIMITER + MOCK_ACCOUNT_NAME + "/";
  String USE_EMULATOR_PROPERTY_NAME
      = "fs.azure.test.emulator";

  String KEY_DISABLE_THROTTLING
      = "fs.azure.disable.bandwidth.throttling";
  String KEY_READ_TOLERATE_CONCURRENT_APPEND
      = "fs.azure.io.read.tolerate.concurrent.append";
  /**
   * Path for page blobs: {@value}.
   */
  String DEFAULT_PAGE_BLOB_DIRECTORY = "pageBlobs";

  String DEFAULT_ATOMIC_RENAME_DIRECTORIES
      = "/atomicRenameDir1,/atomicRenameDir2";

  /**
   * Base directory for page blobs.
   */
  Path PAGE_BLOB_DIR = new Path("/" + DEFAULT_PAGE_BLOB_DIRECTORY);
}
