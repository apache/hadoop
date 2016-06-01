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

/**
 * Constants for S3A Testing.
 */
public interface S3ATestConstants {

  /**
   * Prefix for any cross-filesystem scale test options.
   */
  String SCALE_TEST = "scale.test.";

  /**
   * Prefix for s3a-specific scale tests.
   */
  String S3A_SCALE_TEST = "fs.s3a.scale.test.";

  /**
   * Prefix for FS s3a tests.
   */
  String TEST_FS_S3A = "test.fs.s3a.";

  /**
   * Name of the test filesystem.
   */
  String TEST_FS_S3A_NAME = TEST_FS_S3A + "name";

  /**
   * The number of operations to perform: {@value}.
   */
  String KEY_OPERATION_COUNT = SCALE_TEST + "operation.count";

  /**
   * The readahead buffer: {@value}.
   */
  String KEY_READ_BUFFER_SIZE = S3A_SCALE_TEST + "read.buffer.size";
  int DEFAULT_READ_BUFFER_SIZE = 16384;

  /**
   * Key for a multi MB test file: {@value}.
   */
  String KEY_CSVTEST_FILE = S3A_SCALE_TEST + "csvfile";

  /**
   * Default path for the multi MB test file: {@value}.
   */
  String DEFAULT_CSVTEST_FILE = "s3a://landsat-pds/scene_list.gz";

  /**
   * The default number of operations to perform: {@value}.
   */
  long DEFAULT_OPERATION_COUNT = 2005;

  /**
   * Run the encryption tests?
   */
  String KEY_ENCRYPTION_TESTS = TEST_FS_S3A + "encryption.enabled";
}
