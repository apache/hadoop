/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.fs.test.formats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.io.wrappedio.impl.DynamicWrappedIO;
import org.apache.iceberg.hadoop.HadoopFileIO;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertSuccessfulBulkDelete;
import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;
import static org.apache.hadoop.fs.contract.ContractTestUtils.touch;
import static org.apache.hadoop.io.wrappedio.WrappedIO.bulkDelete_delete;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Contract tests for iceberg bulk delete operation,
 * verifyying
 */
public abstract class AbstractIcebergDeleteTest extends AbstractFSContractTestBase {

  private static final Logger LOG =
          LoggerFactory.getLogger(AbstractIcebergDeleteTest.class);

  private static final String DELETE_FILE_PARALLELISM = "iceberg.hadoop.delete-file-parallelism";

  /** Is bulk delete enabled on hadoop runtimes with API support: {@value}. */
  public static final String ICEBERG_BULK_DELETE_ENABLED = "iceberg.hadoop.bulk.delete.enabled";

  /**
   * Page size for bulk delete. This is calculated based
   * on the store implementation.
   */
  protected int pageSize;

  /**
   * Base path for the bulk delete tests.
   * All the paths to be deleted should be under this base path.
   */
  protected Path basePath;

  /**
   * Reflection support.
   */
  private DynamicWrappedIO dynamicWrappedIO;

  /**
   * Create a configuration with the iceberg settings
   * added.
   * @return a configuration for subclasses to extend
   */

  @Override
  protected Configuration createConfiguration() {
    final Configuration conf = super.createConfiguration();
    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    FileSystem fs = getFileSystem();
    basePath = path(getClass().getName());
    dynamicWrappedIO = new DynamicWrappedIO();
    pageSize = dynamicWrappedIO.bulkDelete_pageSize(fs, basePath);
    fs.mkdirs(basePath);
  }

  public Path getBasePath() {
    return basePath;
  }

  protected int getExpectedPageSize() {
    return 1;
  }

}
