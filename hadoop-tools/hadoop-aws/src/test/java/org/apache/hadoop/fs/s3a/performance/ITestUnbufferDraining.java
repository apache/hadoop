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

package org.apache.hadoop.fs.s3a.performance;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.io.IOUtils;

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.s3a.Constants.ASYNC_DRAIN_THRESHOLD;
import static org.apache.hadoop.fs.s3a.Constants.ESTABLISH_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.INPUT_FADVISE;
import static org.apache.hadoop.fs.s3a.Constants.MAXIMUM_CONNECTIONS;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.PREFETCH_ENABLED_KEY;
import static org.apache.hadoop.fs.s3a.Constants.READAHEAD_RANGE;
import static org.apache.hadoop.fs.s3a.Constants.REQUEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.SOCKET_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;

/**
 * Test stream unbuffer performance/behavior with stream draining
 * and aborting.
 */
public class ITestUnbufferDraining extends AbstractS3ACostTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestUnbufferDraining.class);

  public static final int READAHEAD = 1000;

  public static final int FILE_SIZE = 50_000;

  public static final int ATTEMPTS = 10;

  private FileSystem brittleFS;

  /**
   * Create with markers kept, always.
   */
  public ITestUnbufferDraining() {
    super(false);
  }

  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    removeBaseAndBucketOverrides(conf,
        ASYNC_DRAIN_THRESHOLD,
        ESTABLISH_TIMEOUT,
        INPUT_FADVISE,
        MAX_ERROR_RETRIES,
        MAXIMUM_CONNECTIONS,
        PREFETCH_ENABLED_KEY,
        READAHEAD_RANGE,
        REQUEST_TIMEOUT,
        RETRY_LIMIT,
        SOCKET_TIMEOUT);

    return conf;
  }

  @Override
  public void setup() throws Exception {
    super.setup();

    // now create a new FS with minimal http capacity and recovery
    // a separate one is used to avoid test teardown suffering
    // from the lack of http connections and short timeouts.
    Configuration conf = getConfiguration();
    // kick off async drain for any data
    conf.setInt(ASYNC_DRAIN_THRESHOLD, 1);
    conf.setInt(MAXIMUM_CONNECTIONS, 2);
    conf.setInt(MAX_ERROR_RETRIES, 1);
    conf.setInt(ESTABLISH_TIMEOUT, 1000);
    conf.setInt(READAHEAD_RANGE, READAHEAD);
    conf.setInt(RETRY_LIMIT, 1);

    brittleFS = FileSystem.newInstance(getFileSystem().getUri(), conf);
  }

  @Override
  public void teardown() throws Exception {
    super.teardown();
    IOUtils.cleanupWithLogger(LOG, brittleFS);
  }

  public FileSystem getBrittleFS() {
    return brittleFS;
  }

  /**
   * Test stream close performance/behavior with stream draining
   * and unbuffer.
   */
  @Test
  public void testUnbufferDraining() throws Throwable {

    describe("unbuffer draining");
    FileStatus st = createTestFile();

    int offset = FILE_SIZE - READAHEAD + 1;
    try (FSDataInputStream in = getBrittleFS().openFile(st.getPath())
        .withFileStatus(st)
        .must(ASYNC_DRAIN_THRESHOLD, 1)
        .build().get()) {
      describe("Initiating unbuffer with async drain\n");
      for (int i = 0; i < ATTEMPTS; i++) {
        describe("Starting read/unbuffer #%d", i);
        in.seek(offset);
        in.read();
        in.unbuffer();
      }
    }
  }

  /**
   * Test stream close performance/behavior with stream draining
   * and unbuffer.
   */
  @Test
  public void testUnbufferAborting() throws Throwable {

    describe("unbuffer draining");
    FileStatus st = createTestFile();


    // open the file at the beginning with a whole file read policy,
    // so even with s3a switching to random on unbuffer,
    // this always does a full GET
    try (FSDataInputStream in = getBrittleFS().openFile(st.getPath())
        .withFileStatus(st)
        .must(ASYNC_DRAIN_THRESHOLD, 1)
        .must(FS_OPTION_OPENFILE_READ_POLICY,
            FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
        .build().get()) {

      describe("Initiating unbuffer with async drain\n");
      for (int i = 0; i < ATTEMPTS; i++) {
        describe("Starting read/unbuffer #%d", i);
        in.read();
        in.unbuffer();
      }
    }
  }

  private FileStatus createTestFile() throws IOException {
    byte[] data = dataset(FILE_SIZE, '0', 10);
    S3AFileSystem fs = getFileSystem();

    Path path = methodPath();
    ContractTestUtils.createFile(fs, path, true, data);
    FileStatus st = fs.getFileStatus(path);
    return st;
  }


}
