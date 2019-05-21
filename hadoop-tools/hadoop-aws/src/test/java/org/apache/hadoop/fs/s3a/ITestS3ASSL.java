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

import java.io.IOException;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.security.ssl.OpenSSLSocketFactory;
import org.apache.hadoop.util.NativeCodeLoader;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.junit.Assume.assumeTrue;

/**
 * Tests non-default values for {@link Constants#SSL_CHANNEL_MODE}.
 */
public class ITestS3ASSL extends AbstractS3ATestBase {

  @Test
  public void testOpenSSL() throws IOException {
    assumeTrue("Unable to load native libraries",
            NativeCodeLoader.isNativeCodeLoaded());
    assumeTrue("Build was not compiled with support for OpenSSL",
            NativeCodeLoader.buildSupportsOpenssl());
    Configuration conf = new Configuration(getConfiguration());
    conf.setEnum(Constants.SSL_CHANNEL_MODE,
            OpenSSLSocketFactory.SSLChannelMode.OpenSSL);
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      writeThenReadFile(fs, path("ITestS3ASSL/testOpenSSL"));
    }
  }

  @Test
  public void testJSEE() throws IOException {
    Configuration conf = new Configuration(getConfiguration());
    conf.setEnum(Constants.SSL_CHANNEL_MODE,
            OpenSSLSocketFactory.SSLChannelMode.Default_JSSE);
    try (S3AFileSystem fs = S3ATestUtils.createTestFileSystem(conf)) {
      writeThenReadFile(fs, path("ITestS3ASSL/testJSEE"));
    }
  }

  /**
   * Helper function that writes and then reads a file. Unlike
   * {@link #writeThenReadFile(Path, int)} it takes a {@link FileSystem} as a
   * parameter.
   */
  private void writeThenReadFile(FileSystem fs, Path path) throws IOException {
    byte[] data = dataset(1024, 'a', 'z');
    writeDataset(fs, path, data, data.length, 1024, true);
    ContractTestUtils.verifyFileContents(fs, path, data);
  }
}
