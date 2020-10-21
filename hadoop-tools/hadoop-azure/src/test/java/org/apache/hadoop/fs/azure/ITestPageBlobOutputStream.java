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
package org.apache.hadoop.fs.azure;

import java.io.IOException;
import java.util.EnumSet;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.LambdaTestUtils;

public class ITestPageBlobOutputStream extends AbstractWasbTestBase {

  private static final Path TEST_FILE_PATH = new Path(
      "TestPageBlobOutputStream.txt");

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();
    // Configure the page blob directories key so every file created is a page
    // blob.
    conf.set(AzureNativeFileSystemStore.KEY_PAGE_BLOB_DIRECTORIES, "/");
    return AzureBlobStorageTestAccount.create("testpagebloboutputstream",
        EnumSet.of(AzureBlobStorageTestAccount.CreateOptions.CreateContainer),
        conf, true);
  }

  @Test
  public void testHflush() throws Exception {
    Path path = fs.makeQualified(TEST_FILE_PATH);
    FSDataOutputStream os = fs.create(path);
    os.write(1);
    os.hflush();
    // Delete the blob so that Azure call will fail.
    fs.delete(path, false);
    os.write(2);
    LambdaTestUtils.intercept(IOException.class,
        "The specified blob does not exist", () -> {
          os.hflush();
        });
    LambdaTestUtils.intercept(IOException.class,
        "The specified blob does not exist", () -> {
          os.close();
        });
  }

  @Test
  public void testHsync() throws Exception {
    Path path = fs.makeQualified(TEST_FILE_PATH);
    FSDataOutputStream os = fs.create(path);
    os.write(1);
    os.hsync();
    // Delete the blob so that Azure call will fail.
    fs.delete(path, false);
    os.write(2);
    LambdaTestUtils.intercept(IOException.class,
        "The specified blob does not exist", () -> {
          os.hsync();
        });
    LambdaTestUtils.intercept(IOException.class,
        "The specified blob does not exist", () -> {
          os.close();
        });
  }
}