/**
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

package org.apache.hadoop.fs.azurebfs;

import java.util.Hashtable;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.services.AbfsInputStream;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.ONE_MB;

/**
 * Test FileSystemProperties.
 */
public class ITestFileSystemProperties extends AbstractAbfsIntegrationTest {
  private static final int TEST_DATA = 100;
  private static final String TEST_PATH = "/testfile";
  public ITestFileSystemProperties() throws Exception {
  }

  @Test
  public void testReadWriteBytesToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_PATH);
    try(FSDataOutputStream stream = fs.create(testPath)) {
      stream.write(TEST_DATA);
    }

    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertEquals(1, fileStatus.getLen());

    try(FSDataInputStream inputStream = fs.open(testPath, 4 * 1024 * 1024)) {
      int i = inputStream.read();
      assertEquals(TEST_DATA, i);
    }
  }

  @Test
  public void testWriteOneByteToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = path(TEST_PATH);
    try(FSDataOutputStream stream = fs.create(testPath)) {
      stream.write(TEST_DATA);
    }

    FileStatus fileStatus = fs.getFileStatus(testPath);
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void testBase64FileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value }");
    TracingContext tracingContext = getTestTracingContext(fs, true);
    fs.getAbfsStore().setFilesystemProperties(properties, tracingContext);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore()
        .getFilesystemProperties(tracingContext);

    assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testBase64PathProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest }");
    Path testPath = path(TEST_PATH);
    touch(testPath);
    TracingContext tracingContext = getTestTracingContext(fs, true);
    fs.getAbfsStore().setPathProperties(testPath, properties, tracingContext);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore()
        .getPathStatus(testPath, tracingContext);

    assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value歲 }");
    TracingContext tracingContext = getTestTracingContext(fs, true);
    fs.getAbfsStore().setFilesystemProperties(properties, tracingContext);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore()
        .getFilesystemProperties(tracingContext);

    assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidPathProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest兩 }");
    Path testPath = path(TEST_PATH);
    touch(testPath);
    TracingContext tracingContext = getTestTracingContext(fs, true);
    fs.getAbfsStore().setPathProperties(testPath, properties, tracingContext);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore()
        .getPathStatus(testPath, tracingContext);

    assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testSetFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("containerForDevTest", "true");
    TracingContext tracingContext = getTestTracingContext(fs, true);
    fs.getAbfsStore().setFilesystemProperties(properties, tracingContext);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore()
        .getFilesystemProperties(tracingContext);

    assertEquals(properties, fetchedProperties);
  }

  @Test
  //Test to verify buffersize remains the same as set in the configuration, irrespective of the parameter passed to FSDataInputStream
  public void testBufferSizeSet() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AbfsConfiguration abfsConfig = fs.getAbfsStore().getAbfsConfiguration();
    int bufferSizeConfig = 6 * ONE_MB;
    int bufferSizeArg = 10 * ONE_MB;

    Path testPath = path(TEST_PATH);
    fs.create(testPath);

    abfsConfig.setReadBufferSize(bufferSizeConfig);
    FSDataInputStream inputStream = fs.open(testPath, bufferSizeArg);
    AbfsInputStream abfsInputStream
        = (AbfsInputStream) inputStream.getWrappedStream();
    int actualBufferSize = abfsInputStream.getBufferSize();

    Assertions.assertThat(actualBufferSize)
        .describedAs("Buffer size should be set to the value in the configuration")
        .isEqualTo(bufferSizeConfig);
    Assertions.assertThat(actualBufferSize)
        .describedAs("Buffer size should not be set to the value passed as argument")
        .isNotEqualTo(bufferSizeArg);
  }
}
