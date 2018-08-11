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

import org.junit.Test;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Test FileSystemProperties.
 */
public class ITestFileSystemProperties extends AbstractAbfsIntegrationTest {
  private static final int TEST_DATA = 100;
  private static final Path TEST_PATH = new Path("/testfile");
  public ITestFileSystemProperties() {
  }

  @Test
  public void testReadWriteBytesToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    testWriteOneByteToFileAndEnsureThreadPoolCleanup();

    try(FSDataInputStream inputStream = fs.open(TEST_PATH, 4 * 1024 * 1024)) {
      int i = inputStream.read();
      assertEquals(TEST_DATA, i);
    }
  }

  @Test
  public void testWriteOneByteToFileAndEnsureThreadPoolCleanup() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    try(FSDataOutputStream stream = fs.create(TEST_PATH)) {
      stream.write(TEST_DATA);
    }

    FileStatus fileStatus = fs.getFileStatus(TEST_PATH);
    assertEquals(1, fileStatus.getLen());
  }

  @Test
  public void testBase64FileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value }");
    fs.getAbfsStore().setFilesystemProperties(properties);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore().getFilesystemProperties();

    assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testBase64PathProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest }");
    touch(TEST_PATH);
    fs.getAbfsStore().setPathProperties(TEST_PATH, properties);
    Hashtable<String, String> fetchedProperties =
            fs.getAbfsStore().getPathProperties(TEST_PATH);

    assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: value歲 }");
    fs.getAbfsStore().setFilesystemProperties(properties);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore().getFilesystemProperties();

    assertEquals(properties, fetchedProperties);
  }

  @Test (expected = Exception.class)
  public void testBase64InvalidPathProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("key", "{ value: valueTest兩 }");
    touch(TEST_PATH);
    fs.getAbfsStore().setPathProperties(TEST_PATH, properties);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore().getPathProperties(TEST_PATH);

    assertEquals(properties, fetchedProperties);
  }

  @Test
  public void testSetFileSystemProperties() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Hashtable<String, String> properties = new Hashtable<>();
    properties.put("containerForDevTest", "true");
    fs.getAbfsStore().setFilesystemProperties(properties);
    Hashtable<String, String> fetchedProperties = fs.getAbfsStore().getFilesystemProperties();

    assertEquals(properties, fetchedProperties);
  }
}
