/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl.live;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.adl.AdlFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * This class is responsible for testing ContentSummary, ListStatus on
 * file/folder.
 */
public class TestMetadata {

  private FileSystem adlStore;
  private Path parent;

  public TestMetadata() {
    parent = new Path("test");
  }

  @Before
  public void setUp() throws Exception {
    Assume.assumeTrue(AdlStorageConfiguration.isContractTestEnabled());
    adlStore = AdlStorageConfiguration.createStorageConnector();
  }

  @After
  public void cleanUp() throws Exception {
    if (AdlStorageConfiguration.isContractTestEnabled()) {
      adlStore.delete(parent, true);
    }
  }

  @Test
  public void testContentSummaryOnFile() throws IOException {
    Path child = new Path(UUID.randomUUID().toString());
    Path testFile = new Path(parent, child);
    OutputStream out = adlStore.create(testFile);

    for (int i = 0; i < 1024; ++i) {
      out.write(97);
    }
    out.close();

    Assert.assertTrue(adlStore.isFile(testFile));
    ContentSummary summary = adlStore.getContentSummary(testFile);
    Assert.assertEquals(1024, summary.getSpaceConsumed());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(0, summary.getDirectoryCount());
    Assert.assertEquals(1024, summary.getLength());
  }

  @Test
  public void testContentSummaryOnFolder() throws IOException {
    Path child = new Path(UUID.randomUUID().toString());
    Path testFile = new Path(parent, child);
    OutputStream out = adlStore.create(testFile);

    for (int i = 0; i < 1024; ++i) {
      out.write(97);
    }
    out.close();

    Assert.assertTrue(adlStore.isFile(testFile));
    ContentSummary summary = adlStore.getContentSummary(parent);
    Assert.assertEquals(1024, summary.getSpaceConsumed());
    Assert.assertEquals(1, summary.getFileCount());
    Assert.assertEquals(1, summary.getDirectoryCount());
    Assert.assertEquals(1024, summary.getLength());
  }

  @Test
  public void listStatusOnFile() throws IOException {
    Path path = new Path(parent, "a.txt");
    FileSystem fs = adlStore;
    fs.createNewFile(path);
    Assert.assertTrue(fs.isFile(path));
    FileStatus[] statuses = fs.listStatus(path);
    Assert
        .assertEquals(path.makeQualified(fs.getUri(), fs.getWorkingDirectory()),
            statuses[0].getPath());
  }

  @Test
  public void testUserRepresentationConfiguration() throws IOException {
    // Validating actual user/group OID or friendly name is outside scope of
    // this test.
    Path path = new Path(parent, "a.txt");
    AdlFileSystem fs = (AdlFileSystem) adlStore;

    // When set to true, User/Group information should be user friendly name.
    // That is non GUID value.
    fs.setUserGroupRepresentationAsUPN(false);
    fs.createNewFile(path);
    Assert.assertTrue(fs.isFile(path));
    FileStatus fileStatus = fs.getFileStatus(path);
    UUID.fromString(fileStatus.getGroup());
    UUID.fromString(fileStatus.getOwner());

    // When set to false, User/Group information should be AAD represented
    // unique OID. That is GUID value.
    // Majority of the cases, user friendly name would not be GUID value.
    fs.setUserGroupRepresentationAsUPN(true);
    fileStatus = fs.getFileStatus(path);
    try {
      UUID.fromString(fileStatus.getGroup());
      UUID.fromString(fileStatus.getOwner());
      fail("Expected user friendly name to be non guid value.");
    } catch (IllegalArgumentException e) {
      // expected to fail since
    }
  }
}

