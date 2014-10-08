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

package org.apache.hadoop.fs.azure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the scenario where a colon is included in the file/directory name.
 * 
 * NativeAzureFileSystem#create(), #mkdir(), and #rename() disallow the
 * creation/rename of files/directories through WASB that have colons in the
 * names.
 */
public class TestNativeAzureFileSystemFileNameCheck {
  private FileSystem fs = null;
  private AzureBlobStorageTestAccount testAccount = null;
  private String root = null;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
    fs = testAccount.getFileSystem();
    root = fs.getUri().toString();
  }

  @After
  public void tearDown() throws Exception {
    testAccount.cleanup();
    root = null;
    fs = null;
    testAccount = null;
  }

  @Test
  public void testCreate() throws Exception {
    // positive test
    Path testFile1 = new Path(root + "/testFile1");
    assertTrue(fs.createNewFile(testFile1));

    // negative test
    Path testFile2 = new Path(root + "/testFile2:2");
    try {
      fs.createNewFile(testFile2);
      fail("Should've thrown.");
    } catch (IOException e) { // ignore
    }
  }

  @Test
  public void testRename() throws Exception {
    // positive test
    Path testFile1 = new Path(root + "/testFile1");
    assertTrue(fs.createNewFile(testFile1));
    Path testFile2 = new Path(root + "/testFile2");
    fs.rename(testFile1, testFile2);
    assertTrue(!fs.exists(testFile1) && fs.exists(testFile2));

    // negative test
    Path testFile3 = new Path(root + "/testFile3:3");
    try {
      fs.rename(testFile2, testFile3);
      fail("Should've thrown.");
    } catch (IOException e) { // ignore
    }
    assertTrue(fs.exists(testFile2));
  }

  @Test
  public void testMkdirs() throws Exception {
    // positive test
    Path testFolder1 = new Path(root + "/testFolder1");
    assertTrue(fs.mkdirs(testFolder1));

    // negative test
    Path testFolder2 = new Path(root + "/testFolder2:2");
    try {
      assertTrue(fs.mkdirs(testFolder2));
      fail("Should've thrown.");
    } catch (IOException e) { // ignore
    }
  }

  @Test
  public void testWasbFsck() throws Exception {
    // positive test
    Path testFolder1 = new Path(root + "/testFolder1");
    assertTrue(fs.mkdirs(testFolder1));
    Path testFolder2 = new Path(testFolder1, "testFolder2");
    assertTrue(fs.mkdirs(testFolder2));
    Path testFolder3 = new Path(testFolder1, "testFolder3");
    assertTrue(fs.mkdirs(testFolder3));
    Path testFile1 = new Path(testFolder2, "testFile1");
    assertTrue(fs.createNewFile(testFile1));
    Path testFile2 = new Path(testFolder1, "testFile2");
    assertTrue(fs.createNewFile(testFile2));
    assertFalse(runWasbFsck(testFolder1));

    // negative test
    InMemoryBlockBlobStore backingStore
        = testAccount.getMockStorage().getBackingStore();
    backingStore.setContent(
        AzureBlobStorageTestAccount.toMockUri("testFolder1/testFolder2/test2:2"),
        new byte[] { 1, 2 },
        new HashMap<String, String>(), false, 0);
    assertTrue(runWasbFsck(testFolder1));
  }

  private boolean runWasbFsck(Path p) throws Exception {
    WasbFsck fsck = new WasbFsck(fs.getConf());
    fsck.setMockFileSystemForTesting(fs);
    fsck.run(new String[] { p.toString() });
    return fsck.getPathNameWarning();
  }
}