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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests which look at fsck recovery.
 */
public class TestWasbFsck extends AbstractWasbTestWithTimeout {
  private AzureBlobStorageTestAccount testAccount;
  private FileSystem fs;
  private InMemoryBlockBlobStore backingStore;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
    fs = testAccount.getFileSystem();
    backingStore = testAccount.getMockStorage().getBackingStore();
  }

  @After
  public void tearDown() throws Exception {
    testAccount.cleanup();
    fs = null;
    backingStore = null;
  }

  /**
   * Counts the number of temporary blobs in the backing store.
   */
  private int getNumTempBlobs() {
    int count = 0;
    for (String key : backingStore.getKeys()) {
      if (key.contains(NativeAzureFileSystem.AZURE_TEMP_FOLDER)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Tests that we recover files properly
   */
  @Test
  @Ignore  /* flush() no longer does anything  @@TODO: reinstate an appropriate test of fsck recovery*/
  public void testRecover() throws Exception {
    Path danglingFile = new Path("/crashedInTheMiddle");

    // Create a file and leave it dangling and try to recover it.
    FSDataOutputStream stream = fs.create(danglingFile);
    stream.write(new byte[] { 1, 2, 3 });
    stream.flush();

    // Now we should still only see a zero-byte file in this place
    FileStatus fileStatus = fs.getFileStatus(danglingFile);
    assertNotNull(fileStatus);
    assertEquals(0, fileStatus.getLen());
    assertEquals(1, getNumTempBlobs());

    // Run WasbFsck -move to recover the file.
    runFsck("-move");

    // Now we should the see the file in lost+found with the data there.
    fileStatus = fs.getFileStatus(new Path("/lost+found",
        danglingFile.getName()));
    assertNotNull(fileStatus);
    assertEquals(3, fileStatus.getLen());
    assertEquals(0, getNumTempBlobs());
    // But not in its original location
    assertFalse(fs.exists(danglingFile));
  }

  private void runFsck(String command) throws Exception {
    Configuration conf = fs.getConf();
    // Set the dangling cutoff to zero, so every temp blob is considered
    // dangling.
    conf.setInt(NativeAzureFileSystem.AZURE_TEMP_EXPIRY_PROPERTY_NAME, 0);
    WasbFsck fsck = new WasbFsck(conf);
    fsck.setMockFileSystemForTesting(fs);
    fsck.run(new String[] { AzureBlobStorageTestAccount.MOCK_WASB_URI, command });
  }

  /**
   * Tests that we delete dangling files properly
   */
  @Test
  public void testDelete() throws Exception {
    Path danglingFile = new Path("/crashedInTheMiddle");

    // Create a file and leave it dangling and try to delete it.
    FSDataOutputStream stream = fs.create(danglingFile);
    stream.write(new byte[] { 1, 2, 3 });
    stream.flush();

    // Now we should still only see a zero-byte file in this place
    FileStatus fileStatus = fs.getFileStatus(danglingFile);
    assertNotNull(fileStatus);
    assertEquals(0, fileStatus.getLen());
    assertEquals(1, getNumTempBlobs());

    // Run WasbFsck -delete to delete the file.
    runFsck("-delete");

    // Now we should see no trace of the file.
    assertEquals(0, getNumTempBlobs());
    assertFalse(fs.exists(danglingFile));
  }
}
