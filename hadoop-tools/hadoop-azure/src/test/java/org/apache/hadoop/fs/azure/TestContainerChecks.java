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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import java.io.FileNotFoundException;
import java.util.EnumSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.CreateOptions;
import org.junit.After;
import org.junit.Test;

import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * Tests that WASB creates containers only if needed.
 */
public class TestContainerChecks {
  private AzureBlobStorageTestAccount testAccount;

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  @Test
  public void testContainerExistAfterDoesNotExist() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container and will set file system store
    // state to DoesNotExist
    try {
      fs.listStatus(new Path("/"));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());

    // Create a container outside of the WASB FileSystem
    container.create();
    // Add a file to the container outside of the WASB FileSystem
    CloudBlockBlob blob = testAccount.getBlobReference("foo");
    BlobOutputStream outputStream = blob.openOutputStream();
    outputStream.write(new byte[10]);
    outputStream.close();

    // Make sure the file is visible
    assertTrue(fs.exists(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerCreateAfterDoesNotExist() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container and will set file system store
    // state to DoesNotExist
    try {
      assertNull(fs.listStatus(new Path("/")));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());

    // Create a container outside of the WASB FileSystem
    container.create();

    // Write should succeed
    assertTrue(fs.createNewFile(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerCreateOnWrite() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container.
    try {
      fs.listStatus(new Path("/"));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());

    // Neither should a read.
    try {
      fs.open(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (FileNotFoundException ex) {
    }
    assertFalse(container.exists());

    // Neither should a rename
    assertFalse(fs.rename(new Path("/foo"), new Path("/bar")));
    assertFalse(container.exists());

    // But a write should.
    assertTrue(fs.createNewFile(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerChecksWithSas() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // The container shouldn't be there
    assertFalse(container.exists());

    // A write should just fail
    try {
      fs.createNewFile(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (AzureException ex) {
    }
    assertFalse(container.exists());
  }
}
