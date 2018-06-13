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

import static org.junit.Assume.assumeNotNull;

import java.io.FileNotFoundException;
import java.util.EnumSet;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount.CreateOptions;
import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * Tests that WASB creates containers only if needed.
 */
public class ITestContainerChecks extends AbstractWasbTestWithTimeout {
  private AzureBlobStorageTestAccount testAccount;
  private boolean runningInSASMode = false;

  @After
  public void tearDown() throws Exception {
    testAccount = AzureTestUtils.cleanup(testAccount);
  }

  @Before
  public void setMode() {
    runningInSASMode = AzureBlobStorageTestAccount.createTestConfiguration().
        getBoolean(AzureNativeFileSystemStore.KEY_USE_SECURE_MODE, false);
  }

  @Test
  public void testContainerExistAfterDoesNotExist() throws Exception {
    testAccount = blobStorageTestAccount();
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
          ex.getMessage().contains("is not found"));
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

  protected AzureBlobStorageTestAccount blobStorageTestAccount()
      throws Exception {
    return AzureBlobStorageTestAccount.create("",
        EnumSet.noneOf(CreateOptions.class));
  }

  @Test
  public void testContainerCreateAfterDoesNotExist() throws Exception {
    testAccount = blobStorageTestAccount();
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
          ex.getMessage().contains("is not found"));
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
    testAccount = blobStorageTestAccount();
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
          ex.getMessage().contains("is not found"));
    }
    assertFalse(container.exists());

    // Neither should a read.
    Path foo = new Path("/testContainerCreateOnWrite-foo");
    Path bar = new Path("/testContainerCreateOnWrite-bar");
    LambdaTestUtils.intercept(FileNotFoundException.class,
        new Callable<String>() {
          @Override
          public String call() throws Exception {
            fs.open(foo).close();
            return "Stream to " + foo;
          }
        }
    );
    assertFalse(container.exists());

    // Neither should a rename
    assertFalse(fs.rename(foo, bar));
    assertFalse(container.exists());

    // Create a container outside of the WASB FileSystem
    container.create();

    // But a write should.
    assertTrue(fs.createNewFile(foo));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerChecksWithSas() throws Exception {

    Assume.assumeFalse(runningInSASMode);
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // The container shouldn't be there
    assertFalse(container.exists());

    // A write should just fail
    try {
      fs.createNewFile(new Path("/testContainerChecksWithSas-foo"));
      assertFalse("Should've thrown.", true);
    } catch (AzureException ex) {
    }
    assertFalse(container.exists());
  }
}
