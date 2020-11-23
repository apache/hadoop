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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that we put the correct metadata on blobs created through WASB.
 */
public class TestBlobMetadata extends AbstractWasbTestWithTimeout {
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

  private static String getExpectedOwner() throws Exception {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  private static String getExpectedPermissionString(String permissionString)
      throws Exception {
    return String.format(
        "{\"owner\":\"%s\",\"group\":\"%s\",\"permissions\":\"%s\"}",
        getExpectedOwner(),
        NativeAzureFileSystem.AZURE_DEFAULT_GROUP_DEFAULT,
        permissionString);
  }

  /**
   * Tests that WASB stamped the version in the container metadata.
   */
  @Test
  public void testContainerVersionMetadata() throws Exception {
    // Do a write operation to trigger version stamp
    fs.createNewFile(new Path("/foo"));
    HashMap<String, String> containerMetadata =
        backingStore.getContainerMetadata();
    assertNotNull(containerMetadata);
    assertEquals(AzureNativeFileSystemStore.CURRENT_WASB_VERSION,
        containerMetadata.get(AzureNativeFileSystemStore.VERSION_METADATA_KEY));
  }

  private static final class FsWithPreExistingContainer implements Closeable {
    private final MockStorageInterface mockStorage;
    private final NativeAzureFileSystem fs;

    private FsWithPreExistingContainer(MockStorageInterface mockStorage,
        NativeAzureFileSystem fs) {
      this.mockStorage = mockStorage;
      this.fs = fs;
    }

    public NativeAzureFileSystem getFs() {
      return fs;
    }

    public HashMap<String, String> getContainerMetadata() {
      return mockStorage.getBackingStore().getContainerMetadata();
    }

    public static FsWithPreExistingContainer create() throws Exception {
      return create(null);
    }

    public static FsWithPreExistingContainer create(
        HashMap<String, String> containerMetadata) throws Exception {
      AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
      MockStorageInterface mockStorage = new MockStorageInterface();
      store.setAzureStorageInteractionLayer(mockStorage);
      NativeAzureFileSystem fs = new NativeAzureFileSystem(store);
      Configuration conf = new Configuration();
      AzureBlobStorageTestAccount.setMockAccountKey(conf);
      mockStorage.addPreExistingContainer(
          AzureBlobStorageTestAccount.getMockContainerUri(), containerMetadata);
      fs.initialize(new URI(AzureBlobStorageTestAccount.MOCK_WASB_URI), conf);
      return new FsWithPreExistingContainer(mockStorage, fs);
    }

    @Override
    public void close() throws IOException {
      fs.close();
    }
  }

  /**
   * Tests that WASB stamped the version in the container metadata if it does a
   * write operation to a pre-existing container.
   */
  @Test
  public void testPreExistingContainerVersionMetadata() throws Exception {
    // Create a mock storage with a pre-existing container that has no
    // WASB version metadata on it.
    FsWithPreExistingContainer fsWithContainer = FsWithPreExistingContainer
        .create();

    // Now, do some read operations (should touch the metadata)
    assertFalse(fsWithContainer.getFs().exists(new Path("/IDontExist")));
    assertEquals(0, fsWithContainer.getFs().listStatus(new Path("/")).length);

    // Check that no container metadata exists yet
    assertNull(fsWithContainer.getContainerMetadata());

    // Now do a write operation - should stamp the version
    fsWithContainer.getFs().mkdirs(new Path("/dir"));

    // Check that now we have the version stamp
    assertNotNull(fsWithContainer.getContainerMetadata());
    assertEquals(
        AzureNativeFileSystemStore.CURRENT_WASB_VERSION,
        fsWithContainer.getContainerMetadata().get(
            AzureNativeFileSystemStore.VERSION_METADATA_KEY));
    fsWithContainer.close();
  }

  /**
   * Tests that WASB works well with an older version container with ASV-era
   * version and metadata.
   */
  @Test
  public void testFirstContainerVersionMetadata() throws Exception {
    // Create a mock storage with a pre-existing container that has
    // ASV version metadata on it.
    HashMap<String, String> containerMetadata = new HashMap<String, String>();
    containerMetadata.put(AzureNativeFileSystemStore.OLD_VERSION_METADATA_KEY,
        AzureNativeFileSystemStore.FIRST_WASB_VERSION);
    FsWithPreExistingContainer fsWithContainer = FsWithPreExistingContainer
        .create(containerMetadata);

    // Now, do some read operations (should touch the metadata)
    assertFalse(fsWithContainer.getFs().exists(new Path("/IDontExist")));
    assertEquals(0, fsWithContainer.getFs().listStatus(new Path("/")).length);

    // Check that no container metadata exists yet
    assertEquals(
        AzureNativeFileSystemStore.FIRST_WASB_VERSION,
        fsWithContainer.getContainerMetadata().get(
            AzureNativeFileSystemStore.OLD_VERSION_METADATA_KEY));
    assertNull(fsWithContainer.getContainerMetadata().get(
        AzureNativeFileSystemStore.VERSION_METADATA_KEY));

    // Now do a write operation - should stamp the version
    fsWithContainer.getFs().mkdirs(new Path("/dir"));

    // Check that now we have the version stamp
    assertEquals(
        AzureNativeFileSystemStore.CURRENT_WASB_VERSION,
        fsWithContainer.getContainerMetadata().get(
            AzureNativeFileSystemStore.VERSION_METADATA_KEY));
    assertNull(fsWithContainer.getContainerMetadata().get(
        AzureNativeFileSystemStore.OLD_VERSION_METADATA_KEY));
    fsWithContainer.close();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testPermissionMetadata() throws Exception {
    FsPermission justMe = new FsPermission(FsAction.READ_WRITE, FsAction.NONE,
        FsAction.NONE);
    Path selfishFile = new Path("/noOneElse");
    fs.create(selfishFile, justMe, true, 4096, fs.getDefaultReplication(),
        fs.getDefaultBlockSize(), null).close();
    String mockUri = AzureBlobStorageTestAccount.toMockUri(selfishFile);
    assertNotNull("converted URI", mockUri);
    HashMap<String, String> metadata = backingStore
        .getMetadata(mockUri);
    assertNotNull(metadata);
    String storedPermission = metadata.get("hdi_permission");
    assertEquals(getExpectedPermissionString("rw-------"), storedPermission);
    FileStatus retrievedStatus = fs.getFileStatus(selfishFile);
    assertNotNull(retrievedStatus);
    assertEquals(justMe, retrievedStatus.getPermission());
    assertEquals(getExpectedOwner(), retrievedStatus.getOwner());
    assertEquals(NativeAzureFileSystem.AZURE_DEFAULT_GROUP_DEFAULT,
        retrievedStatus.getGroup());
  }

  /**
   * Tests that WASB understands the old-style ASV metadata and changes it when
   * it gets the chance.
   */
  @Test
  public void testOldPermissionMetadata() throws Exception {
    Path selfishFile = new Path("/noOneElse");
    HashMap<String, String> metadata =
        new HashMap<String, String>();
    metadata.put("asv_permission",
        getExpectedPermissionString("rw-------"));
    backingStore.setContent(
        AzureBlobStorageTestAccount.toMockUri(selfishFile),
        new byte[] { },
        metadata, false, 0);
    FsPermission justMe = new FsPermission(
        FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    FileStatus retrievedStatus = fs.getFileStatus(selfishFile);
    assertNotNull(retrievedStatus);
    assertEquals(justMe, retrievedStatus.getPermission());
    assertEquals(getExpectedOwner(), retrievedStatus.getOwner());
    assertEquals(NativeAzureFileSystem.AZURE_DEFAULT_GROUP_DEFAULT,
        retrievedStatus.getGroup());
    FsPermission meAndYou = new FsPermission(
        FsAction.READ_WRITE, FsAction.READ_WRITE, FsAction.NONE);
    fs.setPermission(selfishFile, meAndYou);
    metadata =
        backingStore.getMetadata(
            AzureBlobStorageTestAccount.toMockUri(selfishFile));
    assertNotNull(metadata);
    String storedPermission = metadata.get("hdi_permission");
    assertEquals(getExpectedPermissionString("rw-rw----"),
        storedPermission);
    assertNull(metadata.get("asv_permission"));
  }

  @Test
  public void testFolderMetadata() throws Exception {
    Path folder = new Path("/folder");
    FsPermission justRead = new FsPermission(FsAction.READ, FsAction.READ,
        FsAction.READ);
    fs.mkdirs(folder, justRead);
    HashMap<String, String> metadata = backingStore
        .getMetadata(AzureBlobStorageTestAccount.toMockUri(folder));
    assertNotNull(metadata);
    assertEquals("true", metadata.get("hdi_isfolder"));
    assertEquals(getExpectedPermissionString("r--r--r--"),
        metadata.get("hdi_permission"));
  }
}
