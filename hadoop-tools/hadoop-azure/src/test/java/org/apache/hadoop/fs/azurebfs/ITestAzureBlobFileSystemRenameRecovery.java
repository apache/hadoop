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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicity;
import org.apache.hadoop.fs.azurebfs.services.VersionedFileStatus;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderFormat;
import org.apache.hadoop.test.LambdaTestUtils;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_BLOB_DIR_RENAME_MAX_THREAD;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CONSUMER_MAX_LAG;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_PRODUCER_QUEUE_MAX_SIZE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.BLOB_ALREADY_EXISTS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.BLOB_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicity.SUFFIX;
import static org.apache.hadoop.fs.azurebfs.utils.AbfsTestUtils.createFiles;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test rename recovery operation.
 */
public class ITestAzureBlobFileSystemRenameRecovery extends
    AbstractAbfsIntegrationTest {

  private static final int FAILED_CALL = 15;

  private static final int TOTAL_FILES = 25;

  public ITestAzureBlobFileSystemRenameRecovery() throws Exception {
    super();
  }

  /**
   * Tests renaming a directory with a failure during the copy operation.
   * Simulates an error when copying on the 6th call.
   */
  @Test
  public void testRenameCopyFailureInBetween() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem(getConfig()))) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
      fs.getAbfsStore().setClient(client);
      Path src = new Path("/hbase/A1/A2");
      Path dst = new Path("/hbase/A1/A3");

      // Create sample files in the source directory
      createFiles(fs, src, TOTAL_FILES);

      // Track the number of copy operations
      AtomicInteger copyCall = new AtomicInteger(0);
      renameCrashInBetween(fs, src, dst, client, copyCall);
    }
  }

  /**
   * Tests renaming a directory with a failure during the delete operation.
   * Simulates an error on the 6th delete operation and verifies the behavior.
   */
  @Test
  public void testRenameDeleteFailureInBetween() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem(getConfig()))) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
      fs.getAbfsStore().setClient(client);
      Path src = new Path("/hbase/A1/A2");
      Path dst = new Path("/hbase/A1/A3");

      // Create sample files in the source directory
      createFiles(fs, src, TOTAL_FILES);

      // Track the number of delete operations
      AtomicInteger deleteCall = new AtomicInteger(0);
      Mockito.doAnswer(deleteRequest -> {
        if (deleteCall.get() == FAILED_CALL) {
          throw new AbfsRestOperationException(
              BLOB_PATH_NOT_FOUND.getStatusCode(),
              BLOB_PATH_NOT_FOUND.getErrorCode(),
              BLOB_PATH_NOT_FOUND.getErrorMessage(),
              new Exception());
        }
        deleteCall.incrementAndGet();
        return deleteRequest.callRealMethod();
      }).when(client).deleteBlobPath(Mockito.any(Path.class),
          Mockito.anyString(), Mockito.any(TracingContext.class));

      renameOperationWithRecovery(fs, src, dst, deleteCall);
    }
  }

  /**
   * Tests renaming a directory with a failure during the copy operation.
   * Since, destination path already exists, there will be adjustment in the
   * destination path. After crash recovery, recovery should succeed even in the
   * case when destination path already exists.
   * Simulates an error when copying on the 6th call.
   */
  @Test
  public void testRenameRecoveryWhenDestAlreadyExist() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem(getConfig()))) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
      fs.getAbfsStore().setClient(client);
      Path src = new Path("/hbase/A1/A2");
      Path dst = new Path("/hbase/A1/A3");

      // Create sample files in the source directory
      createFiles(fs, src, TOTAL_FILES);
      // Create the destination directory
      fs.mkdirs(dst);

      // Track the number of copy operations
      AtomicInteger copyCall = new AtomicInteger(0);
      // Assertions to validate source and dest status before rename
      validateRename(fs, src, dst, true, true, false);
      renameCrashInBetween(fs, src, dst, client, copyCall);
    }
  }

  /**
   * Tests renaming a directory with a failure during the copy operation.
   * Since, destination path already exists, there will be adjustment in the
   * destination path. After crash recovery, recovery should succeed even in the
   * case when destination path already exists.
   * Simulates an error when copying on the 6th call.
   */
  @Test
  public void testRenameRecoveryWithMarkerPresentInDest() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem(getConfig()))) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
      fs.getAbfsStore().setClient(client);
      Path src = new Path("/hbase/A1/A2");
      Path dst = new Path("/hbase/A1/A3");

      // Create sample files in the source directory
      createFiles(fs, src, TOTAL_FILES);

      // Track the number of copy operations
      AtomicInteger copyCall = new AtomicInteger(0);
      renameCrashInBetween(fs, src, dst, client, copyCall);
    }
  }

  /**
   * Test to check behaviour when rename is called on a atomic rename directory
   * for which rename pending json file is already present.
   * @throws Exception in case of failure
   */
  @Test
  public void testRenameWhenAlreadyRenamePendingJsonFilePresent() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem(getConfig()))) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
      fs.getAbfsStore().setClient(client);
      Path src = new Path("/hbase/A1/A2");
      Path dst = new Path("/hbase/A1/A3");

      // Create sample files in the source directory
      createFiles(fs, src, TOTAL_FILES);

      // Track the number of copy operations
      AtomicInteger copyCall = new AtomicInteger(0);
      renameCrashInBetween(fs, src, dst, client, copyCall);
    }
  }

  /**
   * Test case to verify crash recovery with a single child folder.
   *
   * This test simulates a scenario where a pending rename JSON file exists for a single child folder
   * under the parent directory. It ensures that when listing the files in the parent directory,
   * only the child folder is returned, and no additional files are listed.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithSingleChildFolder() throws Exception {
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);

    FileStatus[] fileStatuses = fs.listStatus(new Path("/hbase/A1"));

    Assertions.assertThat(fileStatuses.length)
        .describedAs("List should return 0 file")
        .isEqualTo(0);
    assertPendingJsonFile(fs, renameJson, fileStatuses, path, false);
  }

  /**
   * Test case to verify crash recovery with multiple child folders.
   *
   * This test simulates a scenario where a pending rename JSON file exists, and multiple files are
   * created in the parent directory. It ensures that when listing the files in the parent directory,
   * the correct number of files is returned.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithMultipleChildFolder() throws Exception {
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);

    fs.create(new Path("/hbase/A1/file1.txt"));
    fs.create(new Path("/hbase/A1/file2.txt"));

    FileStatus[] fileStatuses = fs.listStatus(new Path("/hbase/A1"));

    Assertions.assertThat(fileStatuses.length)
        .describedAs("List should return 2 files")
        .isEqualTo(2);
    assertPendingJsonFile(fs, renameJson, fileStatuses, path, false);
  }

  /**
   * Test case to verify crash recovery with a pending rename JSON file.
   *
   * This test simulates a scenario where a pending rename JSON file exists in the parent directory,
   * and it ensures that after the deletion of the target directory and creation of new files,
   * the listing operation correctly returns the remaining files.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithPendingJsonFile() throws Exception {
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);

    fs.delete(path, true);
    fs.create(new Path("/hbase/A1/file1.txt"));
    fs.create(new Path("/hbase/A1/file2.txt"));

    FileStatus[] fileStatuses = fs.listStatus(path.getParent());

    Assertions.assertThat(fileStatuses.length)
        .describedAs("List should return 2 files")
        .isEqualTo(2);
    assertPendingJsonFile(fs, renameJson, fileStatuses, path, false);
  }

  /**
   * Test case to verify crash recovery when no pending rename JSON file exists.
   *
   * This test simulates a scenario where there is no pending rename JSON file in the directory.
   * It ensures that the listing operation correctly returns all files in the parent directory.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithoutAnyPendingJsonFile() throws Exception {
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);

    fs.delete(renameJson, true);
    fs.create(new Path("/hbase/A1/file1.txt"));
    fs.create(new Path("/hbase/A1/file2.txt"));

    FileStatus[] fileStatuses = fs.listStatus(path.getParent());

    Assertions.assertThat(fileStatuses.length)
        .describedAs("List should return 3 files")
        .isEqualTo(3);
    // Pending json file not present, no recovery take place, so source directory should exist.
    assertPendingJsonFile(fs, renameJson, fileStatuses, path, true);
  }

  /**
   * Test case to verify crash recovery when a pending rename JSON directory exists.
   *
   * This test simulates a scenario where a pending rename JSON directory exists, ensuring that the
   * listing operation correctly returns all files in the parent directory without triggering a redo
   * rename operation. It also checks that the directory with the suffix "-RenamePending.json" exists.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithPendingJsonDir() throws Exception {
    try (AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem())) {
      assumeBlobServiceType();
      AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

      Path path = new Path("/hbase/A1/A2");
      Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
      fs.mkdirs(renameJson);

      fs.create(new Path(path.getParent(), "file1.txt"));
      fs.create(new Path(path, "file2.txt"));

      AtomicInteger redoRenameCall = new AtomicInteger(0);
      Mockito.doAnswer(answer -> {
        redoRenameCall.incrementAndGet();
        return answer.callRealMethod();
      }).when(client).getRedoRenameAtomicity(Mockito.any(Path.class),
          Mockito.anyInt(), Mockito.any(TracingContext.class));

      FileStatus[] fileStatuses = fs.listStatus(path.getParent());

      Assertions.assertThat(fileStatuses.length)
          .describedAs("List should return 3 files")
          .isEqualTo(3);

      Assertions.assertThat(redoRenameCall.get())
          .describedAs("No redo rename call should be made")
          .isEqualTo(0);

      Assertions.assertThat(
              Arrays.stream(fileStatuses)
                  .anyMatch(status -> renameJson.toUri().getPath().equals(status.getPath().toUri().getPath())))
          .describedAs("Directory with suffix -RenamePending.json should exist.")
          .isTrue();
    }
  }

  /**
   * Test case to verify crash recovery during listing with multiple pending rename JSON files.
   *
   * This test simulates a scenario where multiple pending rename JSON files exist, ensuring that
   * crash recovery properly handles the situation. It verifies that two redo rename calls are made
   * and that the list operation returns the correct number of paths.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testListCrashRecoveryWithMultipleJsonFile() throws Exception {
    Path path = new Path("/hbase/A1/A2");

    // 1st Json file
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    // 2nd Json file
    Path path2 = new Path("/hbase/A1/A3");
    fs.create(new Path(path2, "file3.txt"));

    Path renameJson2 = new Path(path2.getParent(), path2.getName() + SUFFIX);
    VersionedFileStatus fileStatus
        = (VersionedFileStatus) fs.getFileStatus(path2);

    new RenameAtomicity(path2, new Path("/hbase/test4"),
        renameJson2, getTestTracingContext(fs, true),
        fileStatus.getEtag(), client).preRename();

    fs.create(new Path(path, "file2.txt"));

    AtomicInteger redoRenameCall = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
      redoRenameCall.incrementAndGet();
      return answer.callRealMethod();
    }).when(client).getRedoRenameAtomicity(Mockito.any(Path.class),
        Mockito.anyInt(), Mockito.any(TracingContext.class));

    FileStatus[] fileStatuses = fs.listStatus(path.getParent());

    Assertions.assertThat(fileStatuses.length)
        .describedAs("List should return 0 paths")
        .isEqualTo(0);

    Assertions.assertThat(redoRenameCall.get())
        .describedAs("2 redo rename calls should be made")
        .isEqualTo(2);
    assertPathStatus(fs, path, false,
        "Source directory should not exist.");
    assertPathStatus(fs, new Path("/hbase/test4/file.txt"), true,
        "File in destination directory should exist.");
    assertPathStatus(fs, path2, false,
        "Source directory should not exist");
    assertPathStatus(fs, new Path("/hbase/test4/file2.txt"), true,
        "File in destination directory should exist.");
    assertPathStatus(fs, renameJson, false,
        "Rename Pending Json file should not exist.");
    assertPathStatus(fs, renameJson2, false,
        "Rename Pending Json file should not exist.");
  }

  /**
   * Test case to verify path status when a pending rename JSON file exists.
   *
   * This test simulates a scenario where a rename operation was pending, and ensures that
   * the path status retrieval triggers a redo rename operation. The test also checks that
   * the correct error code (`PATH_NOT_FOUND`) is returned.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testGetPathStatusWithPendingJsonFile() throws Exception {
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);

    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);

    fs.create(new Path("/hbase/A1/file1.txt"));
    fs.create(new Path("/hbase/A1/file2.txt"));

    AbfsConfiguration conf = fs.getAbfsStore().getAbfsConfiguration();

    AtomicInteger redoRenameCall = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
      redoRenameCall.incrementAndGet();
      return answer.callRealMethod();
    }).when(client).getRedoRenameAtomicity(Mockito.any(Path.class),
        Mockito.anyInt(), Mockito.any(TracingContext.class));

    TracingContext tracingContext = new TracingContext(
        conf.getClientCorrelationId(), fs.getFileSystemId(),
        FSOperationType.GET_FILESTATUS, TracingHeaderFormat.ALL_ID_FORMAT, null);

    AzureServiceErrorCode azureServiceErrorCode = intercept(
        AbfsRestOperationException.class, () -> client.getPathStatus(
            path.toUri().getPath(), true,
            tracingContext, null)).getErrorCode();

    Assertions.assertThat(azureServiceErrorCode.getErrorCode())
        .describedAs("Path had to be recovered from atomic rename operation.")
        .isEqualTo(PATH_NOT_FOUND.getErrorCode());

    Assertions.assertThat(redoRenameCall.get())
        .describedAs("There should be one redo rename call")
        .isEqualTo(1);

    Assertions.assertThat(fs.exists(renameJson))
        .describedAs("Rename Pending Json file should not exist.")
        .isFalse();
  }

  /**
   * Test case to verify the behavior when the ETag of a file changes during a rename operation.
   *
   * This test simulates a scenario where the ETag of a file changes after the creation of a
   * rename pending JSON file. The steps include:
   * - Creating a rename pending JSON file with an old ETag.
   * - Deleting the original directory for an ETag change.
   * - Creating new files in the directory.
   * - Verifying that the copy blob call is not triggered.
   * - Verifying that the rename atomicity operation is called once.
   *
   * The test ensures that the system correctly handles the ETag change during the rename process.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testETagChangedDuringRename() throws Exception {
    assumeBlobServiceType();
    Path path = new Path("/hbase/A1/A2");
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    // Create rename pending json file with old etag
    AzureBlobFileSystem fs = createJsonFile(path, renameJson);
    AbfsBlobClient abfsBlobClient = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.getAbfsStore().setClient(abfsBlobClient);

    // Delete the directory to change etag
    fs.delete(path, true);

    fs.create(new Path(path, "file1.txt"));
    fs.create(new Path(path, "file2.txt"));
    AtomicInteger numberOfCopyBlobCalls = new AtomicInteger(0);
    Mockito.doAnswer(copyBlob -> {
          numberOfCopyBlobCalls.incrementAndGet();
          return copyBlob.callRealMethod();
        })
        .when(abfsBlobClient)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));

    AtomicInteger numberOfRedoRenameAtomicityCalls = new AtomicInteger(0);
    Mockito.doAnswer(redoRenameAtomicity -> {
          numberOfRedoRenameAtomicityCalls.incrementAndGet();
          return redoRenameAtomicity.callRealMethod();
        })
        .when(abfsBlobClient)
        .getRedoRenameAtomicity(Mockito.any(Path.class), Mockito.anyInt(),
            Mockito.any(TracingContext.class));
    // Call list status to trigger rename redo
    fs.listStatus(path.getParent());
    Assertions.assertThat(numberOfRedoRenameAtomicityCalls.get())
        .describedAs("There should be one call to getRedoRenameAtomicity")
        .isEqualTo(1);
    Assertions.assertThat(numberOfCopyBlobCalls.get())
        .describedAs("There should be no copy blob call")
        .isEqualTo(0);
    Assertions.assertThat(fs.exists(renameJson))
        .describedAs("Rename Pending Json file should not exist.")
        .isFalse();
  }

  /**
   * Triggers rename recovery by calling getPathStatus on the source path.
   * This simulates a scenario where the rename operation was interrupted,
   * and the system needs to recover the state of the source path.
   *
   * @param fs The AzureBlobFileSystem instance.
   * @param src The source path to trigger recovery on.
   * @throws Exception If an error occurs during the recovery process.
   */
  private void triggerRenameRecovery(AzureBlobFileSystem fs, Path src) throws Exception {
    // Trigger rename recovery
    TracingContext tracingContext = new TracingContext(
        getConfiguration().getClientCorrelationId(), fs.getFileSystemId(),
        FSOperationType.GET_FILESTATUS, TracingHeaderFormat.ALL_ID_FORMAT, null);
    AzureServiceErrorCode errorCode = LambdaTestUtils.intercept(
        AbfsRestOperationException.class, () -> {
          fs.getAbfsStore().getClient().getPathStatus(src.toUri().getPath(), true,
              tracingContext, null);
        }).getErrorCode();
    Assertions.assertThat(errorCode)
        .describedAs("Path had to be recovered from atomic rename operation.")
        .isEqualTo(PATH_NOT_FOUND);
  }

  /**
   * Simulates a failure during the rename operation by throwing an exception
   * when the copyBlob method is called. This is used to test the behavior of
   * the rename recovery operation when a blob already exists at the destination.
   *
   * @param fs The AzureBlobFileSystem instance.
   * @param src The source path to rename.
   * @param dst The destination path for the rename operation.
   * @param client The AbfsBlobClient instance.
   * @param copyCall The AtomicInteger to track the number of copy calls.
   * @throws AzureBlobFileSystemException If an error occurs during the operation.
   */
  private void renameCrashInBetween(AzureBlobFileSystem fs, Path src, Path dst,
      AbfsBlobClient client, AtomicInteger copyCall)
      throws Exception {
    Mockito.doAnswer(copyRequest -> {
      if (copyCall.get() == FAILED_CALL) {
        throw new AbfsRestOperationException(
            BLOB_ALREADY_EXISTS.getStatusCode(),
            BLOB_ALREADY_EXISTS.getErrorCode(),
            BLOB_ALREADY_EXISTS.getErrorMessage(),
            new Exception());
      }
      copyCall.incrementAndGet();
      return copyRequest.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class),
        Mockito.any(Path.class), Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));
    renameOperationWithRecovery(fs, src, dst, copyCall);
  }

  /**
   * Helper method to create the configuration for the AzureBlobFileSystem.
   *
   * @return The configuration object.
   */
  private Configuration getConfig() {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(FS_AZURE_PRODUCER_QUEUE_MAX_SIZE, "5");
    config.set(FS_AZURE_CONSUMER_MAX_LAG, "3");
    config.set(FS_AZURE_BLOB_DIR_RENAME_MAX_THREAD, "2");
    return config;
  }

  /**
   * Spies on the AzureBlobFileSystem's store and client to enable mocking and verification
   * of client interactions in tests. It replaces the actual store and client with mocked versions.
   *
   * @param fs the AzureBlobFileSystem instance
   * @return the spied AbfsClient for interaction verification
   */
  private AbfsClient addSpyHooksOnClient(final AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();
    return client;
  }

  /**
   * Helper method to validate that the rename was successful and that the destination exists.
   *
   * @param fs The AzureBlobFileSystem instance to check the existence on.
   * @param dst The destination path.
   * @param src The source path.
   * @throws IOException If an I/O error occurs during the validation.
   */
  private void validateRename(AzureBlobFileSystem fs, Path src, Path dst,
      boolean isSrcExist, boolean isDstExist, boolean isJsonExist) throws Exception {
    // Validate pending JSON file status
    assertPathStatus(fs,
        new Path(src.getParent(), src.getName() + SUFFIX), isJsonExist,
        "Pending JSON file");

    // Validate source directory status
    assertPathStatus(fs, src, isSrcExist, "Source directory");

    // Validate destination directory status
    assertPathStatus(fs, dst, isDstExist, "Destination directory");
  }

  /**
   * Helper method to assert the status of a path in the AzureBlobFileSystem.
   *
   * @param fs The AzureBlobFileSystem instance to check the existence on.
   * @param path The path to check.
   * @param shouldExist Whether the path should exist or not.
   * @param description A description for the assertion.
   * @throws Exception If an error occurs during the assertion.
   */
  private void assertPathStatus(AzureBlobFileSystem fs, Path path,
      boolean shouldExist, String description) throws Exception{
    TracingContext tracingContext = getTestTracingContext(fs, true);
    AbfsBlobClient client = ((AbfsBlobClient) fs.getAbfsClient());
    if (shouldExist) {
      int actualStatus = client.getPathStatus(
              path.toUri().getPath(), tracingContext,
              null, true)
          .getResult().getStatusCode();
      Assertions.assertThat(actualStatus)
          .describedAs("%s should exists", description)
          .isEqualTo(HTTP_OK);
    } else {
      AzureServiceErrorCode errorCode = LambdaTestUtils.intercept(
          AbfsRestOperationException.class, () -> {
            client.getPathStatus(path.toUri().getPath(), true,
                tracingContext, null);
          }).getErrorCode();
      Assertions.assertThat(errorCode)
          .describedAs("%s should not exists", description)
          .isEqualTo(BLOB_PATH_NOT_FOUND);
    }
  }

  /**
   * Helper method to create a json file.
   * @param path parent path
   * @param renameJson rename json path
   * @return file system
   * @throws IOException in case of failure
   */
  private AzureBlobFileSystem createJsonFile(Path path, Path renameJson)
      throws IOException {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();

    fs.setWorkingDirectory(new Path(ROOT_PATH));
    fs.create(new Path(path, "file.txt"));

    VersionedFileStatus fileStatus
        = (VersionedFileStatus) fs.getFileStatus(path);

    new RenameAtomicity(path, new Path("/hbase/test4"),
        renameJson, getTestTracingContext(fs, true),
        fileStatus.getEtag(), client)
        .preRename();

    Assertions.assertThat(fs.exists(renameJson))
        .describedAs("Rename Pending Json file should exist.")
        .isTrue();

    return fs;
  }

  /**
   * Helper method to perform the rename operation and validate the results.
   *
   * @param fs The AzureBlobFileSystem instance to use for the rename operation.
   * @param src The source path (directory).
   * @param dst The destination path (directory).
   * @param countCall The AtomicInteger to track the number of operations.
   * @throws Exception If an error occurs during the rename operation.
   */
  private void renameOperationWithRecovery(AzureBlobFileSystem fs, Path src,
      Path dst, AtomicInteger countCall) throws Exception {
    Assertions.assertThat(fs.rename(src, dst))
        .describedAs("Rename should crash in between.")
        .isFalse();

    // Validate copy operation count
    Assertions.assertThat(countCall.get())
        .describedAs("Operation count should be less than 10.")
        .isLessThan(TOTAL_FILES);

    // Assertions to validate renamed destination and source
    validateRename(fs, src, dst, true, true, true);

    // Validate that rename redo operation was triggered
    countCall.set(0);
    triggerRenameRecovery(fs, src);

    Assertions.assertThat(countCall.get())
        .describedAs("Operation count should be greater than 0.")
        .isGreaterThan(0);

    // Validate final state of destination and source
    validateRename(fs, src, dst, false, true, false);
  }

  /**
   * Helper method to assert that the pending JSON file does not exist
   * and that the list of file statuses does not contain the rename pending JSON file.
   *
   * @param fs The AzureBlobFileSystem instance.
   * @param renameJson The path of the rename pending JSON file.
   * @param fileStatuses The array of FileStatus objects to check.
   * @param srcPath The source path to check.
   * @throws Exception If an error occurs during the assertion.
   */
  private void assertPendingJsonFile(AzureBlobFileSystem fs,
      Path renameJson, FileStatus[] fileStatuses,
      Path srcPath, boolean isSrcPathExist) throws Exception {
    Assertions.assertThat(fs.exists(renameJson))
        .describedAs("Rename Pending Json file should not exist.")
        .isFalse();

    Assertions.assertThat(
            Arrays.stream(fileStatuses)
                .anyMatch(status ->
                    renameJson.toUri().getPath()
                        .equals(status.getPath().toUri().getPath())))
        .describedAs(
            "List status should not contains any file with suffix -RenamePending.json.")
        .isFalse();

    Assertions.assertThat(
            Arrays.stream(fileStatuses)
                .anyMatch(status ->
                    srcPath.toUri().getPath()
                        .equals(status.getPath().toUri().getPath())))
        .describedAs(
            "List status should not contains source path.")
        .isEqualTo(isSrcPathExist);
  }
}
