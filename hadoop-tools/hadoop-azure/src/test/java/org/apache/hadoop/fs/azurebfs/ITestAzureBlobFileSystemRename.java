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
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsDriverException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.BlobRenameHandler;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicity;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicityTestUtils;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DIRECTORY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.EMPTY_STRING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_TRANSACTION_ID;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_RESOURCE_TYPE;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil.mockAddClientTransactionIdToHeader;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.ERR_RENAME_RECOVERY;
import static org.apache.hadoop.fs.azurebfs.services.RenameAtomicity.SUFFIX;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;
import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Test rename operation.
 */
public class ITestAzureBlobFileSystemRename extends
    AbstractAbfsIntegrationTest {

  private static final int MAX_ITERATIONS = 20;

  private static final int BLOB_COUNT = 11;

  public ITestAzureBlobFileSystemRename() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileIsRenamed() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("testEnsureFileIsRenamed-src");
    touch(src);
    Path dest = path("testEnsureFileIsRenamed-dest");
    fs.delete(dest, true);
    assertRenameOutcome(fs, src, dest, true);

    assertIsFile(fs, dest);
    assertPathDoesNotExist(fs, "expected renamed", src);
  }

  @Test
  public void testRenameWithPreExistingDestination() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path src = path("renameSrc");
    touch(src);
    Path dest = path("renameDest");
    touch(dest);
    assertRenameOutcome(fs, src, dest, false);
  }

  @Test
  public void testRenameFileUnderDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path sourceDir = path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = path("/testDst");
    assertRenameOutcome(fs, sourceDir, destDir, true);
    FileStatus[] fileStatus = fs.listStatus(destDir);
    assertNotNull("Null file status", fileStatus);
    FileStatus status = fileStatus[0];
    assertEquals("Wrong filename in " + status,
        filename, status.getPath().getName());
  }

  @Test
  public void testRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testDir = path("testDir");
    fs.mkdirs(testDir);
    Path test1 = new Path(testDir + "/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path(testDir + "/test1/test2"));
    fs.mkdirs(new Path(testDir + "/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path(testDir + "/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    Path source = path("/test");
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path(source + "/" + i);
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          touch(fileName);
          return null;
        }
      };

      tasks.add(es.submit(callable));
    }

    for (Future<Void> task : tasks) {
      task.get();
    }

    es.shutdownNow();
    Path dest = path("/renamedDir");
    assertRenameOutcome(fs, source, dest, true);

    FileStatus[] files = fs.listStatus(dest);
    assertEquals("Wrong number of files in listing", 1000, files.length);
    assertPathDoesNotExist(fs, "rename source dir", source);
  }

  @Test
  public void testRenameRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    assertRenameOutcome(fs,
        new Path("/"),
        new Path("/testRenameRoot"),
        false);
    assertRenameOutcome(fs,
        new Path(fs.getUri().toString() + "/"),
        new Path(fs.getUri().toString() + "/s"),
        false);
  }

  @Test
  public void testPosixRenameDirectory() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    Path testDir2 = path("testDir2");
    fs.mkdirs(new Path(testDir2 + "/test1/test2/test3"));
    fs.mkdirs(new Path(testDir2 + "/test4"));
    assertTrue(fs.rename(new Path(testDir2 + "/test1/test2/test3"),
        new Path(testDir2 + "/test4")));
    assertPathExists(fs, "This path should exist", testDir2);
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test1/test2"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4"));
    assertPathExists(fs, "This path should exist",
        new Path(testDir2 + "/test4/test3"));
    assertPathDoesNotExist(fs, "This path should not exist",
        new Path(testDir2 + "/test1/test2/test3"));
  }

  @Test
  public void testRenameWithNoDestinationParentDir() throws Exception {
    describe("Verifying the expected behaviour of ABFS rename when "
        + "destination parent Dir doesn't exist.");

    final AzureBlobFileSystem fs = getFileSystem();
    Path sourcePath = path(getMethodName());
    Path destPath = new Path("falseParent", "someChildFile");

    byte[] data = dataset(1024, 'a', 'z');
    writeDataset(fs, sourcePath, data, data.length, 1024, true);

    // Verify that renaming on a destination with no parent dir wasn't
    // successful.
    assertFalse("Rename result expected to be false with no Parent dir",
        fs.rename(sourcePath, destPath));

    // Verify that metadata was in an incomplete state after the rename
    // failure, and we retired the rename once more.
    IOStatistics ioStatistics = fs.getIOStatistics();
    AbfsClient client = fs.getAbfsStore().getClient();
    IOStatisticAssertions.assertThatStatisticCounter(ioStatistics,
            RENAME_PATH_ATTEMPTS.getStatName())
        .describedAs("For Dfs endpoint: There should be 2 rename "
            + "attempts if metadata incomplete state failure is hit."
            + "For Blob endpoint: There would be only one rename attempt which "
            + "would have a failed precheck.")
        .isEqualTo(client instanceof AbfsDfsClient ? 2 : 1);
  }

  /**
   * Tests renaming a directory to the root directory. This test ensures that a directory can be renamed
   * successfully to the root directory and that the renamed directory appears as expected.
   *
   * The test creates a directory (`/src1/src2`), renames it to the root (`/`), and verifies that
   * the renamed directory (`/src2`) exists in the root.
   *
   * @throws Exception if an error occurs during test execution
   */
  @Test
  public void testRenameToRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src1/src2"));
    assertTrue(fs.rename(new Path("/src1/src2"), new Path("/")));
    assertTrue(fs.exists(new Path("/src2")));
  }

  /**
   * Tests renaming a non-existent file to the root directory. This test ensures that the rename
   * operation returns `false` when attempting to rename a file that does not exist.
   *
   * The test attempts to rename a file located at `/file` (which does not exist) to the root directory `/`
   * and verifies that the rename operation fails.
   *
   * @throws Exception if an error occurs during test execution
   */
  @Test
  public void testRenameNotFoundBlobToEmptyRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assertFalse(fs.rename(new Path("/file"), new Path("/")));
  }

  /**
   * Tests renaming a source path to a destination path that contains a colon in the path.
   * This verifies that the rename operation handles paths with special characters like a colon.
   *
   * The test creates a source directory and renames it to a destination path that includes a colon,
   * ensuring that the operation succeeds without errors.
   *
   * @throws Exception if an error occurs during test execution
   */
  @Test(expected = IOException.class)
  public void testRenameBlobToDstWithColonInPath() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.create(new Path("/src"));
    fs.rename(new Path("/src"), new Path("/dst:file"));
  }

  /**
   * Tests renaming a directory within the same parent directory when there is no marker file.
   * This test ensures that the rename operation succeeds even when no special marker file is present.
   *
   * The test creates a file in a directory, deletes the blob path using the client, and then attempts
   * to rename the directory. It verifies that the rename operation completes successfully.
   *
   * @throws Exception if an error occurs during test execution
   */
  @Test
  public void testRenameBlobInSameDirectoryWithNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsStore().getClient();
    fs.create(new Path("/srcDir/dir/file"));
    client.deleteBlobPath(new Path("/srcDir/dir"), null,
        getTestTracingContext(fs, true));
    assertTrue(fs.rename(new Path("/srcDir/dir"), new Path("/srcDir")));
  }

  /**
   * <pre>
   * Test to check behaviour of rename API if the destination directory is already
   * there. The HNS call and the one for Blob endpoint should have same behaviour.
   *
   * /testDir2/test1/test2/test3 contains (/file)
   * There is another path that exists: /testDir2/test4/test3
   * On rename(/testDir2/test1/test2/test3, /testDir2/test4).
   * </pre>
   *
   * Expectation for HNS / Blob endpoint:<ol>
   * <li>Rename should fail</li>
   * <li>No file should be transferred to destination directory</li>
   * </ol>
   */
  @Test
  public void testPosixRenameDirectoryWhereDirectoryAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.mkdirs(new Path("testDir2/test4/test3"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if (getIsNamespaceEnabled(fs)
        || fs.getAbfsClient() instanceof AbfsBlobClient) {
      assertFalse(fs.exists(new Path("testDir2/test4/test3/file")));
      assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    } else {
      assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
      assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    }
  }

  /**
   * <pre>
   * Test to check behaviour of rename API if the destination directory is already
   * there. The HNS call and the one for Blob endpoint should have same behaviour.
   *
   * /testDir2/test1/test2/test3 contains (/file)
   * There is another path that exists: /testDir2/test4/test3
   * On rename(/testDir2/test1/test2/test3, /testDir2/test4).
   * </pre>
   *
   * Expectation for HNS / Blob endpoint:<ol>
   * <li>Rename should fail</li>
   * <li>No file should be transferred to destination directory</li>
   * </ol>
   */
  @Test
  public void testPosixRenameDirectoryWherePartAlreadyThereOnDestination()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.create(new Path("testDir2/test1/test2/test3/file"));
    fs.create(new Path("testDir2/test1/test2/test3/file1"));
    fs.mkdirs(new Path("testDir2/test4/"));
    fs.create(new Path("testDir2/test4/file1"));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
    assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
    assertFalse(fs.exists(new Path("testDir2/test4/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3/file1")));
    assertTrue(fs.exists(new Path("testDir2/test4/file1")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3/file1")));
  }

  /**
   * Test that after completing rename for a directory which is enabled for
   * AtomicRename, the RenamePending JSON file is deleted.
   */
  @Test
  public void testRenamePendingJsonIsRemovedPostSuccessfulRename()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;
    Mockito.doAnswer(answer -> {
          final String correctDeletePath = "/hbase/test1/test2/test3" + SUFFIX;
          if (correctDeletePath.equals(
              ((Path) answer.getArgument(0)).toUri().getPath())) {
            correctDeletePathCount[0] = 1;
          }
          return null;
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    assertTrue(fs.rename(new Path("hbase/test1/test2/test3"),
        new Path("hbase/test4")));
    assertEquals("RenamePendingJson should be deleted",
        1,
        (int) correctDeletePathCount[0]);
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
   * A helper method to set up the test environment and execute the common logic for handling
   * failed rename operations and recovery in HBase. This method performs the necessary setup
   * (creating directories and files) and then triggers the `crashRenameAndRecover` method
   * with a provided recovery action.
   *
   * This method is used by different tests that require different recovery actions, such as
   * performing `listStatus` or checking the existence of a path after a failed rename.
   *
   * @param fs the AzureBlobFileSystem instance to be used in the test
   * @param client the AbfsBlobClient instance to be used in the test
   * @param srcPath the source path for the rename operation
   * @param failedCopyPath the path that simulates a failed copy during rename
   * @param recoveryAction the specific recovery action to be performed after the rename failure
   *                       (e.g., listing directory status or checking path existence)
   * @throws Exception if any error occurs during setup or execution of the recovery action
   */
  private void setupAndTestHBaseFailedRenameRecovery(
      final AzureBlobFileSystem fs,
      final AbfsBlobClient client,
      final String srcPath,
      final String failedCopyPath,
      final FunctionRaisingIOE<AzureBlobFileSystem, Void> recoveryAction)
      throws Exception {
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcPath));
    fs.mkdirs(new Path(srcPath, "test3"));
    fs.create(new Path(srcPath + "/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    crashRenameAndRecover(fs, client, srcPath, recoveryAction);
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 403 on a copy of one of the blob.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameWithListRecovery()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";

    setupAndTestHBaseFailedRenameRecovery(fs, client, srcPath, failedCopyPath,
        (abfsFs) -> {
          abfsFs.listStatus(new Path(srcPath).getParent());
          return null;
        });
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 403 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * GetFileStatus API will be called on the directory. Expectation is that the
   * GetFileStatus API of {@link AzureBlobFileSystem} should recover the paused
   * rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameWithGetFileStatusRecovery()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";

    setupAndTestHBaseFailedRenameRecovery(fs, client, srcPath, failedCopyPath,
        (abfsFs) -> {
          abfsFs.exists(new Path(srcPath));
          return null;
        });
  }


  /**
   * Simulates a rename failure, performs a recovery action, and verifies that the "RenamePendingJson"
   * file is deleted. It checks that the rename operation is successfully completed after recovery.
   *
   * @param fs the AzureBlobFileSystem instance
   * @param client the AbfsBlobClient instance
   * @param srcPath the source path for the rename operation
   * @param recoveryCallable the recovery action to perform
   * @throws Exception if an error occurs during recovery or verification
   */
  private void crashRenameAndRecover(final AzureBlobFileSystem fs,
      AbfsBlobClient client,
      final String srcPath,
      final FunctionRaisingIOE<AzureBlobFileSystem, Void> recoveryCallable)
      throws Exception {
    crashRename(fs, client, srcPath);
    AzureBlobFileSystem fs2 = Mockito.spy(getFileSystem());
    fs2.setWorkingDirectory(new Path(ROOT_PATH));
    client = (AbfsBlobClient) addSpyHooksOnClient(fs2);
    int[] renameJsonDeleteCounter = new int[1];
    Mockito.doAnswer(answer -> {
          if ((ROOT_PATH + srcPath + SUFFIX)
              .equalsIgnoreCase(((Path) answer.getArgument(0)).toUri().getPath())) {
            renameJsonDeleteCounter[0] = 1;
          }
          return answer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    recoveryCallable.apply(fs2);
    Assertions.assertThat(renameJsonDeleteCounter[0])
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);
    //List would complete the rename orchestration.
    assertFalse(fs2.exists(new Path("hbase/test1/test2")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3/file")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3/file")));
    assertFalse(fs2.exists(new Path("hbase/test1/test2/test3/file1")));
    assertTrue(fs2.exists(new Path("hbase/test4/test2/test3/file1")));
  }

  /**
   * Simulates a rename failure by triggering an `AbfsRestOperationException` during the rename process.
   * It intercepts the exception and ensures that all leases acquired during the atomic rename are released.
   *
   * @param fs the AzureBlobFileSystem instance used for the rename operation
   * @param client the AbfsBlobClient instance used for mocking the rename failure
   * @param srcPath the source path for the rename operation
   * @throws Exception if an error occurs during the simulated failure or lease release
   */
  private void crashRename(final AzureBlobFileSystem fs,
      final AbfsBlobClient client,
      final String srcPath) throws Exception {
    BlobRenameHandler[] blobRenameHandlers = new BlobRenameHandler[1];
    AbfsClientTestUtil.mockGetRenameBlobHandler(client,
        blobRenameHandler -> {
          blobRenameHandlers[0] = blobRenameHandler;
          return null;
        });
    //Fail rename orchestration on path hbase/test1/test2/test3/file1
    Mockito.doThrow(new AbfsRestOperationException(HTTP_FORBIDDEN, "", "",
            new Exception()))
        .when(client)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    LambdaTestUtils.intercept(AccessDeniedException.class, () -> {
      fs.rename(new Path(srcPath),
          new Path("hbase/test4"));
    });
    //Release all the leases taken by atomic rename orchestration
    List<AbfsLease> leases = new ArrayList<>(blobRenameHandlers[0].getLeases());
    for (AbfsLease lease : leases) {
      lease.free();
    }
  }

  /**
   * Simulates a scenario where HMaster in Hbase starts up and executes listStatus
   * API on the directory that has to be renamed by some other executor-machine.
   * The scenario is that RenamePending JSON is created but before it could be
   * appended, it has been opened by the HMaster. The HMaster will delete it. The
   * machine doing rename would have to recreate the JSON file.
   * ref: <a href="https://issues.apache.org/jira/browse/HADOOP-12678">issue</a>
   */
  @Test
  public void testHbaseListStatusBeforeRenamePendingFileAppendedWithIngressOnBlob()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    testRenamePreRenameFailureResolution(fs);
    testAtomicityRedoInvalidFile(fs);
  }

  /**
   * Tests renaming a directory in AzureBlobFileSystem when the creation of the "RenamePendingJson"
   * file fails on the first attempt. It ensures the renaming operation is retried.
   *
   * The test verifies that the creation of the "RenamePendingJson" file is attempted twice:
   * once on failure and once on retry.
   *
   * @param fs the AzureBlobFileSystem instance for the test
   * @throws Exception if an error occurs during the test
   */
  private void testRenamePreRenameFailureResolution(final AzureBlobFileSystem fs)
      throws Exception {
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path src = new Path("hbase/test1/test2");
    Path dest = new Path("hbase/test4");
    fs.mkdirs(src);
    fs.mkdirs(new Path(src, "test3"));
    final int[] renamePendingJsonWriteCounter = new int[1];
    /*
     * Fail the creation of RenamePendingJson file on the first attempt.
     */
    Answer renamePendingJsonCreateAns = createAnswer -> {
      Path path = createAnswer.getArgument(0);
      Mockito.doAnswer(clientFlushAns -> {
            if (renamePendingJsonWriteCounter[0]++ == 0) {
              fs.delete(path, true);
            }
            return clientFlushAns.callRealMethod();
          })
          .when(client)
          .flush(Mockito.any(byte[].class), Mockito.anyString(),
              Mockito.anyBoolean(), Mockito.nullable(String.class),
              Mockito.nullable(String.class), Mockito.anyString(),
              Mockito.nullable(ContextEncryptionAdapter.class),
              Mockito.any(TracingContext.class));
      return createAnswer.callRealMethod();
    };
    RenameAtomicityTestUtils.addCreatePathMock(client,
        renamePendingJsonCreateAns);
    fs.rename(src, dest);
    Assertions.assertThat(renamePendingJsonWriteCounter[0])
        .describedAs("Creation of RenamePendingJson should be attempted twice")
        .isEqualTo(2);
  }

  /**
   * Tests the behavior of the redo operation when an invalid "RenamePendingJson" file exists.
   * It verifies that the file is deleted and that no copy operation is performed.
   *
   * The test simulates a scenario where the "RenamePendingJson" file is partially written and
   * ensures that the `redo` method correctly deletes the file and does not trigger a copy operation.
   *
   * @param fs the AzureBlobFileSystem instance for the test
   * @throws Exception if an error occurs during the test
   */
  private void testAtomicityRedoInvalidFile(final AzureBlobFileSystem fs)
      throws Exception {
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{".getBytes(StandardCharsets.UTF_8));
    os.close();
    int[] renameJsonDeleteCounter = new int[1];
    Mockito.doAnswer(deleteAnswer -> {
          Path ansPath = deleteAnswer.getArgument(0);
          if (renameJson.toUri()
              .getPath()
              .equalsIgnoreCase(ansPath.toUri().getPath())) {
            renameJsonDeleteCounter[0]++;
          }
          return deleteAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    new RenameAtomicity(renameJson, 1,
        getTestTracingContext(fs, true), null, client).redo();
    Assertions.assertThat(renameJsonDeleteCounter[0])
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);
    Mockito.verify(client, Mockito.times(0)).copyBlob(Mockito.any(Path.class),
        Mockito.any(Path.class), Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));
  }

  /**
   * Test to check the atomicity of rename operation. The rename operation should
   * be atomic and should not leave any intermediate state.
   */
  @Test
  public void testRenameJsonDeletedBeforeRenameAtomicityCanDelete()
      throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{}".getBytes(StandardCharsets.UTF_8));
    os.close();
    int[] renameJsonDeleteCounter = new int[1];
    Mockito.doAnswer(deleteAnswer -> {
          Path ansPath = deleteAnswer.getArgument(0);
          if (renameJson.toUri()
              .getPath()
              .equalsIgnoreCase(ansPath.toUri().getPath())) {
            renameJsonDeleteCounter[0]++;
          }
          getFileSystem().delete(ansPath, true);
          return deleteAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    new RenameAtomicity(renameJson, 2,
        getTestTracingContext(fs, true), null, client);
  }

  /**
   * Tests the scenario where the rename operation is complete before the redo
   * operation for atomicity, leading to a failure. This test verifies that the
   * system correctly handles the case when a rename operation is attempted after
   * the source path has already been deleted, which should result in an error.
   * <p>
   * The test simulates a situation where a `renameJson` file is created for the
   * rename operation, and the source path is deleted during the read process in
   * the redo operation. The `redoRenameAtomicity` is then executed, and it is
   * expected to fail with a `404` error, indicating that the source path no longer exists.
   * <p>
   * The test ensures that the system can handle this error condition and return
   * the correct response, preventing a potentially invalid or inconsistent state.
   *
   * @throws Exception If an error occurs during file system operations.
   */
  @Test
  public void testRenameCompleteBeforeRenameAtomicityRedo() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    /*
     * Create renameJson file.
     */
    AzureBlobFileSystemStore.VersionedFileStatus fileStatus
        = (AzureBlobFileSystemStore.VersionedFileStatus) fs.getFileStatus(path);
    int jsonLen = new RenameAtomicity(path,
        new Path("/hbase/test4"), renameJson,
        getTestTracingContext(fs, true), fileStatus.getEtag(),
        client).preRename();
    RenameAtomicity redoRenameAtomicity = Mockito.spy(
        new RenameAtomicity(renameJson, jsonLen,
            getTestTracingContext(fs, true), null, client));
    RenameAtomicityTestUtils.addReadPathMock(redoRenameAtomicity,
        readCallbackAnswer -> {
          byte[] bytes = (byte[]) readCallbackAnswer.callRealMethod();
          fs.delete(path, true);
          return bytes;
        });
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
        redoRenameAtomicity::redo);
    Assertions.assertThat(ex.getStatusCode())
        .describedAs("RenameAtomicity redo should fail with 404")
        .isEqualTo(SOURCE_PATH_NOT_FOUND.getStatusCode());
    Assertions.assertThat(ex.getErrorCode())
        .describedAs("RenameAtomicity redo should fail with 404")
        .isEqualTo(SOURCE_PATH_NOT_FOUND);
  }

  /**
   * Tests the idempotency of the copyBlob operation during a rename when the
   * destination already exists. This test simulates a scenario where the source
   * blob is copied to the destination before the actual rename operation is invoked.
   * It ensures that the copyBlob operation can handle idempotency issues and perform
   * the rename successfully even when the destination is pre-created.
   * <p>
   * The test verifies that the rename operation successfully copies the blob from
   * the source to the destination, and the source is deleted, leaving only the
   * destination file. This ensures that the system behaves correctly in scenarios
   * where the destination path already contains the blob.
   *
   * @throws Exception If an error occurs during file system operations.
   */
  @Test
  public void testCopyBlobIdempotency() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path src = new Path("/srcDir/src");
    Path dst = new Path("/dst");
    fs.create(src);
    Mockito.doAnswer(answer -> {
      Path srcCopy = answer.getArgument(0);
      Path dstCopy = answer.getArgument(1);
      String leaseId = answer.getArgument(2);
      TracingContext tracingContext = answer.getArgument(3);
      /*
       * To fail copyBlob with idempotency issue, making a copy of the source to destination
       * before the invoked copy
       */
      ((AbfsBlobClient) getFileSystem().getAbfsClient()).copyBlob(srcCopy,
          dstCopy, leaseId, tracingContext);
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));
    Assertions.assertThat(fs.rename(src, dst))
        .describedAs("Rename should be successful and copyBlob should"
            + "be able to handle idempotency issue")
        .isTrue();
    Assertions.assertThat(fs.exists(src))
        .describedAs("Source should not exist after rename")
        .isFalse();
    Assertions.assertThat(fs.exists(dst))
        .describedAs("Destination should exist after rename")
        .isTrue();
  }

  /**
   * Tests the idempotency of the rename operation when the destination path is
   * created by some other process before the rename operation. This test simulates
   * the scenario where a source blob is renamed, and the destination path already
   * exists due to actions from another process. It ensures that the rename operation
   * behaves idempotently and correctly handles the case where the destination is
   * pre-created.
   * <p>
   * The test verifies that the rename operation fails (since the destination already
   * exists), but the source path remains intact, and the blob copy operation is able
   * to handle the idempotency issue.
   *
   * @throws IOException If an error occurs during file system operations.
   */
  @Test
  public void testRenameBlobIdempotencyWhereDstIsCreatedFromSomeOtherProcess()
      throws IOException {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path src = new Path("/src");
    Path dst = new Path("/dst");
    fs.create(src);
    Mockito.doAnswer(answer -> {
      Path dstCopy = answer.getArgument(1);
      fs.create(dstCopy);
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class),
        Mockito.any(TracingContext.class));
    Assertions.assertThat(fs.rename(src, dst))
        .describedAs("Rename should be successful and copyBlob should"
            + "be able to handle idempotency issue")
        .isFalse();
    Assertions.assertThat(fs.exists(src))
        .describedAs("Source should exist after rename failure")
        .isTrue();
  }

  /**
   * Tests renaming a directory when the destination directory is missing a marker blob.
   * This test involves creating multiple directories and files, deleting a blob (marker) in the
   * destination directory, and renaming the source directory to the destination.
   * It then verifies that the renamed directory exists at the expected destination path.
   *
   * @throws Exception If an error occurs during the file system operations or assertions.
   */
  @Test
  public void testRenameDirWhenMarkerBlobIsAbsentOnDstDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.mkdirs(new Path("/test1"));
    fs.mkdirs(new Path("/test1/test2"));
    fs.mkdirs(new Path("/test1/test2/test3"));
    fs.create(new Path("/test1/test2/test3/file"));
    ((AbfsBlobClient) fs.getAbfsClient())
        .deleteBlobPath(new Path("/test1/test2"),
            null, getTestTracingContext(fs, true));
    fs.mkdirs(new Path("/test4/test5"));
    fs.rename(new Path("/test4"), new Path("/test1/test2"));
    assertTrue(fs.exists(new Path("/test1/test2/test4/test5")));
  }

  /**
   * Tests the renaming of a directory when the source directory does not have a marker file.
   * This test creates a file within a source directory, deletes the source directory from the blob storage,
   * creates a new target directory, and renames the source directory to the target location.
   * It verifies that the renamed source directory exists in the target path.
   *
   * @throws Exception If an error occurs during the file system operations or assertions.
   */
  @Test
  public void testBlobRenameSrcDirHasNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.create(new Path("/test1/test2/file1"));
    ((AbfsBlobClient) fs.getAbfsStore().getClient())
        .deleteBlobPath(new Path("/test1"), null,
            getTestTracingContext(fs, true));
    fs.mkdirs(new Path("/test2"));
    fs.rename(new Path("/test1"), new Path("/test2"));
    assertTrue(fs.exists(new Path("/test2/test1")));
  }

  /**
   * Mocks the progress status for a copy blob operation.
   * This method simulates a copy operation that is pending and not yet completed.
   * It intercepts the `copyBlob` method and modifies its response to return a "COPY_STATUS_PENDING"
   * status for the copy operation.
   *
   * @param spiedClient The {@link AbfsBlobClient} instance that is being spied on.
   * @throws AzureBlobFileSystemException if the mock setup fails.
   */
  private void addMockForProgressStatusOnCopyOperation(final AbfsBlobClient spiedClient)
      throws AzureBlobFileSystemException {
    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = Mockito.spy(
              (AbfsRestOperation) answer.callRealMethod());
          AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
          Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
              HttpHeaderConfigurations.X_MS_COPY_STATUS);
          Mockito.doReturn(httpOp).when(op).getResult();
          return op;
        })
        .when(spiedClient)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
  }

  /**
   * Verifies the behavior of a blob copy operation that takes time to complete.
   * The test ensures the following:
   * <ul>
   *   <li>A file is created and a rename operation is initiated.</li>
   *   <li>The copy operation progress is mocked to simulate a time-consuming process.</li>
   *   <li>The rename operation triggers a call to handle the copy progress.</li>
   *   <li>The test checks that the file exists after the rename and that the
   *       `handleCopyInProgress` method is called exactly once.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
        fileSystem);
    addMockForProgressStatusOnCopyOperation(spiedClient);
    fileSystem.create(new Path("/test1/file"));
    BlobRenameHandler[] blobRenameHandlers = new BlobRenameHandler[1];
    AbfsClientTestUtil.mockGetRenameBlobHandler(spiedClient,
        blobRenameHandler -> {
          blobRenameHandlers[0] = blobRenameHandler;
          return null;
        });
    fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(blobRenameHandlers[0], Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  /**
   * Mocks the final status of a blob copy operation.
   * This method ensures that when checking the status of a copy operation in progress,
   * it returns the specified final status (e.g., success, failure, aborted).
   *
   * @param spiedClient The mocked Azure Blob client to apply the mock behavior.
   * @param requiredCopyFinalStatus The final status of the copy operation to be returned
   *                                (e.g., COPY_STATUS_FAILED, COPY_STATUS_ABORTED).
   */
  private void addMockForCopyOperationFinalStatus(final AbfsBlobClient spiedClient,
      final String requiredCopyFinalStatus) {
    AbfsClientTestUtil.mockGetRenameBlobHandler(spiedClient,
        blobRenameHandler -> {
          Mockito.doAnswer(onHandleCopyInProgress -> {
                Path handlePath = onHandleCopyInProgress.getArgument(0);
                TracingContext tracingContext = onHandleCopyInProgress.getArgument(
                    1);
                Mockito.doAnswer(onStatusCheck -> {
                      AbfsRestOperation op = Mockito.spy(
                          (AbfsRestOperation) onStatusCheck.callRealMethod());
                      AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
                      Mockito.doReturn(requiredCopyFinalStatus)
                          .when(httpOp)
                          .getResponseHeader(
                              HttpHeaderConfigurations.X_MS_COPY_STATUS);
                      Mockito.doReturn(httpOp).when(op).getResult();
                      return op;
                    })
                    .when(spiedClient)
                    .getPathStatus(handlePath.toUri().getPath(),
                        tracingContext, null, false);
                return onHandleCopyInProgress.callRealMethod();
              })
              .when(blobRenameHandler)
              .handleCopyInProgress(Mockito.any(Path.class),
                  Mockito.any(TracingContext.class), Mockito.any(String.class));
          return null;
        });
  }

  /**
   * Verifies the behavior when a blob copy operation takes time and eventually fails.
   * The test ensures the following:
   * <ul>
   *   <li>A file is created and a copy operation is initiated.</li>
   *   <li>The copy operation is mocked to eventually fail.</li>
   *   <li>The rename operation triggers an exception due to the failed copy.</li>
   *   <li>The test checks that the appropriate 'COPY_FAILED' error code and status code are returned.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testCopyBlobTakeTimeAndEventuallyFail() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
        fileSystem);
    addMockForProgressStatusOnCopyOperation(spiedClient);
    fileSystem.create(new Path("/test1/file"));
    addMockForCopyOperationFinalStatus(spiedClient, COPY_STATUS_FAILED);
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
        () -> {
          fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
        });
    Assertions.assertThat(ex.getStatusCode())
        .describedAs("Expecting COPY_FAILED status code")
        .isEqualTo(COPY_BLOB_FAILED.getStatusCode());
    Assertions.assertThat(ex.getErrorCode())
        .describedAs("Expecting COPY_FAILED error code")
        .isEqualTo(COPY_BLOB_FAILED);
  }

  /**
   * Verifies the behavior when a blob copy operation takes time and is eventually aborted.
   * The test ensures the following:
   * <ul>
   *   <li>A file is created and a copy operation is initiated.</li>
   *   <li>The copy operation is mocked to eventually be aborted.</li>
   *   <li>The rename operation triggers an exception due to the aborted copy.</li>
   *   <li>The test checks that the appropriate 'COPY_ABORTED' error code and status code are returned.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testCopyBlobTakeTimeAndEventuallyAborted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
        fileSystem);
    addMockForProgressStatusOnCopyOperation(spiedClient);
    fileSystem.create(new Path("/test1/file"));
    addMockForCopyOperationFinalStatus(spiedClient, COPY_STATUS_ABORTED);
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
        () -> {
          fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
        });
    Assertions.assertThat(ex.getStatusCode())
        .describedAs("Expecting COPY_ABORTED status code")
        .isEqualTo(COPY_BLOB_ABORTED.getStatusCode());
    Assertions.assertThat(ex.getErrorCode())
        .describedAs("Expecting COPY_ABORTED error code")
        .isEqualTo(COPY_BLOB_ABORTED);
  }

  /**
   * Verifies the behavior when a blob copy operation takes time and the destination blob
   * is deleted during the process. The test ensures the following:
   * <ul>
   *   <li>A source file is created and a copy operation is initiated.</li>
   *   <li>During the copy process, the destination file is deleted.</li>
   *   <li>The copy operation returns a pending status.</li>
   *   <li>The test checks that the destination file does not exist after the copy operation is interrupted.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testCopyBlobTakeTimeAndBlobIsDeleted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
        fileSystem);
    String srcFile = "/test1/file";
    String dstFile = "/test1/file2";
    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = Mockito.spy(
              (AbfsRestOperation) answer.callRealMethod());
          fileSystem.delete(new Path(dstFile), false);
          AbfsHttpOperation httpOp = Mockito.spy(op.getResult());
          Mockito.doReturn(COPY_STATUS_PENDING).when(httpOp).getResponseHeader(
              HttpHeaderConfigurations.X_MS_COPY_STATUS);
          Mockito.doReturn(httpOp).when(op).getResult();
          return op;
        })
        .when(spiedClient)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    fileSystem.create(new Path(srcFile));
    assertFalse(fileSystem.rename(new Path(srcFile), new Path(dstFile)));
    assertFalse(fileSystem.exists(new Path(dstFile)));
  }

  /**
   * Verifies the behavior when attempting to copy a blob after the source has been deleted
   * in the Azure Blob FileSystem. The test ensures the following:
   * <ul>
   *   <li>A source blob is created and then deleted.</li>
   *   <li>An attempt to copy the deleted source blob results in a 'not found' error.</li>
   *   <li>The test checks that the correct HTTP error (404 Not Found) is returned when copying a non-existent source.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testCopyAfterSourceHasBeenDeleted() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    fs.create(new Path("/src"));
    TracingContext tracingContext = new TracingContext("clientCorrelationId",
        "fileSystemId", FSOperationType.TEST_OP,
        getConfiguration().getTracingHeaderFormat(),
        null);
    client.deleteBlobPath(new Path("/src"), null,
        getTestTracingContext(fs, true));
    Boolean srcBlobNotFoundExReceived = false;
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
        () -> {
          client.copyBlob(new Path("/src"), new Path("/dst"),
              null, getTestTracingContext(fs, true));
        });
    Assertions.assertThat(ex.getStatusCode())
        .describedAs("Source has to be not found at copy")
        .isEqualTo(HTTP_NOT_FOUND);
  }

  /**
   * Verifies that parallel rename operations in the Azure Blob FileSystem fail when
   * trying to perform an atomic rename with lease acquisition. The test ensures the following:
   * <ul>
   *   <li>A directory is created and a rename operation is attempted.</li>
   *   <li>A parallel thread attempts to rename the directory while the lease is being acquired.</li>
   *   <li>The parallel rename operation should fail due to a lease conflict, triggering an exception.</li>
   *   <li>The test verifies that the expected conflict exception is thrown when attempting a parallel rename.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testParallelRenameForAtomicRenameShouldFail() throws Exception {
    Configuration config = getRawConfiguration();
    config.set(FS_AZURE_LEASE_THREADS, "2");
    AzureBlobFileSystem fs = Mockito.spy(
        (AzureBlobFileSystem) FileSystem.newInstance(config));
    assumeBlobServiceType();
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path src = new Path("/hbase/src");
    Path dst = new Path("/hbase/dst");
    fs.mkdirs(src);
    AtomicBoolean leaseAcquired = new AtomicBoolean(false);
    AtomicBoolean exceptionOnParallelRename = new AtomicBoolean(false);
    AtomicBoolean parallelThreadDone = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
          AbfsRestOperation op = (AbfsRestOperation) answer.callRealMethod();
          leaseAcquired.set(true);
          while (!parallelThreadDone.get()) {}
          return op;
        })
        .when(client)
        .acquireLease(Mockito.anyString(), Mockito.anyInt(),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    new Thread(() -> {
      while (!leaseAcquired.get()) {}
      try {
        fs.rename(src, dst);
      } catch (Exception e) {
        if (e.getCause() instanceof AbfsLease.LeaseException
            && e.getCause().getCause() instanceof AbfsRestOperationException
            && ((AbfsRestOperationException) e.getCause()
            .getCause()).getStatusCode() == HTTP_CONFLICT) {
          exceptionOnParallelRename.set(true);
        }
      } finally {
        parallelThreadDone.set(true);
      }
    }).start();
    fs.rename(src, dst);
    while (!parallelThreadDone.get()) {}
    Assertions.assertThat(exceptionOnParallelRename.get())
        .describedAs("Parallel rename should fail")
        .isTrue();
  }

  /**
   * Verifies the behavior of appending data to a blob during a rename operation in the
   * Azure Blob FileSystem. The test ensures the following:
   * <ul>
   *   <li>A file is created and data is appended to it while a rename operation is in progress.</li>
   *   <li>The append operation should fail due to the rename operation in progress.</li>
   *   <li>The test checks that the append operation is properly interrupted and fails as expected.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testAppendAtomicBlobDuringRename() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path src = new Path("/hbase/src");
    Path dst = new Path("/hbase/dst");
    FSDataOutputStream os = fs.create(src);
    AtomicBoolean copyInProgress = new AtomicBoolean(false);
    AtomicBoolean outputStreamClosed = new AtomicBoolean(false);
    AtomicBoolean appendFailed = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
      copyInProgress.set(true);
      while (!outputStreamClosed.get()) {}
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    new Thread(() -> {
      while (!copyInProgress.get()) {}
      try {
        os.write(1);
        os.close();
      } catch (IOException e) {
        appendFailed.set(true);
      } finally {
        outputStreamClosed.set(true);
      }
    }).start();
    fs.rename(src, dst);
    Assertions.assertThat(appendFailed.get())
        .describedAs("Append should fail")
        .isTrue();
  }

  /**
   * Verifies the behavior of renaming a directory in the Azure Blob FileSystem when
   * there is a neighboring directory with the same prefix. The test ensures the following:
   * <ul>
   *   <li>Two directories with similar prefixes are created, along with files inside them.</li>
   *   <li>The rename operation moves one directory to a new location.</li>
   *   <li>Files in the renamed directory are moved, while files in the neighboring directory with the same prefix remain unaffected.</li>
   *   <li>Correct existence checks are performed to confirm the renamed directory and its files are moved, and the original directory is deleted.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testBlobRenameOfDirectoryHavingNeighborWithSamePrefix()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeBlobServiceType();
    fs.mkdirs(new Path("/testDir/dir"));
    fs.mkdirs(new Path("/testDir/dirSamePrefix"));
    fs.create(new Path("/testDir/dir/file1"));
    fs.create(new Path("/testDir/dir/file2"));
    fs.create(new Path("/testDir/dirSamePrefix/file1"));
    fs.create(new Path("/testDir/dirSamePrefix/file2"));
    fs.rename(new Path("/testDir/dir"), new Path("/testDir/dir2"));
    Assertions.assertThat(fs.exists(new Path("/testDir/dirSamePrefix/file1")))
        .isTrue();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/file1")))
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/file2")))
        .isFalse();
    Assertions.assertThat(fs.exists(new Path("/testDir/dir/")))
        .isFalse();
  }

  /**
   * Verifies the behavior of renaming a directory in the Azure Blob FileSystem when
   * the `listPath` operation returns paginated results with one object per list.
   * The test ensures the following:
   * <ul>
   *   <li>A directory and its files are created.</li>
   *   <li>The `listPath` operation is mocked to return one file at a time in each paginated result.</li>
   *   <li>The rename operation successfully moves the directory and its files to a new location.</li>
   *   <li>All files are verified to exist in the new location after the rename.</li>
   * </ul>
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testBlobRenameWithListGivingPaginatedResultWithOneObjectPerList()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.mkdirs(new Path("/testDir/dir1"));
    for (int i = 0; i < 10; i++) {
      fs.create(new Path("/testDir/dir1/file" + i));
    }
    Mockito.doAnswer(answer -> {
          String path = answer.getArgument(0);
          boolean recursive = answer.getArgument(1);
          String continuation = answer.getArgument(3);
          TracingContext context = answer.getArgument(4);
          return getFileSystem().getAbfsClient()
              .listPath(path, recursive, 1, continuation, context);
        })
        .when(spiedClient)
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    fs.rename(new Path("/testDir/dir1"), new Path("/testDir/dir2"));
    for (int i = 0; i < 10; i++) {
      Assertions.assertThat(fs.exists(new Path("/testDir/dir2/file" + i)))
          .describedAs("File " + i + " should exist in /testDir/dir2")
          .isTrue();
    }
  }

  /**
   * Verifies that the producer stops on a rename failure due to an access denial
   * (HTTP_FORBIDDEN error) in the Azure Blob FileSystem. The test ensures the following:
   * <ul>
   *   <li>Multiple file creation tasks are submitted concurrently.</li>
   *   <li>The rename operation is attempted but fails with an access denied exception.</li>
   *   <li>On failure, the list operation for the source directory is invoked at most twice.</li>
   * </ul>
   * The test simulates a failure scenario where the rename operation encounters an access
   * denied error, and the list operation should stop after the failure.
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testProducerStopOnRenameFailure() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    fs.mkdirs(new Path("/src"));
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < MAX_ITERATIONS; i++) {
      int iter = i;
      Future future = executorService.submit(() -> {
        try {
          fs.create(new Path("/src/file" + iter));
        } catch (IOException ex) {}
      });
      futureList.add(future);
    }
    for (Future future : futureList) {
      future.get();
    }
    AbfsBlobClient client = (AbfsBlobClient) fs.getAbfsClient();
    AbfsBlobClient spiedClient = Mockito.spy(client);
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    store.setClient(spiedClient);
    Mockito.doReturn(store).when(fs).getAbfsStore();
    final int[] copyCallInvocation = new int[1];
    Mockito.doAnswer(answer -> {
          throw new AbfsRestOperationException(HTTP_FORBIDDEN, "", "",
              new Exception());
        }).when(spiedClient)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    AbfsClientTestUtil.mockGetRenameBlobHandler(spiedClient,
        (blobRenameHandler) -> {
          Mockito.doAnswer(answer -> {
                try {
                  answer.callRealMethod();
                } catch (AbfsRestOperationException ex) {
                  if (ex.getStatusCode() == HTTP_FORBIDDEN) {
                    copyCallInvocation[0]++;
                  }
                  throw ex;
                }
                throw new AssertionError("List Consumption should have failed");
              })
              .when(blobRenameHandler).listRecursiveAndTakeAction();
          return null;
        });
    final int[] listCallInvocation = new int[1];
    Mockito.doAnswer(answer -> {
          if (answer.getArgument(0).equals("/src")) {
            if (listCallInvocation[0] == 1) {
              while (copyCallInvocation[0] == 0) {}
            }
            listCallInvocation[0]++;
            return getFileSystem().getAbfsClient().listPath(answer.getArgument(0),
                answer.getArgument(1), 1,
                answer.getArgument(3), answer.getArgument(4));
          }
          return answer.callRealMethod();
        })
        .when(spiedClient)
        .listPath(Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyInt(),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    intercept(AccessDeniedException.class,
        () -> {
          fs.rename(new Path("/src"), new Path("/dst"));
        });
    Assertions.assertThat(listCallInvocation[0])
        .describedAs("List on src should have been invoked at-most twice."
            + "One before consumption and the other after consumption has starting."
            + "Once consumption fails, listing would be stopped.")
        .isLessThanOrEqualTo(2);
  }

  /**
   * Verifies the behavior of renaming a directory through the Azure Blob FileSystem
   * when the source directory is deleted just before the rename operation is resumed.
   * It ensures that:
   * <ul>
   *   <li>No blobs are copied during the resume operation.</li>
   * </ul>
   * The test simulates a crash, deletes the source directory, and checks for the expected result.
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testRenameResumeThroughListStatusWithSrcDirDeletedJustBeforeResume()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path srcPath = new Path("hbase/test1/");
    Path failurePath = new Path(srcPath, "file");
    fs.mkdirs(srcPath);
    fs.create(failurePath);
    crashRename(fs, client, srcPath.toUri().getPath());
    fs.delete(srcPath, true);
    AtomicInteger copiedBlobs = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
      copiedBlobs.incrementAndGet();
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    fs.listStatus(new Path("hbase"));
    Assertions.assertThat(copiedBlobs.get())
        .describedAs("No Copy on resume")
        .isEqualTo(0);
  }

  /**
   * Verifies the behavior of renaming a directory through the Azure Blob FileSystem
   * when the source directory's ETag changes just before the rename operation is resumed.
   * It ensures that:
   * <ul>
   *   <li>No blobs are copied during the resume operation.</li>
   *   <li>The pending rename JSON file is deleted.</li>
   * </ul>
   * The test simulates a crash, retries the operation, and checks for the expected results.
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testRenameResumeThroughListStatusWithSrcDirETagChangedJustBeforeResume()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path srcPath = new Path("hbase/test1/");
    Path failurePath = new Path(srcPath, "file");
    fs.mkdirs(srcPath);
    fs.create(failurePath);
    crashRename(fs, client, srcPath.toUri().getPath()
    );
    fs.delete(srcPath, true);
    fs.mkdirs(srcPath);
    AtomicInteger copiedBlobs = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
      copiedBlobs.incrementAndGet();
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    AtomicInteger pendingJsonDeleted = new AtomicInteger(0);
    Mockito.doAnswer(listAnswer -> {
          Path path = listAnswer.getArgument(0);
          if (path.toUri().getPath().endsWith(SUFFIX)) {
            pendingJsonDeleted.incrementAndGet();
          }
          return listAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    fs.listStatus(new Path("/hbase"));
    Assertions.assertThat(copiedBlobs.get())
        .describedAs("No Copy on resume")
        .isEqualTo(0);
    Assertions.assertThat(pendingJsonDeleted.get())
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);
  }

  /**
   * Test case to verify the behavior of renaming a directory through the Azure Blob
   * FileSystem when the source directory's ETag changes just before the rename operation
   * is resumed. This test specifically checks the following:
   *
   * @throws Exception if any errors occur during the test execution
   */
  @Test
  public void testRenameResumeThroughGetStatusWithSrcDirETagChangedJustBeforeResume()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path srcPath = new Path("hbase/test1/");
    Path failurePath = new Path(srcPath, "file");
    fs.mkdirs(srcPath);
    fs.create(failurePath);
    crashRename(fs, client, srcPath.toUri().getPath()
    );
    fs.delete(srcPath, true);
    fs.mkdirs(srcPath);
    AtomicInteger copiedBlobs = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
      copiedBlobs.incrementAndGet();
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
        Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    AtomicInteger pendingJsonDeleted = new AtomicInteger(0);
    Mockito.doAnswer(listAnswer -> {
          Path path = listAnswer.getArgument(0);
          if (path.toUri().getPath().endsWith(SUFFIX)) {
            pendingJsonDeleted.incrementAndGet();
          }
          return listAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    Assertions.assertThat(fs.exists(srcPath))
        .describedAs("Source should exist")
        .isTrue();
    Assertions.assertThat(copiedBlobs.get())
        .describedAs("No Copy on resume")
        .isEqualTo(0);
    Assertions.assertThat(pendingJsonDeleted.get())
        .describedAs("RenamePendingJson should be deleted")
        .isEqualTo(1);
  }

  /**
   * Test to assert that the CID in src marker blob copy and delete contains the
   * total number of blobs operated in the rename directory.
   * Also, to assert that all operations in the rename-directory flow have same
   * primaryId and opType.
   */
  @Test
  public void testRenameSrcDirDeleteEmitDeletionCountInClientRequestId()
      throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeBlobServiceType();
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String dirPathStr = "/testDir/dir1";
    fs.mkdirs(new Path(dirPathStr));
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int iter = i;
      Future future = executorService.submit(() ->
          fs.create(new Path("/testDir/dir1/file" + iter)));
      futures.add(future);
    }
    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();
    final TracingHeaderValidator tracingHeaderValidator
        = new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.RENAME, true, 0);
    fs.registerListener(tracingHeaderValidator);
    Mockito.doAnswer(copyAnswer -> {
          if (dirPathStr.equalsIgnoreCase(
              ((Path) copyAnswer.getArgument(0)).toUri().getPath())) {
            tracingHeaderValidator.setOperatedBlobCount(BLOB_COUNT);
            return copyAnswer.callRealMethod();
          }
          return copyAnswer.callRealMethod();
        })
        .when(client)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    Mockito.doAnswer(deleteAnswer -> {
          if (dirPathStr.equalsIgnoreCase(
              ((Path) deleteAnswer.getArgument(0)).toUri().getPath())) {
            Object result = deleteAnswer.callRealMethod();
            tracingHeaderValidator.setOperatedBlobCount(null);
            return result;
          }
          return deleteAnswer.callRealMethod();
        })
        .when(client)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
    fs.rename(new Path(dirPathStr), new Path("/dst/"));
  }

  /**
   * Test to verify the idempotency of the `rename` operation in Azure Blob File System when retrying
   * after a failure. The test simulates a "path not found" error (HTTP 404) on the first attempt,
   * checks that the operation correctly retries using the appropriate transaction ID,
   * and ensures that the source file is renamed to the destination path once successful.
   *
   * @throws Exception if an error occurs during the file system operations or mocking
   */
  @Test
  public void testRenamePathRetryIdempotency() throws Exception {
    Configuration configuration = new Configuration(getRawConfiguration());
    configuration.set(FS_AZURE_ENABLE_CLIENT_TRANSACTION_ID, "true");
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(false);
      AbfsDfsClient abfsClient = (AbfsDfsClient) Mockito.spy(fs.getAbfsClient());
      fs.getAbfsStore().setClient(abfsClient);
      Path sourceDir = path("/testSrc");
      assertMkdirs(fs, sourceDir);
      String filename = "file1";
      Path sourceFilePath = new Path(sourceDir, filename);
      touch(sourceFilePath);
      Path destFilePath = new Path(sourceDir, "file2");

      final List<AbfsHttpHeader> headers = new ArrayList<>();
      mockRetriedRequest(abfsClient, headers);

      AbfsRestOperation getPathRestOp = Mockito.mock(AbfsRestOperation.class);
      AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
      Mockito.doAnswer(answer -> {
        String requiredHeader = null;
        for (AbfsHttpHeader httpHeader : headers) {
          if (X_MS_CLIENT_TRANSACTION_ID.equalsIgnoreCase(
              httpHeader.getName())) {
            requiredHeader = httpHeader.getValue();
            break;
          }
        }
        return requiredHeader;
      }).when(op).getResponseHeader(X_MS_CLIENT_TRANSACTION_ID);
      Mockito.doReturn(true).when(getPathRestOp).hasResult();
      Mockito.doReturn(op).when(getPathRestOp).getResult();
      Mockito.doReturn(DIRECTORY)
          .when(op)
          .getResponseHeader(X_MS_RESOURCE_TYPE);
      Mockito.doReturn(getPathRestOp).when(abfsClient).getPathStatus(
          Mockito.nullable(String.class), Mockito.nullable(Boolean.class),
          Mockito.nullable(TracingContext.class),
          Mockito.nullable(ContextEncryptionAdapter.class));
      fs.rename(sourceFilePath, destFilePath);
    }
  }

  /**
   * Test to verify that the client transaction ID is included in the response header
   * after renaming a file in Azure Blob Storage.
   *
   * This test ensures that when a file is renamed, the Azure Blob FileSystem client
   * properly includes the client transaction ID in the response header for the renamed file.
   * The test uses a configuration where client transaction ID is enabled and verifies
   * its presence after performing a rename operation.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testGetClientTransactionIdAfterRename() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(false);
      AbfsDfsClient abfsDfsClient = (AbfsDfsClient) Mockito.spy(fs.getAbfsClient());
      fs.getAbfsStore().setClient(abfsDfsClient);
      final String[] clientTransactionId = new String[1];
      mockAddClientTransactionIdToHeader(abfsDfsClient, clientTransactionId);
      Path sourceDir = path("/testSrc");
      assertMkdirs(fs, sourceDir);
      String filename = "file1";
      Path sourceFilePath = new Path(sourceDir, filename);
      touch(sourceFilePath);
      Path destFilePath = new Path(sourceDir, "file2");
      fs.rename(sourceFilePath, destFilePath);

      final AbfsHttpOperation getPathStatusOp =
          abfsDfsClient.getPathStatus(destFilePath.toUri().getPath(), false,
              getTestTracingContext(fs, true), null).getResult();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction id should be present in dest file")
          .isNotNull();
      Assertions.assertThat(
              getPathStatusOp.getResponseHeader(X_MS_CLIENT_TRANSACTION_ID))
          .describedAs("Client transaction ID should be equal to the one set in the header")
          .isEqualTo(clientTransactionId[0]);
    }
  }

  /**
   * Tests the recovery process during a file rename operation in Azure Blob File System when
   * the `getPathStatus` method encounters a timeout exception. The test ensures that the proper
   * error message is returned when the operation fails during recovery.
   *
   * @throws Exception If an error occurs during the test setup or execution.
   */
  @Test
  public void testFailureInGetPathStatusDuringRenameRecovery() throws Exception {
    try (AzureBlobFileSystem fs = getFileSystem()) {
      assumeRecoveryThroughClientTransactionID(false);
      AbfsDfsClient abfsDfsClient = (AbfsDfsClient) Mockito.spy(fs.getAbfsClient());
      fs.getAbfsStore().setClient(abfsDfsClient);
      final String[] clientTransactionId = new String[1];
      mockAddClientTransactionIdToHeader(abfsDfsClient, clientTransactionId);
      mockRetriedRequest(abfsDfsClient, new ArrayList<>());
      boolean[] flag = new boolean[1];
      Mockito.doAnswer(getPathStatus -> {
        if (!flag[0]) {
          flag[0] = true;
          throw new AbfsRestOperationException(HTTP_CLIENT_TIMEOUT, "", "", new Exception());
        }
        return getPathStatus.callRealMethod();
      }).when(abfsDfsClient).getPathStatus(
          Mockito.nullable(String.class), Mockito.nullable(Boolean.class),
          Mockito.nullable(TracingContext.class),
          Mockito.nullable(ContextEncryptionAdapter.class));

      Path sourceDir = path("/testSrc");
      assertMkdirs(fs, sourceDir);
      String filename = "file1";
      Path sourceFilePath = new Path(sourceDir, filename);
      touch(sourceFilePath);
      Path destFilePath = new Path(sourceDir, "file2");

      String errorMessage = intercept(AbfsDriverException.class,
          () -> fs.rename(sourceFilePath, destFilePath)).getErrorMessage();

      Assertions.assertThat(errorMessage)
          .describedAs("getPathStatus should fail while recovering")
          .contains(ERR_RENAME_RECOVERY);
    }
  }

  /**
   * Mocks the retry behavior for an AbfsDfsClient request. The method intercepts
   * the Abfs operation and simulates an HTTP conflict (HTTP 404) error on the
   * first invocation. It creates a mock HTTP operation with a PUT method and
   * specific status codes and error messages.
   *
   * @param abfsDfsClient The AbfsDfsClient to mock operations for.
   * @param headers The list of HTTP headers to which request headers will be added.
   *
   * @throws Exception If an error occurs during mock creation or operation execution.
   */
   private void mockRetriedRequest(AbfsDfsClient abfsDfsClient,
       final List<AbfsHttpHeader> headers) throws Exception {
     TestAbfsClient.mockAbfsOperationCreation(abfsDfsClient,
         new MockIntercept<AbfsRestOperation>() {
           private int count = 0;

           @Override
           public void answer(final AbfsRestOperation mockedObj,
               final InvocationOnMock answer)
               throws AbfsRestOperationException {
             if (count == 0) {
               count = 1;
               AbfsHttpOperation op = Mockito.mock(AbfsHttpOperation.class);
               Mockito.doReturn(HTTP_METHOD_PUT).when(op).getMethod();
               Mockito.doReturn(EMPTY_STRING).when(op).getStorageErrorMessage();
               Mockito.doReturn(SOURCE_PATH_NOT_FOUND.getErrorCode()).when(op)
                   .getStorageErrorCode();
               Mockito.doReturn(true).when(mockedObj).hasResult();
               Mockito.doReturn(op).when(mockedObj).getResult();
               Mockito.doReturn(HTTP_NOT_FOUND).when(op).getStatusCode();
               headers.addAll(mockedObj.getRequestHeaders());
               throw new AbfsRestOperationException(HTTP_NOT_FOUND,
                   SOURCE_PATH_NOT_FOUND.getErrorCode(), EMPTY_STRING, null, op);
             }
           }
         });
   }
}
