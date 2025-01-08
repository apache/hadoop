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

import org.apache.hadoop.fs.FileSystem;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.security.ContextEncryptionAdapter;
import org.apache.hadoop.fs.azurebfs.services.AbfsBlobClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsDfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsLease;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.BlobRenameHandler;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicity;
import org.apache.hadoop.fs.azurebfs.services.RenameAtomicityTestUtils;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.apache.hadoop.fs.statistics.IOStatisticAssertions;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.functional.FunctionRaisingIOE;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.RENAME_PATH_ATTEMPTS;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.ROOT_PATH;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_LEASE_THREADS;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.SOURCE_PATH_NOT_FOUND;
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
    Assert.assertTrue(fs.rename(new Path(testDir2 + "/test1/test2/test3"), new Path(testDir2 + "/test4")));
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

  @Test
  public void testRenameToRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src1/src2"));
    assertTrue(fs.rename(new Path("/src1/src2"), new Path("/")));
    assertTrue(fs.exists(new Path("/src2")));
  }

  @Test
  public void testRenameNotFoundBlobToEmptyRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assertFalse(fs.rename(new Path("/file"), new Path("/")));
  }

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    assertTrue(fs.getAbfsStore().getClient() instanceof AbfsBlobClient);
  }

  @Test(expected = IOException.class)
  public void testRenameBlobToDstWithColonInPath() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/src"));
    fs.rename(new Path("/src"), new Path("/dst:file"));
  }

  @Test
  public void testRenameBlobInSameDirectoryWithNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
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
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"),
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
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    assumeNonHnsAccountBlobEndpoint(fs);
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
    assertTrue("RenamePendingJson should be deleted",
            correctDeletePathCount[0] == 1);
  }
  private AbfsClient addSpyHooksOnClient(final AzureBlobFileSystem fs) {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    Mockito.doReturn(store).when(fs).getAbfsStore();
    AbfsClient client = Mockito.spy(store.getClient());
    Mockito.doReturn(client).when(store).getClient();
    return client;
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
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcPath));
    fs.mkdirs(new Path(srcPath, "test3"));
    fs.create(new Path(srcPath + "/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    crashRenameAndRecover(fs, client, srcPath, (abfsFs) -> {
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
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String srcPath = "hbase/test1/test2";
    final String failedCopyPath = srcPath + "/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcPath));
    fs.mkdirs(new Path(srcPath, "test3"));
    fs.create(new Path(srcPath + "/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    crashRenameAndRecover(fs, client, srcPath, (abfsFs) -> {
      abfsFs.exists(new Path(srcPath));
      return null;
    });
  }
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
    renameJsonDeleteCounter[0] = 0;
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
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    testRenamePreRenameFailureResolution(fs);
    testAtomicityRedoInvalidFile(fs);
  }
  private void testRenamePreRenameFailureResolution(final AzureBlobFileSystem fs)
          throws Exception {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
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
  private void testAtomicityRedoInvalidFile(final AzureBlobFileSystem fs)
          throws Exception {
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{".getBytes(StandardCharsets.UTF_8));
    os.close();
    int[] renameJsonDeleteCounter = new int[1];
    renameJsonDeleteCounter[0] = 0;
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
            getTestTracingContext(fs, true), null, client, null).redo();
    Assertions.assertThat(renameJsonDeleteCounter[0])
            .describedAs("RenamePendingJson should be deleted")
            .isEqualTo(1);
    Mockito.verify(client, Mockito.times(0)).copyBlob(Mockito.any(Path.class),
            Mockito.any(Path.class), Mockito.nullable(String.class),
            Mockito.any(TracingContext.class));
  }
  @Test
  public void testRenameJsonDeletedBeforeRenameAtomicityCanDelete()
          throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    fs.setWorkingDirectory(new Path(ROOT_PATH));
    Path path = new Path("/hbase/test1/test2");
    fs.mkdirs(new Path(path, "test3"));
    Path renameJson = new Path(path.getParent(), path.getName() + SUFFIX);
    OutputStream os = fs.create(renameJson);
    os.write("{}".getBytes(StandardCharsets.UTF_8));
    os.close();
    int[] renameJsonDeleteCounter = new int[1];
    renameJsonDeleteCounter[0] = 0;
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
            getTestTracingContext(fs, true), null, client, null);
  }
  @Test
  public void testRenameCompleteBeforeRenameAtomicityRedo() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
            getTestTracingContext(fs, true), fileStatus.getEtag(), client).preRename();
    RenameAtomicity redoRenameAtomicity = Mockito.spy(
            new RenameAtomicity(renameJson, jsonLen,
                    getTestTracingContext(fs, true), null, client, null));
    RenameAtomicityTestUtils.addReadPathMock(redoRenameAtomicity,
            readCallbackAnswer -> {
              byte[] bytes = (byte[]) readCallbackAnswer.callRealMethod();
              fs.delete(path, true);
              return bytes;
            });
    AbfsRestOperationException ex = intercept(AbfsRestOperationException.class,
            () -> {
              redoRenameAtomicity.redo();
            });
    Assertions.assertThat(ex.getStatusCode())
            .describedAs("RenameAtomicity redo should fail with 404")
            .isEqualTo(SOURCE_PATH_NOT_FOUND.getStatusCode());
    Assertions.assertThat(ex.getErrorCode())
            .describedAs("RenameAtomicity redo should fail with 404")
            .isEqualTo(SOURCE_PATH_NOT_FOUND);
  }
  @Test
  public void testCopyBlobIdempotency() throws Exception {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testRenameBlobIdempotencyWhereDstIsCreatedFromSomeOtherProcess()
          throws IOException {
    final AzureBlobFileSystem fs = Mockito.spy(this.getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testRenameDirWhenMarkerBlobIsAbsentOnDstDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testBlobRenameSrcDirHasNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/test1/test2/file1"));
    ((AbfsBlobClient) fs.getAbfsStore().getClient())
            .deleteBlobPath(new Path("/test1"), null,
                    getTestTracingContext(fs, true));
    fs.mkdirs(new Path("/test2"));
    fs.rename(new Path("/test1"), new Path("/test2"));
    assertTrue(fs.exists(new Path("/test2/test1")));
  }
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
  @Test
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
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
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(blobRenameHandlers[0], Mockito.times(1))
            .handleCopyInProgress(Mockito.any(Path.class),
                    Mockito.any(TracingContext.class), Mockito.any(String.class));
  }
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
  @Test
  public void testCopyBlobTakeTimeAndEventuallyFail() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
            fileSystem);
    addMockForProgressStatusOnCopyOperation(spiedClient);
    fileSystem.create(new Path("/test1/file"));
    final String requiredCopyFinalStatus = COPY_STATUS_FAILED;
    addMockForCopyOperationFinalStatus(spiedClient, requiredCopyFinalStatus);
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
  @Test
  public void testCopyBlobTakeTimeAndEventuallyAborted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AbfsBlobClient spiedClient = (AbfsBlobClient) addSpyHooksOnClient(
            fileSystem);
    addMockForProgressStatusOnCopyOperation(spiedClient);
    fileSystem.create(new Path("/test1/file"));
    final String requiredCopyFinalStatus = COPY_STATUS_ABORTED;
    addMockForCopyOperationFinalStatus(spiedClient, requiredCopyFinalStatus);
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
  @Test
  public void testCopyBlobTakeTimeAndBlobIsDeleted() throws Exception {
    AzureBlobFileSystem fileSystem = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fileSystem);
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
  @Test
  public void testCopyAfterSourceHasBeenDeleted() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testParallelRenameForAtomicRenameShouldFail() throws Exception {
    Configuration config = getRawConfiguration();
    config.set(FS_AZURE_LEASE_THREADS, "2");
    AzureBlobFileSystem fs = Mockito.spy(
            (AzureBlobFileSystem) FileSystem.newInstance(config));
    assumeNonHnsAccountBlobEndpoint(fs);
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
              while (!parallelThreadDone.get()) ;
              return op;
            })
            .when(client)
            .acquireLease(Mockito.anyString(), Mockito.anyInt(),
                    Mockito.nullable(String.class),
                    Mockito.any(TracingContext.class));
    new Thread(() -> {
      while (!leaseAcquired.get()) ;
      try {
        fs.rename(src, dst);
      } catch (Exception e) {
        if (e.getCause() instanceof AbfsLease.LeaseException
                && e.getCause().getCause() instanceof AbfsRestOperationException &&
                ((AbfsRestOperationException) e.getCause()
                        .getCause()).getStatusCode() == HTTP_CONFLICT) {
          exceptionOnParallelRename.set(true);
        }
      } finally {
        parallelThreadDone.set(true);
      }
    }).start();
    fs.rename(src, dst);
    while (!parallelThreadDone.get()) ;
    Assertions.assertThat(exceptionOnParallelRename.get())
            .describedAs("Parallel rename should fail")
            .isTrue();
  }
  @Test
  public void testAppendAtomicBlobDuringRename() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    Path src = new Path("/hbase/src");
    Path dst = new Path("/hbase/dst");
    FSDataOutputStream os = fs.create(src);
    AtomicBoolean copyInProgress = new AtomicBoolean(false);
    AtomicBoolean outputStreamClosed = new AtomicBoolean(false);
    AtomicBoolean appendFailed = new AtomicBoolean(false);
    Mockito.doAnswer(answer -> {
      copyInProgress.set(true);
      while (!outputStreamClosed.get()) ;
      return answer.callRealMethod();
    }).when(client).copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.nullable(String.class), Mockito.any(TracingContext.class));
    new Thread(() -> {
      while (!copyInProgress.get()) ;
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
  @Test
  public void testBlobRenameOfDirectoryHavingNeighborWithSamePrefix()
          throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testBlobRenameWithListGivingPaginatedResultWithOneObjectPerList()
          throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
   * Assert that Rename operation failure should stop List producer.
   */
  @Test
  public void testProducerStopOnRenameFailure() throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.mkdirs(new Path("/src"));
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
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
    copyCallInvocation[0] = 0;
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
    listCallInvocation[0] = 0;
    Mockito.doAnswer(answer -> {
              if (answer.getArgument(0).equals("/src")) {
                if (listCallInvocation[0] == 1) {
                  while (copyCallInvocation[0] == 0) ;
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
  @Test
  public void testRenameResumeThroughListStatusWithSrcDirDeletedJustBeforeResume()
          throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testRenameResumeThroughListStatusWithSrcDirETagChangedJustBeforeResume()
          throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
  @Test
  public void testRenameResumeThroughGetStatusWithSrcDirETagChangedJustBeforeResume()
          throws Exception {
    AzureBlobFileSystem fs = Mockito.spy(getFileSystem());
    assumeNonHnsAccountBlobEndpoint(fs);
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
    assumeNonHnsAccountBlobEndpoint(fs);
    AbfsBlobClient client = (AbfsBlobClient) addSpyHooksOnClient(fs);
    String dirPathStr = "/testDir/dir1";
    fs.mkdirs(new Path(dirPathStr));
    ExecutorService executorService = Executors.newFixedThreadPool(5);
    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final int iter = i;
      Future future = executorService.submit(() -> {
        return fs.create(new Path("/testDir/dir1/file" + iter));
      });
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
                tracingHeaderValidator.setOperatedBlobCount(11);
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
}
