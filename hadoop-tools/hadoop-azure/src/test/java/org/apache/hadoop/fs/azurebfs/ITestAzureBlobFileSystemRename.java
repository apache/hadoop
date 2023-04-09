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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsClientTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOpTestUtil;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationTestUtil;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;

import static org.apache.hadoop.fs.azurebfs.RenameAtomicityUtils.SUFFIX;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_ABORTED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_FAILED;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.COPY_STATUS_PENDING;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_ABORTED;
import static org.apache.hadoop.fs.azurebfs.contracts.services.AzureServiceErrorCode.COPY_BLOB_FAILED;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathDoesNotExist;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertRenameOutcome;

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
    Path sourceDir = new Path("/testSrc");
    assertMkdirs(fs, sourceDir);
    String filename = "file1";
    Path file1 = new Path(sourceDir, filename);
    touch(file1);

    Path destDir = new Path("/testDst");
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
    fs.mkdirs(new Path("testDir"));
    Path test1 = new Path("testDir/test1");
    fs.mkdirs(test1);
    fs.mkdirs(new Path("testDir/test1/test2"));
    fs.mkdirs(new Path("testDir/test1/test2/test3"));

    assertRenameOutcome(fs, test1,
        new Path("testDir/test10"), true);
    assertPathDoesNotExist(fs, "rename source dir", test1);
  }

  @Test
  public void testRenameFirstLevelDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final List<Future<Void>> tasks = new ArrayList<>();

    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 1000; i++) {
      final Path fileName = new Path("/test/" + i);
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
    Path source = new Path("/test");
    Path dest = new Path("/renamedDir");
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
    fs.mkdirs(new Path("testDir2/test1/test2/test3"));
    fs.mkdirs(new Path("testDir2/test4"));
    Assert.assertTrue(fs.rename(new Path("testDir2/test1/test2/test3"), new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test4/test3")));
    assertFalse(fs.exists(new Path("testDir2/test1/test2/test3")));
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
    Assert.assertFalse(fs.rename(new Path("testDir2/test1/test2/test3"),
        new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2")));
    assertTrue(fs.exists(new Path("testDir2/test4")));
    assertTrue(fs.exists(new Path("testDir2/test1/test2/test3")));
    if (getIsNamespaceEnabled(fs)
        || fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
        == PrefixMode.BLOB) {
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
    byte[] etag = fs.getXAttr(new Path("testDir2/test4/file1"), "ETag");
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

  private void assumeNonHnsAccountBlobEndpoint(final AzureBlobFileSystem fs) {
    Assume.assumeTrue("To work on only on non-HNS Blob endpoint",
        fs.getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }

  /**
   * Test that after completing rename for a directory which is enabled for
   * AtomicRename, the RenamePending JSON file is deleted.
   */
  @Test
  public void testRenamePendingJsonIsRemovedPostSuccessfulRename()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    Mockito.doAnswer(answer -> {
      final String correctDeletePath = "/hbase/test1/test2/test3" + SUFFIX;
      if (correctDeletePath.equals(
          ((Path) answer.getArgument(0)).toUri().getPath())) {
        correctDeletePathCount[0] = 1;
      }
      return null;
    }).when(spiedFs).delete(Mockito.any(Path.class), Mockito.anyBoolean());
    Assert.assertTrue(spiedFs.rename(new Path("hbase/test1/test2/test3"),
        new Path("hbase/test4")));
    Assert.assertTrue(correctDeletePathCount[0] == 1);
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRename() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore azureBlobFileSystemStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(azureBlobFileSystemStore);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final TracingContext tracingContext = answer.getArgument(2);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2/test3"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2/test3" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(
              ("/" + "hbase/test1/test2/test3" + SUFFIX).equalsIgnoreCase(
                  path.toUri().getPath()));
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          TracingContext tracingContext = answer.getArgument(1);
          Assert.assertTrue(
              ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath()));
          deletedCount.incrementAndGet();
          client.deleteBlobPath(path, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

    spiedFsForListPath.listStatus(new Path("hbase/test1/test2"));
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 2);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * ListStatus API will be called on the directory. Expectation is that the ListStatus
   * API of {@link AzureBlobFileSystem} should recover the paused rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameForNestedSourceThroughListFile()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore azureBlobFileSystemStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(azureBlobFileSystemStore);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final TracingContext tracingContext = answer.getArgument(2);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
              path.toUri().getPath()));
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          TracingContext tracingContext = answer.getArgument(1);
          Assert.assertTrue(
              ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath()));
          deletedCount.incrementAndGet();
          client.deleteBlobPath(path, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

    /*
     * listFile on /hbase/test1 would give no result because
     * /hbase/test1/test2 would be totally moved to /hbase/test4.
     */
    final FileStatus[] listFileResult = spiedFsForListPath.listStatus(
        new Path("hbase/test1"));
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 2);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test2/test3/"))));
    Assert.assertTrue(listFileResult.length == 0);
  }

  /**
   * Test for a directory in /hbase directory. To simulate the crash of process,
   * test will throw an exception with 503 on a copy of one of the blob. The
   * source directory is a nested directory.<br>
   * GetFileStatus API will be called on the directory. Expectation is that the
   * GetFileStatus API of {@link AzureBlobFileSystem} should recover the paused
   * rename.
   */
  @Test
  public void testHBaseHandlingForFailedRenameForNestedSourceThroughGetPathStatus()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore azureBlobFileSystemStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(azureBlobFileSystemStore);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    //fail copy of /hbase/test1/test2/test3/file1.
    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final TracingContext tracingContext = answer.getArgument(2);
          if (("/" + failedCopyPath).equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertTrue(fs.exists(new Path(failedCopyPath)));
    Assert.assertFalse(spiedFs.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if (("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(("/" + "hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
              path.toUri().getPath()));
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          TracingContext tracingContext = answer.getArgument(1);
          Assert.assertTrue(
              ("/" + failedCopyPath).equalsIgnoreCase(path.toUri().getPath()));
          deletedCount.incrementAndGet();
          client.deleteBlobPath(path, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

    /*
     * getFileStatus on /hbase/test2 should give NOT_FOUND exception, since,
     * /hbase/test1/test2 was partially renamed. On the invocation of getFileStatus
     * on the directory, the remaining rename will be made. And as the directory is renamed,
     * the method should give NOT_FOUND exception.
     */
    FileStatus fileStatus = null;
    Boolean notFoundExceptionReceived = false;
    try {
      fileStatus = spiedFsForListPath.getFileStatus(
          new Path("hbase/test1/test2"));
    } catch (FileNotFoundException ex) {
      notFoundExceptionReceived = true;

    }
    Assert.assertTrue(notFoundExceptionReceived);
    Assert.assertNull(fileStatus);
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 2);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(failedCopyPath)));
    Assert.assertTrue(spiedFsForListPath.exists(new Path(
        failedCopyPath.replace("test1/test2/test3/", "test4/test2/test3/"))));
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
  public void testHbaseListStatusBeforeRenamePendingFileAppended()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore azureBlobFileSystemStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(azureBlobFileSystemStore);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);

    Boolean[] renamePendingJsonCreated = new Boolean[1];
    renamePendingJsonCreated[0] = false;
    Boolean[] parallelListStatusCalledOnTheDirBeingRenamed = new Boolean[1];
    parallelListStatusCalledOnTheDirBeingRenamed[0] = false;
    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      FSDataOutputStream outputStream = fs.create(path, recursive);
      renamePendingJsonCreated[0] = true;
      while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return outputStream;
    }).when(spiedFs).create(Mockito.any(Path.class), Mockito.anyBoolean());

    try {
      new Thread(() -> {
        //wait for the renamePending created;
        while (!renamePendingJsonCreated[0]) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        try {
          spiedFs.listStatus(new Path("hbase/test1"));
          parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).start();
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertFalse(fs.exists(new Path(failedCopyPath)));
    Assert.assertTrue(
        spiedFs.exists(new Path(failedCopyPath.replace("test1/", "test4/"))));
  }

  @Test
  public void testHbaseEmptyRenamePendingJsonDeletedBeforeListStatusCanDelete()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    final String failedCopyPath = "hbase/test1/test2/test3/file1";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path(failedCopyPath));
    fs.mkdirs(new Path("hbase/test4/"));
    fs.create(new Path("hbase/test4/file1"));
    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore azureBlobFileSystemStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(azureBlobFileSystemStore);
    final Integer[] correctDeletePathCount = new Integer[1];
    correctDeletePathCount[0] = 0;

    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);
    AzureBlobFileSystem listFileFs = Mockito.spy(fs);


    Boolean[] renamePendingJsonCreated = new Boolean[1];
    renamePendingJsonCreated[0] = false;
    Boolean[] parallelListStatusCalledOnTheDirBeingRenamed = new Boolean[1];
    parallelListStatusCalledOnTheDirBeingRenamed[0] = false;
    Boolean[] parallelDeleteOfRenamePendingFileFromRenameFlow = new Boolean[1];
    parallelDeleteOfRenamePendingFileFromRenameFlow[0] = false;
    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      FSDataOutputStream outputStream = fs.create(path, recursive);
      renamePendingJsonCreated[0] = true;
      while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return outputStream;
    }).when(spiedFs).create(Mockito.any(Path.class), Mockito.anyBoolean());

    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      Boolean recursive = answer.getArgument(1);
      if (("/hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        while (!parallelListStatusCalledOnTheDirBeingRenamed[0]) {
          Thread.sleep(100);
        }
        parallelDeleteOfRenamePendingFileFromRenameFlow[0] = true;
        return fs.delete(path, recursive);
      }
      return fs.delete(path, recursive);
    }).when(spiedFs).delete(Mockito.any(Path.class), Mockito.anyBoolean());

    Mockito.doAnswer(answer -> {
      Path path = answer.getArgument(0);
      if (("/hbase/test1/test2" + SUFFIX).equalsIgnoreCase(
          path.toUri().getPath())) {
        FSDataInputStream inputStream = fs.open(path);
        parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        while (!parallelDeleteOfRenamePendingFileFromRenameFlow[0]) {
          Thread.sleep(100);
        }
        return inputStream;
      }
      return fs.open(path);
    }).when(listFileFs).open(Mockito.any(Path.class));

    try {
      new Thread(() -> {
        //wait for the renamePending created;
        while (!renamePendingJsonCreated[0]) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        try {
          listFileFs.listStatus(new Path("hbase/test1"));
          parallelListStatusCalledOnTheDirBeingRenamed[0] = true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).start();
      spiedFs.rename(new Path("hbase/test1/test2"),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }
    Assert.assertFalse(fs.exists(new Path(failedCopyPath)));
    Assert.assertTrue(
        spiedFs.exists(new Path(failedCopyPath.replace("test1/", "test4/"))));
  }

  @Test
  public void testInvalidJsonForRenamePendingFile() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path("hbase/test1/test2/test3"));
    fs.create(new Path("hbase/test1/test2/test3/file"));
    fs.create(new Path("hbase/test1/test2/test3/file1"));
    FSDataOutputStream outputStream = fs.create(
        new Path("hbase/test1/test2/test3" + SUFFIX));
    outputStream.writeChars("{ some wrong json");
    outputStream.flush();
    outputStream.close();

    fs.listStatus(new Path("hbase/test1/test2"));
    Assert.assertFalse(fs.exists(new Path("hbase/test1/test2/test3" + SUFFIX)));
  }

  @Test
  public void testEmptyDirRenameResolveFromListStatus() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/hbase/test1/test2/test3";
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(new Path(srcDir));
    fs.mkdirs(new Path("hbase/test4"));

    AzureBlobFileSystem spiedFs = Mockito.spy(fs);

    AzureBlobFileSystemStore spiedAbfsStore = Mockito.spy(
        spiedFs.getAbfsStore());
    spiedFs.setAbfsStore(spiedAbfsStore);
    Mockito.doAnswer(answer -> {
          final Path srcPath = answer.getArgument(0);
          final Path dstPath = answer.getArgument(1);
          final TracingContext tracingContext = answer.getArgument(2);

          if (srcDir.equalsIgnoreCase(srcPath.toUri().getPath())) {
            throw new AbfsRestOperationException(HttpURLConnection.HTTP_UNAVAILABLE,
                AzureServiceErrorCode.INGRESS_OVER_ACCOUNT_LIMIT.getErrorCode(),
                "Ingress is over the account limit.", new Exception());
          }
          fs.getAbfsStore().copyBlob(srcPath, dstPath, tracingContext);
          return null;
        })
        .when(spiedAbfsStore)
        .copyBlob(Mockito.any(Path.class), Mockito.any(Path.class),
            Mockito.any(TracingContext.class));
    try {
      spiedFs.rename(new Path(srcDir),
          new Path("hbase/test4"));
    } catch (Exception ex) {

    }

    Assert.assertFalse(spiedFs.exists(
        new Path(srcDir.replace("test1/test2/test3", "test4/test3/"))));

    //call listPath API, it will recover the rename atomicity.
    final AzureBlobFileSystem spiedFsForListPath = Mockito.spy(fs);
    final int[] openRequiredFile = new int[1];
    openRequiredFile[0] = 0;
    Mockito.doAnswer(answer -> {
      final Path path = answer.getArgument(0);
      if ((srcDir + SUFFIX).equalsIgnoreCase(path.toUri().getPath())) {
        openRequiredFile[0] = 1;
      }
      return fs.open(path);
    }).when(spiedFsForListPath).open(Mockito.any(Path.class));

    /*
     * Check if the fs.delete is on the renameJson file.
     */
    AtomicInteger deletedCount = new AtomicInteger(0);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          Boolean recursive = answer.getArgument(1);
          Assert.assertTrue(
              (srcDir + SUFFIX).equalsIgnoreCase(path.toUri().getPath()));
          deletedCount.incrementAndGet();
          return fs.delete(path, recursive);
        })
        .when(spiedFsForListPath)
        .delete(Mockito.any(Path.class), Mockito.anyBoolean());

    /*
     * Check if the blob which will be retried is deleted from the renameBlob
     * method.
     */
    AbfsClient client = spiedFsForListPath.getAbfsClient();
    final AbfsClient spiedClientForListPath = Mockito.spy(client);
    spiedFsForListPath.getAbfsStore().setClient(spiedClientForListPath);
    Mockito.doAnswer(answer -> {
          Path path = answer.getArgument(0);
          TracingContext tracingContext = answer.getArgument(1);
          Assert.assertTrue((srcDir).equalsIgnoreCase(path.toUri().getPath()));
          deletedCount.incrementAndGet();
          client.deleteBlobPath(path, tracingContext);
          return null;
        })
        .when(spiedClientForListPath)
        .deleteBlobPath(Mockito.any(Path.class),
            Mockito.any(TracingContext.class));

    /*
     * getFileStatus on /hbase/test2 should give NOT_FOUND exception, since,
     * /hbase/test1/test2 was partially renamed. On the invocation of getFileStatus
     * on the directory, the remaining rename will be made. And as the directory is renamed,
     * the method should give NOT_FOUND exception.
     */
    FileStatus fileStatus = null;
    Boolean notFoundExceptionReceived = false;
    try {
      fileStatus = spiedFsForListPath.getFileStatus(new Path(srcDir));
    } catch (FileNotFoundException ex) {
      notFoundExceptionReceived = true;

    }
    Assert.assertTrue(notFoundExceptionReceived);
    Assert.assertNull(fileStatus);
    Assert.assertTrue(openRequiredFile[0] == 1);
    Assert.assertTrue(deletedCount.get() == 2);
    Assert.assertFalse(spiedFsForListPath.exists(new Path(srcDir)));
    Assert.assertTrue(spiedFsForListPath.getFileStatus(
            new Path(srcDir.replace("test1/test2/test3", "test4/test3/")))
        .isDirectory());
  }

  @Test
  public void testRenameBlobIdempotency() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    spiedFs.setAbfsStore(spiedStore);
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Assert.assertTrue(spiedFs.rename(new Path(srcDir), new Path("/test4")));
    Assert.assertTrue(spiedFs.exists(new Path("test4/test3/file1")));
    Assert.assertTrue(spiedFs.exists(new Path("test4/test3/file2")));
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameBlobIdempotencyWhereDstIsCreatedFromSomeOtherProcess()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    spiedFs.setAbfsStore(spiedStore);
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              fs.create(new Path("/test4/test3", "file1"));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Boolean dstAlreadyThere = false;
    try {
      spiedFs.rename(new Path(srcDir), new Path("/test4"));
    } catch (RuntimeException ex) {
      if (ex.getMessage().contains(HttpURLConnection.HTTP_CONFLICT + "")) {
        dstAlreadyThere = true;
      }
    }
    Assert.assertTrue(dstAlreadyThere);
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameBlobIdempotencyWhereDstIsCopiedFromSomeOtherProcess()
      throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String srcDir = "/test1/test2/test3";
    fs.mkdirs(new Path(srcDir));
    fs.create(new Path(srcDir, "file1"));
    fs.create(new Path(srcDir, "file2"));

    fs.mkdirs(new Path("/test4"));

    final AzureBlobFileSystem spiedFs = Mockito.spy(fs);
    final AzureBlobFileSystemStore spiedStore = Mockito.spy(fs.getAbfsStore());
    spiedFs.setAbfsStore(spiedStore);
    final AbfsClient spiedClient = Mockito.spy(fs.getAbfsClient());
    spiedStore.setClient(spiedClient);

    /*
     * First call to copyBlob for file1 will fail with connection-reset, but the
     * backend has got the call. Retry of that API would give 409 error.
     */
    boolean[] hasBeenCalled = new boolean[1];
    hasBeenCalled[0] = false;

    boolean[] connectionResetThrown = new boolean[1];
    connectionResetThrown[0] = false;

    AbfsClientTestUtil.setMockAbfsRestOperationForCopyBlobOperation(spiedClient,
        (spiedRestOp, actualCallMakerOp) -> {

          Mockito.doAnswer(answer -> {
            if (spiedRestOp.getUrl().toString().contains("file1")
                && !hasBeenCalled[0]) {
              hasBeenCalled[0] = true;
              fs.create(new Path("/randomDir/test3/file1"));
              fs.rename(new Path("/randomDir/test3/file1"),
                  new Path("/test4/test3/file1"));
              AbfsRestOperationTestUtil.addAbfsHttpOpProcessResponseMock(
                  spiedRestOp, (mockAbfsHttpOp, actualAbfsHttpOp) -> {
                    Mockito.doAnswer(sendRequestAnswer -> {
                          if (!connectionResetThrown[0]) {
                            connectionResetThrown[0] = true;
                            throw new SocketException("connection-reset");
                          }
                          spiedRestOp.signRequest(actualAbfsHttpOp,
                              sendRequestAnswer.getArgument(2));
                          actualAbfsHttpOp.sendRequest(
                              sendRequestAnswer.getArgument(0),
                              sendRequestAnswer.getArgument(1),
                              sendRequestAnswer.getArgument(2));
                          AbfsHttpOpTestUtil.setConnection(mockAbfsHttpOp,
                              actualAbfsHttpOp);
                          return mockAbfsHttpOp;
                        }).when(mockAbfsHttpOp)
                        .sendRequest(Mockito.nullable(byte[].class),
                            Mockito.anyInt(), Mockito.anyInt());

                    return mockAbfsHttpOp;
                  });
              Mockito.doCallRealMethod()
                  .when(spiedRestOp)
                  .execute(Mockito.any(TracingContext.class));
              spiedRestOp.execute(answer.getArgument(0));
              return spiedRestOp;
            } else {
              actualCallMakerOp.execute(answer.getArgument(0));
              AbfsRestOperationTestUtil.setResult(spiedRestOp,
                  actualCallMakerOp.getResult());
              return actualCallMakerOp;
            }
          }).when(spiedRestOp).execute(Mockito.any(TracingContext.class));
          return spiedRestOp;
        });

    spiedFs.setWorkingDirectory(new Path("/"));

    Boolean dstAlreadyThere = false;
    try {
      spiedFs.rename(new Path(srcDir), new Path("/test4"));
    } catch (RuntimeException ex) {
      if (ex.getMessage().contains(HttpURLConnection.HTTP_CONFLICT + "")) {
        dstAlreadyThere = true;
      }
    }
    Assert.assertTrue(dstAlreadyThere);
    Assert.assertTrue(hasBeenCalled[0]);
    Assert.assertTrue(connectionResetThrown[0]);
  }

  @Test
  public void testRenameLargeNestedDir() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    String dir = "/";
    for (int i = 0; i < 100; i++) {
      dir += ("dir" + i + "/");
      fs.mkdirs(new Path(dir));
    }
    fs.mkdirs(new Path("/dst"));
    fs.rename(new Path("/dir0"), new Path("/dst"));
    dir = "";
    for (int i = 0; i < 100; i++) {
      dir += ("dir" + i + "/");
      Assert.assertTrue("" + i, fs.exists(new Path("/dst/" + dir)));
    }
  }

  @Test
  public void testRenameDirWhenMarkerBlobIsAbsent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.mkdirs(new Path("/test1"));
    fs.mkdirs(new Path("/test1/test2"));
    fs.mkdirs(new Path("/test1/test2/test3"));
    fs.create(new Path("/test1/test2/test3/file"));

    fs.getAbfsClient()
        .deleteBlobPath(new Path("/test1/test2"),
            Mockito.mock(TracingContext.class));
    fs.mkdirs(new Path("/test4/test5"));
    fs.rename(new Path("/test4"), new Path("/test1/test2"));

    Assert.assertTrue(fs.exists(new Path("/test1/test2/test4/test5")));

    fs.mkdirs(new Path("/test6"));
    fs.rename(new Path("/test6"), new Path("/test1/test2/test4/test5"));
    Assert.assertTrue(fs.exists(new Path("/test1/test2/test4/test5/test6")));

    fs.getAbfsClient()
        .deleteBlobPath(new Path("/test1/test2/test4/test5/test6"),
            Mockito.mock(TracingContext.class));
    fs.mkdirs(new Path("/test7"));
    fs.create(new Path("/test7/file"));
    fs.rename(new Path("/test7"), new Path("/test1/test2/test4/test5/test6"));
    Assert.assertTrue(
        fs.exists(new Path("/test1/test2/test4/test5/test6/file")));
  }

  @Test
  public void testBlobRenameSrcDirHasNoMarker() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fs);
    fs.create(new Path("/test1/test2/file1"));
    fs.getAbfsStore()
        .getClient()
        .deleteBlobPath(new Path("/test1"), Mockito.mock(TracingContext.class));
    Assert.assertNull(fs.getAbfsStore()
        .getBlobPropertyWithNotFoundHandling(new Path("/test1"),
            Mockito.mock(TracingContext.class)));
    fs.mkdirs(new Path("/test2"));
    fs.rename(new Path("/test1"), new Path("/test2"));
    Assert.assertNotNull(fs.getAbfsStore()
        .getBlobPropertyWithNotFoundHandling(new Path("/test2/test1"),
            Mockito.mock(TracingContext.class)));
  }

  @Test
  public void testCopyBlobTakeTime() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    fileSystem.setAbfsStore(store);
    Mockito.doReturn(COPY_STATUS_PENDING).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    fileSystem.create(new Path("/test1/file"));
    fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file2")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndEventuallyFail() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    fileSystem.setAbfsStore(store);
    Mockito.doReturn(COPY_STATUS_PENDING).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    Mockito.doReturn(COPY_STATUS_FAILED).when(store).getCopyStatus(Mockito.any(
        AbfsHttpOperation.class));
    fileSystem.create(new Path("/test1/file"));
    Boolean copyBlobFailureCaught = false;
    try {
      fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    } catch (AbfsRestOperationException e) {
      if (COPY_BLOB_FAILED.equals(e.getErrorCode())) {
        copyBlobFailureCaught = true;
      }
    }
    Assert.assertTrue(copyBlobFailureCaught);
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndEventuallyAborted() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    fileSystem.setAbfsStore(store);
    Mockito.doReturn(COPY_STATUS_PENDING).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    Mockito.doReturn(COPY_STATUS_ABORTED).when(store).getCopyStatus(Mockito.any(
        AbfsHttpOperation.class));
    fileSystem.create(new Path("/test1/file"));
    Boolean copyBlobFailureCaught = false;
    try {
      fileSystem.rename(new Path("/test1/file"), new Path("/test1/file2"));
    } catch (AbfsRestOperationException e) {
      if (COPY_BLOB_ABORTED.equals(e.getErrorCode())) {
        copyBlobFailureCaught = true;
      }
    }
    Assert.assertTrue(copyBlobFailureCaught);
    Assert.assertTrue(fileSystem.exists(new Path("/test1/file")));
    Mockito.verify(store, Mockito.times(1))
        .handleCopyInProgress(Mockito.any(Path.class),
            Mockito.any(TracingContext.class), Mockito.any(String.class));
  }

  @Test
  public void testCopyBlobTakeTimeAndBlobIsDeleted() throws Exception {
    AzureBlobFileSystem fileSystem = getFileSystem();
    assumeNonHnsAccountBlobEndpoint(fileSystem);
    AzureBlobFileSystemStore store = Mockito.spy(fileSystem.getAbfsStore());
    String srcFile = "/test1/file";
    String dstFile = "/test1/file2";
    fileSystem.setAbfsStore(store);
    Mockito.doAnswer(answer -> {
          fileSystem.delete(new Path(dstFile), false);
          return COPY_STATUS_PENDING;
        }).when(store)
        .getCopyBlobProgress(Mockito.any(AbfsRestOperation.class));
    fileSystem.create(new Path(srcFile));


    LambdaTestUtils.intercept(FileNotFoundException.class, () -> {
      fileSystem.rename(new Path(srcFile), new Path(dstFile));
    });
    Assert.assertFalse(fileSystem.exists(new Path(dstFile)));
  }
}
