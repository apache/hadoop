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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.concurrent.Callable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.test.LambdaTestUtils;

import org.junit.Assume;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.fs.azurebfs.constants.FSOperationType;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConcurrentWriteOperationDetectedException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.mockito.Mockito;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.TRUE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_BLOB_MKDIR_OVERWRITE;
import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_META_HDI_ISFOLDER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.assertj.core.api.Assertions;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.apache.hadoop.fs.azurebfs.AbfsStatistic.CONNECTIONS_MADE;

/**
 * Test create operation.
 */
public class ITestAzureBlobFileSystemCreate extends
    AbstractAbfsIntegrationTest {
  private static final Path TEST_FILE_PATH = new Path("testfile");
  private static final Path TEST_FOLDER_PATH = new Path("testFolder");
  private static final String TEST_CHILD_FILE = "childFile";

  public ITestAzureBlobFileSystemCreate() throws Exception {
    super();
  }

  @Test
  public void testEnsureFileCreatedImmediately() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    FSDataOutputStream out = fs.create(TEST_FILE_PATH);
    try {
      assertIsFile(fs, TEST_FILE_PATH);
    } finally {
      out.close();
    }
    assertIsFile(fs, TEST_FILE_PATH);
  }

  /**
   * Creating subdirectory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/d"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d/e")));

    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/d")));
    // Asserting directory created still exists as explicit.
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/d"), fs));
  }

  /**
   * Try creating file same as an existing directory.
   * @throws Exception
   */
  @Test
  public void testCreateDirectoryAndFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b/c")));

    // Asserting that directory still exists as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
  }

  /**
   * Creating same file without specifying overwrite.
   * @throws Exception
   */
  @Test
  public void testCreateSameFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    fs.create(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
  }

  /**
   * Creating same file with overwrite flag set to false.
   * @throws Exception
   */
  @Test
  public void testCreateSameFileWithOverwriteFalse() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b/c"), false));
  }

  /**
   * Creation of already existing subpath should fail.
   * @throws Exception
   */
  @Test
  public void testCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b")));
  }

  /**
   * Creating path with parent explicit.
   */
  @Test
  public void testCreatePathParentExplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    fs.create(new Path("a/b/c/d"));
    assertTrue(fs.exists(new Path("a/b/c/d")));

    // asserting that parent stays explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
  }


  /**
   * Test create on implicit directory with explicit parent.
   * @throws Exception
   */
  @Test
  public void testParentExplicitPathImplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
            getAccountName(),
            getFileSystemName(),
            getRawConfiguration(),
            store.getPrefixMode()
            );
    fs.mkdirs(new Path("/explicitParent"));
    String sourcePathName = "/explicitParent/implicitDir";
    Path sourcePath = new Path(sourcePathName);
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(sourcePath).toUri().getPath().substring(1));

    intercept(IOException.class, () ->
            fs.create(sourcePath, true));
    intercept(IOException.class, () ->
            fs.create(sourcePath, false));

    assertTrue("Parent directory should be explicit.",
            BlobDirectoryStateHelper.isExplicitDirectory(sourcePath.getParent(), fs));
    assertTrue("Path should be implicit.",
            BlobDirectoryStateHelper.isImplicitDirectory(sourcePath, fs));
  }

  /**
   * Test create on implicit directory with implicit parent
   * @throws Exception
   */
  @Test
  public void testParentImplicitPathImplicit() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
            getAccountName(),
            getFileSystemName(),
            getRawConfiguration(),
            store.getPrefixMode()
    );
    String parentPathName = "/implicitParent";
    Path parentPath = new Path(parentPathName);
    String sourcePathName = "/implicitParent/implicitDir";
    Path sourcePath = new Path(sourcePathName);

    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(parentPath).toUri().getPath().substring(1));
    azcopyHelper.createFolderUsingAzcopy(fs.makeQualified(sourcePath).toUri().getPath().substring(1));

    intercept(IOException.class, () ->
            fs.create(sourcePath, true));
    intercept(IOException.class, () ->
            fs.create(sourcePath, false));


    assertTrue("Parent directory is implicit.",
            BlobDirectoryStateHelper.isImplicitDirectory(parentPath, fs));

    assertTrue("Path should also be implicit.",
            BlobDirectoryStateHelper.isImplicitDirectory(sourcePath, fs));

  }

  /**
   * Tests create file when file exists already
   * Verifies using eTag for overwrite = true/false
   */
  @Test
  public void testCreateFileExistsImplicitParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();
    AzcopyHelper azcopyHelper = new AzcopyHelper(
            getAccountName(),
            getFileSystemName(),
            getRawConfiguration(),
            store.getPrefixMode()
    );
    String parentPathName = "/implicitParent";
    Path parentPath = new Path(parentPathName);
    azcopyHelper.createFolderUsingAzcopy(parentPathName);

    String fileName = "/implicitParent/testFile";
    Path filePath = new Path(fileName);
    fs.create(filePath);
    String eTag = extractFileEtag(fileName);

    // testing createFile on already existing file path
    fs.create(filePath, true);

    String eTagAfterCreateOverwrite = extractFileEtag(fileName);

    assertTrue("New file eTag after create overwrite should be different from old",
            !eTag.equals(eTagAfterCreateOverwrite));

    intercept(IOException.class, () ->
            fs.create(filePath, false));

    String eTagAfterCreate = extractFileEtag(fileName);

    assertTrue("File eTag should not change as creation fails",
            eTagAfterCreateOverwrite.equals(eTagAfterCreate));

    assertTrue("Parent path should also change to explicit.",
            BlobDirectoryStateHelper.isExplicitDirectory(parentPath, fs));
  }

  /**
   * Tests create file when the parent is an existing file
   * should fail but parent directory should change to explicit
   * @throws Exception FileAlreadyExists for blob and IOException for dfs.
   */
  @Test
  public void testCreateFileParentFile() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final AzureBlobFileSystemStore store = fs.getAbfsStore();

    String parentName = "/testParentFile";
    Path parent = new Path(parentName);
    fs.create(parent);

    String childName = "/testParentFile/testChildFile";
    Path child = new Path(childName);
    IOException e = intercept(IOException.class, () ->
            fs.create(child, false));

    assertFalse("Parent Path should be a file.",
            fs.getAbfsStore().getBlobProperty(parent, getTestTracingContext(fs, false))
                    .getIsDirectory());

  }


  /**
   * Creating directory on existing file path should fail.
   * @throws Exception
   */
  @Test
  public void testCreateMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a/b/c"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  /**
   * Test mkdirs.
   * @throws Exception
   */
  @Test
  public void testMkdirs() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.mkdirs(new Path("a/b/c/e"));

    assertTrue(fs.exists(new Path("a/b")));
    assertTrue(fs.exists(new Path("a/b/c/d")));
    assertTrue(fs.exists(new Path("a/b/c/e")));

    //Asserting that directories created as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b"), fs));
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"), fs));
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/e"), fs));
  }

  /**
   * Creating subpath of directory path should fail.
   * @throws Exception
   */
  @Test
  public void testMkdirsCreateSubPath() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
    intercept(IOException.class, () -> fs.create(new Path("a/b")));

    //Asserting that directories created as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
  }

  /**
   * Test creation of directory by level.
   * @throws Exception
   */
  @Test
  public void testMkdirsByLevel() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a"));
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c/d/e"));

    assertTrue(fs.exists(new Path("a")));
    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/c/d/e")));

    //Asserting that directories created as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/"), fs));
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d/e"), fs));
  }

  /*
    Delete part of a path and validate sub path exists.
   */
  @Test
  public void testMkdirsWithDelete() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b"));
    fs.mkdirs(new Path("a/b/c/d"));
    fs.delete(new Path("a/b/c/d"));
    fs.getFileStatus(new Path("a/b/c"));
    assertTrue(fs.exists(new Path("a/b/c")));
  }

  /**
   * Verify mkdir and rename of parent.
   */
  @Test
  public void testMkdirsWithRename() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c/d"));
    fs.create(new Path("e/file"));
    fs.delete(new Path("a/b/c/d"));
    assertTrue(fs.rename(new Path("e"), new Path("a/b/c/d")));
    assertTrue(fs.exists(new Path("a/b/c/d/file")));
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.setWorkingDirectory(new Path("/"));
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Create a file with name /dir1 and then mkdirs for /dir1/dir2 should fail.
   * @throws Exception
   */
  @Test
  public void testFileCreateMkdirsNonRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path p1 = new Path("dir1");
    fs.create(p1);
    intercept(IOException.class, () -> fs.mkdirs(new Path("dir1/dir2")));
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.mkdirs(new Path("a/b/c"));

    assertTrue(fs.exists(new Path("a/b/c")));
    //Asserting that directories created as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
  }

  /**
   * Creation of same directory without overwrite flag should pass.
   * @throws Exception
   */
  @Test
  public void testCreateSamePathDirectory() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.create(new Path("a"));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a")));
  }

  /**
   * Creation of directory with root as parent
   */
  @Test
  public void testMkdirOnRootAsParent() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("a");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory on root
   */
  @Test
  public void testMkdirOnRoot() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/");
    fs.setWorkingDirectory(new Path("/"));
    fs.mkdirs(path);

    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory on path with unicode chars
   */
  @Test
  public void testMkdirUnicode() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir\u0031");
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory on same path with parallel threads
   */
  @Test
  public void testMkdirParallelRequests() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path path = new Path("/dir1");

    ExecutorService es = Executors.newFixedThreadPool(3);

    List<CompletableFuture<Void>> tasks = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        try {
          fs.mkdirs(path);
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      }, es);
      tasks.add(future);
    }

    // Wait for all the tasks to complete
    CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

    // Assert that the directory created by mkdir exists as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));

  }


  /**
   * Creation of directory with overwrite set to false should not fail according to DFS code.
   * @throws Exception
   */
  @Test
  public void testCreateSameDirectoryOverwriteFalse() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.setBoolean(FS_AZURE_ENABLE_BLOB_MKDIR_OVERWRITE, false);
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    fs1.mkdirs(new Path("a/b/c"));
    fs1.mkdirs(new Path("a/b/c"));

    //Asserting that directories created as explicit
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs1));
  }

  /**
   * Try creating directory same as an existing file.
   */
  @Test
  public void testCreateDirectoryAndFileRecreation() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("a/b/c"));
    fs.create(new Path("a/b/c/d"));
    assertTrue(fs.exists(new Path("a/b/c")));
    assertTrue(fs.exists(new Path("a/b/c/d")));
    intercept(IOException.class, () -> fs.mkdirs(new Path("a/b/c/d")));
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created does not exist
   * @throws Exception
   */
  @Test
  public void testMkdirOnNonExistingPathWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating implicit directory to be used as parent
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    // Creating a directory on non-existing path inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(implicitPath, fs));

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as explicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingExplicitDirWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating implicit directory to be used as parent
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    // Creating an explicit directory on the path first
    fs.mkdirs(path);

    // Creating a directory on existing explicit directory inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(implicitPath, fs));

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory with parent directory existing as explicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithExplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path explicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating an explicit directory to be used a parent
    fs.mkdirs(explicitPath);

    // Creating implicit directory on the path
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));

    // Creating a directory on existing implicit directory inside an explicit directory
    fs.mkdirs(path);

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(explicitPath, fs));

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as implicit directory
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingImplicitDirWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating implicit directory to be used as parent
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    // Creating an implicit directory on path
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));

    // Creating a directory on existing implicit directory inside an implicit directory
    fs.mkdirs(path);

    // Asserting that path created by azcopy becomes explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(implicitPath, fs));

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(path, fs));
  }

  /**
   * Creation of directory with parent directory existing as implicit.
   * And the directory to be created existing as file
   * @throws Exception
   */
  @Test
  public void testMkdirOnExistingFileWithImplicitParentDir() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("dir1");
    final Path path = new Path("dir1/dir2");

    // Creating implicit directory to be used as parent
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    // Creating a file on path
    fs.create(path);

    // Creating a directory on existing file inside an implicit directory
    // Asserting that the mkdir fails
    LambdaTestUtils.intercept(FileAlreadyExistsException.class, () -> {
      fs.mkdirs(path);
    });

    // Asserting that path created by azcopy becomes explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(implicitPath, fs));

    // Asserting that the file still exists at path.
    assertFalse(
        fs.getAbfsStore().getBlobProperty(
            new Path(getFileSystem().makeQualified(path).toUri().getPath()),
            Mockito.mock(TracingContext.class)
        ).getIsDirectory()
    );
  }

  /**
   * Test to validate that if prefix mode is BLOB and client encryption key is not null, exception is thrown.
   */
  @Test
  public void testCPKOverBlob() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName(), "abcd");
    intercept(InvalidConfigurationValueException.class, () -> (AzureBlobFileSystem) FileSystem.newInstance(configuration));
  }

  /**
   * Test to validate that if prefix mode is BLOB and even if client encryption key empty, exception is thrown.
   */
  @Test
  public void testCPKOverBlobEmptyKey() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    Configuration configuration = Mockito.spy(getRawConfiguration());
    configuration.set(FS_AZURE_CLIENT_PROVIDED_ENCRYPTION_KEY + "." + getAccountName(), "");
    intercept(InvalidConfigurationValueException.class, () -> (AzureBlobFileSystem) FileSystem.newInstance(configuration));
  }

  /**
   * Test to validate that if prefix mode is BLOB and if client encryption key is not set, no exception is thrown.
   */
  @Test
  public void testCPKOverBlobNullKey() throws Exception {
    Assume.assumeTrue(getFileSystem().getAbfsStore().getPrefixMode() == PrefixMode.BLOB);
    Configuration configuration = Mockito.spy(getRawConfiguration());
    AzureBlobFileSystem fs1 = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
  }

  /**
   * 1. a/b/c as implicit.
   * 2. Create marker for b.
   * 3. Do mkdir on a/b/c/d.
   * 4. Verify all b,c,d have marker but a is implicit.
   */
  @Test
  public void testImplicitExplicitFolder() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("a/b/c");

    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    Path path = makeQualified(new Path("a/b"));
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put(X_MS_META_HDI_ISFOLDER, TRUE);
    fs.getAbfsStore().createFile(path, null, true,
        null, null, getTestTracingContext(fs, true), metadata);

    fs.mkdirs(new Path("a/b/c/d"));

    assertTrue(BlobDirectoryStateHelper.isImplicitDirectory(new Path("a"), fs));
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b"), fs));
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"), fs));
  }

  /**
   * 1. a/b/c implicit.
   * 2. Marker for a and c.
   * 3. mkdir on a/b/c/d.
   * 4. Verify a,c,d are explicit but b is implicit.
   */
  @Test
  public void testImplicitExplicitFolder1() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    final Path implicitPath = new Path("a/b/c");

    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(),
        getFileSystemName(),
        getFileSystem().getAbfsStore().getAbfsConfiguration().getRawConfiguration(),
        fs.getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(implicitPath).toUri().getPath().substring(1));

    Path path = makeQualified(new Path("a"));
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put(X_MS_META_HDI_ISFOLDER, TRUE);
    fs.getAbfsStore().createFile(path, null, true,
        null, null, getTestTracingContext(fs, true), metadata);

    fs.getAbfsStore().createFile(makeQualified(new Path("a/b/c")), null, true,
        null, null, getTestTracingContext(fs, true), metadata);

    fs.mkdirs(new Path("a/b/c/d"));

    assertTrue(BlobDirectoryStateHelper.isImplicitDirectory(new Path("a/b"), fs));

    // Asserting that the directory created by mkdir exists as explicit.
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a"), fs));
    assertTrue(BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c"), fs));
    assertTrue(
        BlobDirectoryStateHelper.isExplicitDirectory(new Path("a/b/c/d"), fs));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.MKDIR, false, 0));
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.registerListener(null);

    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive1() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException expected) {
    }
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);

  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateNonRecursive2() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();

    Path testFile = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try {
      fs.createNonRecursive(testFile, FsPermission.getDefault(), false, 1024, (short) 1, 1024, null);
      fail("Should've thrown");
    } catch (FileNotFoundException e) {
    }
    fs.mkdirs(TEST_FOLDER_PATH);
    fs.createNonRecursive(testFile, true, 1024, (short) 1, 1024, null)
        .close();
    assertIsFile(fs, testFile);
  }

  /**
   * Attempts to use to the ABFS stream after it is closed.
   */
  @Test
  public void testWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    out.close();
    intercept(IOException.class, () -> out.write('a'));
    intercept(IOException.class, () -> out.write(new byte[]{'a'}));
    // hsync is not ignored on a closed stream
    // out.hsync();
    out.flush();
    out.close();
  }

  /**
   * Attempts to double close an ABFS output stream from within a
   * FilterOutputStream.
   * That class handles a double failure on close badly if the second
   * exception rethrows the first.
   */
  @Test
  public void testTryWithResources() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      // this will cause the next write to failAll
      fs.delete(testPath, false);
      out.write('2');
      out.hsync();
      fail("Expected a failure");
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        assertEquals("suppressed count", 1, suppressed.length);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner);
      }
    }
  }

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testFilterFSWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    intercept(IOException.class,
        () -> {
          try (FilterOutputStream fos = new FilterOutputStream(out)) {
            fos.write('a');
            fos.flush();
            out.hsync();
            fs.delete(testPath, false);
            // trigger the first failure
            throw intercept(IOException.class,
                () -> {
                  fos.write('b');
                  out.hsync();
                  return "hsync didn't raise an IOE";
                });
          }
        });
  }

  /**
   * Tests if the number of connections made for:
   * 1. create overwrite=false of a file that doesnt pre-exist
   * 2. create overwrite=false of a file that pre-exists
   * 3. create overwrite=true of a file that doesnt pre-exist
   * 4. create overwrite=true of a file that pre-exists
   * matches the expectation when run against both combinations of
   * fs.azure.enable.conditional.create.overwrite=true and
   * fs.azure.enable.conditional.create.overwrite=false
   * @throws Throwable
   */
  @Test
  public void testDefaultCreateOverwriteFileTest() throws Throwable {
    testCreateFileOverwrite(true);
    testCreateFileOverwrite(false);
  }

  public void testCreateFileOverwrite(boolean enableConditionalCreateOverwrite)
      throws Throwable {
    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(enableConditionalCreateOverwrite));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    int ifBlobCheckIfPathDir = 0;
    if (fs.getAbfsStore().getPrefixMode() == PrefixMode.BLOB) {
      // connection 1: ListBlobs on path
      // connection 2: getBlobProperties on Path
      ifBlobCheckIfPathDir = 2;
      // in case parent of path is not root,
      // has to be incremented by 1 to account for an mkdirs call
    }

    long totalConnectionMadeBeforeTest = fs.getInstrumentationMap()
        .get(CONNECTIONS_MADE.getStatName());

    int createRequestCount = 0;
    final Path nonOverwriteFile = new Path("/NonOverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 1: Not Overwrite - File does not pre-exist
    // create should be successful
    fs.create(nonOverwriteFile, false);

    // One request to server to create path should be issued
    // two calls added for -
    // 1. getFileStatus
    // 2. actual create call
    createRequestCount+=2;
    createRequestCount+=ifBlobCheckIfPathDir;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    // Case 2: Not Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, false, 0));
    intercept(FileAlreadyExistsException.class,
        () -> fs.create(nonOverwriteFile, false));
    fs.registerListener(null);

    // One request to server to create path should be issued
    // Only single tryGetFileStatus should happen
    // validating path or subpath exists is not reached
    // createFile call is never reached
    createRequestCount++;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    final Path overwriteFilePath = new Path("/OverwriteTest_FileName_"
        + UUID.randomUUID().toString());

    // Case 3: Overwrite - File does not pre-exist
    // create should be successful
    fs.create(overwriteFilePath, true);

    // One request to server to create path should be issued
    // Single increment for actual create call
    createRequestCount++;
    createRequestCount+=ifBlobCheckIfPathDir;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());

    // Case 4: Overwrite - File pre-exists
    fs.registerListener(new TracingHeaderValidator(
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationId(),
        fs.getFileSystemId(), FSOperationType.CREATE, true, 0));
    fs.create(overwriteFilePath, true);
    fs.registerListener(null);

    if (enableConditionalCreateOverwrite) {
      // Three requests will be sent to server to create path,
      // 1. create without overwrite
      // 2. GetFileStatus to get eTag
      // 3. create with overwrite
      createRequestCount += 3;
    } else {
      createRequestCount++;
    }
    createRequestCount+=ifBlobCheckIfPathDir;

    assertAbfsStatistics(
        CONNECTIONS_MADE,
        totalConnectionMadeBeforeTest + createRequestCount,
        fs.getInstrumentationMap());
  }

  /**
   * Test negative scenarios with Create overwrite=false as default
   * With create overwrite=true ending in 3 calls:
   * A. Create overwrite=false
   * B. GFS
   * C. Create overwrite=true
   *
   * Scn1: A fails with HTTP409, leading to B which fails with HTTP404,
   *        detect parallel access
   * Scn2: A fails with HTTP409, leading to B which fails with HTTP500,
   *        fail create with HTTP500
   * Scn3: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP412, detect parallel access
   * Scn4: A fails with HTTP409, leading to B and then C,
   *        which fails with HTTP500, fail create with HTTP500
   * Scn5: A fails with HTTP500, fail create with HTTP500
   */
  @Test
  public void testNegativeScenariosForCreateOverwriteDisabled()
      throws Throwable {

    final AzureBlobFileSystem currentFs = getFileSystem();
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set("fs.azure.enable.conditional.create.overwrite",
        Boolean.toString(true));

    final AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(currentFs.getUri(),
            config);

    // Get mock AbfsClient with current config
    AbfsClient
        mockClient
        = TestAbfsClient.getMockAbfsClient(
        fs.getAbfsStore().getClient(),
        fs.getAbfsStore().getAbfsConfiguration());

    AzureBlobFileSystemStore abfsStore = fs.getAbfsStore();
    abfsStore = setAzureBlobSystemStoreField(abfsStore, "client", mockClient);
    boolean isNamespaceEnabled = abfsStore
        .getIsNamespaceEnabled(getTestTracingContext(fs, false));

    AbfsRestOperation successOp = mock(
        AbfsRestOperation.class);
    AbfsHttpOperation http200Op = mock(
        AbfsHttpOperation.class);
    when(http200Op.getStatusCode()).thenReturn(HTTP_OK);
    when(successOp.getResult()).thenReturn(http200Op);

    AbfsRestOperationException conflictResponseEx
        = getMockAbfsRestOperationException(HTTP_CONFLICT);
    AbfsRestOperationException serverErrorResponseEx
        = getMockAbfsRestOperationException(HTTP_INTERNAL_ERROR);
    AbfsRestOperationException fileNotFoundResponseEx
        = getMockAbfsRestOperationException(HTTP_NOT_FOUND);
    AbfsRestOperationException preConditionResponseEx
        = getMockAbfsRestOperationException(HTTP_PRECON_FAILED);

    // mock for overwrite=false
    doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
        .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
        .doThrow(
            conflictResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            conflictResponseEx) // Scn4: create overwrite=true fails with Http500
        .doThrow(
            serverErrorResponseEx) // Scn5: create overwrite=false fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(false),
            isNamespaceEnabled ? any(String.class) : eq(null),
            isNamespaceEnabled ? any(String.class) : eq(null),
            any(boolean.class), eq(null), any(TracingContext.class));

    // mock for overwrite=false
    doThrow(conflictResponseEx) // Scn1: GFS fails with Http404
        .doThrow(conflictResponseEx) // Scn2: GFS fails with Http500
        .doThrow(
            conflictResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            conflictResponseEx) // Scn4: create overwrite=true fails with Http500
        .doThrow(
            serverErrorResponseEx) // Scn5: create overwrite=false fails with Http500
        .when(mockClient)
        .createPathBlob(any(String.class), eq(true), eq(false),
            any(), eq(null), any(TracingContext.class));

    doThrow(fileNotFoundResponseEx) // Scn1: GFS fails with Http404
        .doThrow(serverErrorResponseEx) // Scn2: GFS fails with Http500
        .doReturn(successOp) // Scn3: create overwrite=true fails with Http412
        .doReturn(successOp) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .getPathStatus(any(String.class), eq(false), any(TracingContext.class));

    // mock for overwrite=true
    doThrow(
        preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .createPathBlob(any(String.class), eq(true), eq(true),
            any(), eq(null), any(TracingContext.class));

    // mock for overwrite=true
    doThrow(
        preConditionResponseEx) // Scn3: create overwrite=true fails with Http412
        .doThrow(
            serverErrorResponseEx) // Scn4: create overwrite=true fails with Http500
        .when(mockClient)
        .createPath(any(String.class), eq(true), eq(true),
            isNamespaceEnabled ? any(String.class) : eq(null),
            isNamespaceEnabled ? any(String.class) : eq(null),
            any(boolean.class), eq(null), any(TracingContext.class));

    // Scn1: GFS fails with Http404
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with File Not found
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn2: GFS fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn3: create overwrite=true fails with Http412
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Pre-Condition
    // Create will fail with ConcurrentWriteOperationDetectedException
    validateCreateFileException(ConcurrentWriteOperationDetectedException.class,
        abfsStore);

    // Scn4: create overwrite=true fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with conflict
    // 2. GFS - pass
    // 3. create overwrite=true - fail with Server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);

    // Scn5: create overwrite=false fails with Http500
    // Sequence of events expected:
    // 1. create overwrite=false - fail with server error
    // Create will fail with 500
    validateCreateFileException(AbfsRestOperationException.class, abfsStore);
  }

  private AzureBlobFileSystemStore setAzureBlobSystemStoreField(
      final AzureBlobFileSystemStore abfsStore,
      final String fieldName,
      Object fieldObject) throws Exception {

    Field abfsClientField = AzureBlobFileSystemStore.class.getDeclaredField(
        fieldName);
    abfsClientField.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(abfsClientField,
        abfsClientField.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
    abfsClientField.set(abfsStore, fieldObject);
    return abfsStore;
  }

  private <E extends Throwable> void validateCreateFileException(final Class<E> exceptionClass, final AzureBlobFileSystemStore abfsStore)
      throws Exception {
    FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL,
        FsAction.ALL);
    FsPermission umask = new FsPermission(FsAction.NONE, FsAction.NONE,
        FsAction.NONE);
    Path testPath = new Path("testFile");
    intercept(
        exceptionClass,
        () -> abfsStore.createFile(testPath, null, true, permission, umask,
            getTestTracingContext(getFileSystem(), true), null));
  }

  private AbfsRestOperationException getMockAbfsRestOperationException(int status) {
    return new AbfsRestOperationException(status, "", "", new Exception());
  }

  /**
   * Attempts to test multiple flush calls.
   */
  @Test
  public void testMultipleFlush() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      out.hsync();
      out.write('2');
      out.hsync();
    }
  }

  /**
   * Delete the blob before flush and verify that an exception should be thrown.
   */
  @Test
  public void testDeleteBeforeFlush() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    try (FSDataOutputStream out = fs.create(testPath)) {
      out.write('1');
      fs.delete(testPath, false);
      out.hsync();
      // this will cause the next write to failAll
    } catch (IOException fnfe) {
      //appendblob outputStream does not generate suppressed exception on close as it is
      //single threaded code
      if (!fs.getAbfsStore().isAppendBlobKey(fs.makeQualified(testPath).toString())) {
        // the exception raised in close() must be in the caught exception's
        // suppressed list
        Throwable[] suppressed = fnfe.getSuppressed();
        assertEquals("suppressed count", 1, suppressed.length);
        Throwable inner = suppressed[0];
        if (!(inner instanceof IOException)) {
          throw inner;
        }
        GenericTestUtils.assertExceptionContains(fnfe.getMessage(), inner.getCause(), inner.getCause().getMessage());
      }
    }
  }

  /**
   * Extracts the eTag for an existing file
   * @param fileName file Path in String from container root
   * @return String etag for the file
   * @throws IOException
   */
  private String extractFileEtag(String fileName) throws IOException {
    final AzureBlobFileSystem fs = getFileSystem();
    final AbfsClient client = fs.getAbfsClient();
    final TracingContext testTracingContext = getTestTracingContext(fs, false);
    AbfsRestOperation op = client.getPathStatus(fileName, true, testTracingContext);
    return AzureBlobFileSystemStore.extractEtagHeader(op.getResult());
  }
}

