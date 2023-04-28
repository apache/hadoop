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
import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.PrefixMode;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;

public class ITestAzureBlobFileSystemExplictImplicitRename
    extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemExplictImplicitRename() throws Exception {
    super();
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    Assume.assumeTrue(
        getFileSystem().getAbfsStore().getAbfsConfiguration().getPrefixMode()
            == PrefixMode.BLOB);
  }

  void createAzCopyDirectory(Path path) throws Exception {
    AzcopyHelper azcopyHelper = new AzcopyHelper(
        getAccountName(), getFileSystemName(),  getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .getRawConfiguration(), getFileSystem().getAbfsStore().getPrefixMode());
    azcopyHelper.createFolderUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));
  }

  void createAzCopyFile(Path path) throws Exception {
    AzcopyHelper azcopyHelper = new AzcopyHelper(getAccountName(),
        getFileSystemName(), getFileSystem().getAbfsStore()
        .getAbfsConfiguration()
        .getRawConfiguration(), getFileSystem().getAbfsStore().getPrefixMode());
    azcopyHelper.createFileUsingAzcopy(
        getFileSystem().makeQualified(path).toUri().getPath().substring(1));
  }

  @Test
  public void testRenameSrcFileInImplicitParentDirectory() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyDirectory(new Path("/src"));
    createAzCopyFile(new Path("/src/file"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore().getBlobProperty(new Path("/src"), Mockito.mock(
          TracingContext.class));
    });
    Assert.assertNotNull(fs.getAbfsStore()
        .getBlobProperty(new Path("/src/file"),
            Mockito.mock(TracingContext.class)));
    Assert.assertTrue(fs.rename(new Path("/src/file"), new Path("/dstFile")));
    Assert.assertNotNull(fs.getAbfsStore()
        .getBlobProperty(new Path("/dstFile"),
            Mockito.mock(TracingContext.class)));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/src/file"),
              Mockito.mock(TracingContext.class));
    });

    Assert.assertFalse(fs.rename(new Path("/src/file"), new Path("/dstFile2")));
  }

  @Test
  public void testRenameNonExistentFileInImplicitParent() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyDirectory(new Path("/src"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore().getBlobProperty(new Path("/src"), Mockito.mock(
          TracingContext.class));
    });

    Assert.assertFalse(fs.rename(new Path("/src/file"), new Path("/dstFile2")));
  }

  @Test
  public void testRenameFileToNonExistingDstInImplicitParent()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    createAzCopyDirectory(new Path("/dstDir"));
    createAzCopyFile(new Path("/dstDir/file2"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/dstDir"),
              Mockito.mock(TracingContext.class));
    });
    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dstDir")));
    Assert.assertTrue(fs.exists(new Path("/dstDir/file")));
  }

  @Test
  public void testRenameFileAsExistingExplicitDirectoryInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    createAzCopyDirectory(new Path("/dst"));
    fs.mkdirs(new Path("/dst/dir"));
    deleteBlobPath(fs, new Path("/dst"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/dst"),
              Mockito.mock(TracingContext.class));
    });
    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/file"),
              Mockito.mock(TracingContext.class));
    });
  }

  @Test
  public void testRenameFileAsExistingImplicitDirectoryInExplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    fs.mkdirs(new Path("/dst"));
    createAzCopyDirectory(new Path("/dst/dir"));
    createAzCopyFile(new Path("/dst/dir/file2"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/dst/dir"),
              Mockito.mock(TracingContext.class));
    });
    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/file"),
              Mockito.mock(TracingContext.class));
    });
  }

  @Test
  public void testRenameFileAsExistingImplicitDirectoryInImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    createAzCopyFile(new Path("/file"));
    createAzCopyDirectory(new Path("/dst"));
    createAzCopyDirectory(new Path("/dst/dir"));
    createAzCopyFile(new Path("/dst/dir/file2"));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/dst"),
              Mockito.mock(TracingContext.class));
    });
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/dst/dir"),
              Mockito.mock(TracingContext.class));
    });
    Assert.assertTrue(fs.rename(new Path("/file"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/file")));
    intercept(AbfsRestOperationException.class, () -> {
      fs.getAbfsStore()
          .getBlobProperty(new Path("/file"),
              Mockito.mock(TracingContext.class));
    });
  }

  @Test
  public void testRenameDirectoryContainingImplicitDirectory()
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    fs.mkdirs(new Path("/src"));
    fs.mkdirs(new Path("/dst"));
    createAzCopyDirectory(new Path("/src/subDir"));
    createAzCopyFile(new Path("/src/subDir/subFile"));
    createAzCopyFile(new Path("/src/subFile"));
    Assert.assertTrue(fs.rename(new Path("/src"), new Path("/dst/dir")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/subFile")));
    Assert.assertTrue(fs.exists(new Path("/dst/dir/subDir/subFile")));
  }

  @Test
  public void testRenameImplicitDirectoryContainingExplicitDirectory()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingImplicitDirectory()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitDirectoryContainingExplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitDirectoryContainingImplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingExplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryContainingImplicitDirectoryInImplicitSrcParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/false,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryWhereDstParentDoesntExist() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/false,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryWhereDstParentDoesntExist()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/false,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToNonExistentDstWithImplicitParent()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToNonExistentDstWithParentIsFile()
      throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/false,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/true,
        /*dstExist*/false,
        /*isDstFile*/false,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameExplicitDirectoryToFileDst() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/true,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameimplicitDirectoryToFileDst() throws Exception {
    explicitImplicitDirectoryRenameTest(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/true,
        /*shouldRenamePass*/false
    );
  }

  @Test
  public void testDirectoryIntoSameNameDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryToSameNameImplicitDirectoryDestination()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameDirectoryToImplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testImplicitDirectoryIntoSameNameDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testImplicitDirectoryIntoExplicitDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToExplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testImplicitDirectoryIntoSameNameImplicitDestination()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/"src",
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testImplicitDirectoryIntoImplicitDestination() throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameSubFile()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/"src",
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameImplicitDirectoryToImplicitDirectoryDestinationHavingSameNameImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/false,
        /*srcSubDirExplicit*/true,
        /*dstParentExplicit*/true,
        /*dstExplicit*/false,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/"src",
        /*isSubDirExplicit*/false, /*shouldRenamePass*/false
    );
  }

  @Test
  public void testRenameExplicitSrcWithImplicitSubDirToImplicitDstWithExplicitSubDir()
    throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/true, /*shouldRenamePass*/true
    );
  }

  @Test
  public void testRenameExplicitSrcWithImplicitSubDirToImplicitDstWithImplicitSubDir()
      throws Exception {
    explicitImplicitDirectoryRenameTestWithDestPathNames(
        /*srcParentExplicit*/true,
        /*srcExplicit*/true,
        /*srcSubDirExplicit*/false,
        /*dstParentExplicit*/false,
        /*dstExplicit*/true,
        /*dstParentExists*/true,
        /*isDstParentFile*/false,
        /*dstExist*/true,
        /*isDstFile*/false,
        /*srcName*/"src",
        /*dstName*/null,
        /*dstSubFileName*/null,
        /*dstSubDirName*/null,
        /*isSubDirExplicit*/false, /*shouldRenamePass*/true
    );
  }


  private void explicitImplicitDirectoryRenameTest(Boolean srcParentExplicit,
      Boolean srcExplicit,
      Boolean srcSubDirExplicit,
      Boolean dstParentExplicit,
      Boolean dstExplicit,
      Boolean dstParentExists,
      Boolean isDstParentFile,
      Boolean dstExist,
      Boolean isDstFile,
      Boolean shouldRenamePass) throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path srcParent = new Path("/srcParent");
    Path src = new Path(srcParent, "src");
    createSourcePaths(srcParentExplicit, srcExplicit, srcSubDirExplicit, fs,
        srcParent,
        src);

    Path dstParent = new Path("/dstParent");
    Path dst = new Path(dstParent, "dst");
    createDestinationPaths(dstParentExplicit, dstExplicit, dstParentExists,
        isDstParentFile,
        dstExist, isDstFile, fs, dstParent, dst, null, null, true);

    if (dstParentExists && !isDstParentFile && !dstParentExplicit) {
      intercept(AbfsRestOperationException.class, () -> {
        fs.getAbfsStore()
            .getBlobProperty(dstParent, Mockito.mock(TracingContext.class));
      });
    }

    explicitImplicitCaseRenameAssert(dstExist, shouldRenamePass, fs, src, dst);
  }

  private void explicitImplicitDirectoryRenameTestWithDestPathNames(Boolean srcParentExplicit,
      Boolean srcExplicit,
      Boolean srcSubDirExplicit,
      Boolean dstParentExplicit,
      Boolean dstExplicit,
      Boolean dstParentExists,
      Boolean isDstParentFile,
      Boolean dstExist,
      Boolean isDstFile,
      String srcName,
      String dstName,
      String dstSubFileName,
      String dstSubDirName,
      final Boolean isSubDirExplicit, Boolean shouldRenamePass)
      throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Path srcParent = new Path("/srcParent");
    Path src = new Path(srcParent, srcName != null ? srcName : "src");
    createSourcePaths(srcParentExplicit, srcExplicit, srcSubDirExplicit, fs,
        srcParent,
        src);

    Path dstParent = new Path("/dstParent");
    Path dst = new Path(dstParent, dstName != null ? dstName : "dst");
    createDestinationPaths(dstParentExplicit, dstExplicit, dstParentExists,
        isDstParentFile,
        dstExist, isDstFile, fs, dstParent, dst, dstSubFileName, dstSubDirName,
        isSubDirExplicit);

    if (dstParentExists && !isDstParentFile && !dstParentExplicit && !dstExplicit) {
      intercept(AbfsRestOperationException.class, () -> {
        fs.getAbfsStore()
            .getBlobProperty(dstParent, Mockito.mock(TracingContext.class));
      });
    }

    explicitImplicitCaseRenameAssert(dstExist, shouldRenamePass, fs, src, dst);
  }

  private void createSourcePaths(final Boolean srcParentExplicit,
      final Boolean srcExplicit,
      final Boolean srcSubDirExplicit,
      final AzureBlobFileSystem fs,
      final Path srcParent,
      final Path src) throws Exception {
    if (srcParentExplicit) {
      fs.mkdirs(srcParent);
    } else {
      createAzCopyDirectory(srcParent);
    }

    if (srcExplicit) {
      fs.mkdirs(src);
      if(!srcParentExplicit) {
        deleteBlobPath(fs, srcParent);
      }
    } else {
      createAzCopyDirectory(src);
    }
    createAzCopyFile(new Path(src, "subFile"));
    if (srcSubDirExplicit) {
      fs.mkdirs(new Path(src, "subDir"));
      if(!srcParentExplicit) {
        deleteBlobPath(fs, srcParent);
      }
      if(!srcExplicit) {
        deleteBlobPath(fs, src);
      }
    } else {
      Path srcSubDir = new Path(src, "subDir");
      createAzCopyDirectory(srcSubDir);
      createAzCopyFile(new Path(srcSubDir, "subFile"));
      intercept(AbfsRestOperationException.class, () -> {
        fs.getAbfsStore()
            .getBlobProperty(srcSubDir, Mockito.mock(TracingContext.class));
      });
    }
    if (!srcParentExplicit) {
      intercept(AbfsRestOperationException.class, () -> {
        fs.getAbfsStore()
            .getBlobProperty(srcParent, Mockito.mock(TracingContext.class));
      });
    }
    if (!srcExplicit) {
      intercept(AbfsRestOperationException.class, () -> {
        fs.getAbfsStore()
            .getBlobProperty(src, Mockito.mock(TracingContext.class));
      });
    }
  }

  private void deleteBlobPath(final AzureBlobFileSystem fs, final Path srcParent)
      throws AzureBlobFileSystemException {
    try {
      fs.getAbfsClient()
          .deleteBlobPath(srcParent, Mockito.mock(TracingContext.class));
    } catch (AbfsRestOperationException ex) {
      if(ex.getStatusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
        throw ex;
      }
    }
  }

  private void createDestinationPaths(final Boolean dstParentExplicit,
      final Boolean dstExplicit,
      final Boolean dstParentExists,
      final Boolean isDstParentFile,
      final Boolean dstExist,
      final Boolean isDstFile,
      final AzureBlobFileSystem fs,
      final Path dstParent,
      final Path dst, final String subFileName, final String subDirName,
      final Boolean isSubDirExplicit) throws Exception {
    if (dstParentExists) {
      if (!isDstParentFile) {
        if (dstParentExplicit) {
          fs.mkdirs(dstParent);
        } else {
          createAzCopyDirectory(dstParent);
        }
      } else {
        createAzCopyFile(dstParent);
      }
    }

    if (dstExist) {
      if (!isDstFile) {
        if (dstExplicit) {
          fs.mkdirs(dst);
          if(!dstParentExplicit) {
            deleteBlobPath(fs, dstParent);
          }
        } else {
          createAzCopyDirectory(dst);
        }
        if (subFileName != null) {
          createAzCopyFile(new Path(dst, subFileName));
        }
        if (subDirName != null) {
          if (isSubDirExplicit) {
            fs.mkdirs(new Path(dst, subDirName));
            if(!dstParentExplicit) {
              deleteBlobPath(fs, dstParent);
            }
            if(!dstExplicit) {
              deleteBlobPath(fs, dst);
            }
          } else {
            createAzCopyDirectory(new Path(dst, subDirName));
          }
        }
      } else {
        createAzCopyFile(dst);
      }
    }
  }

  private void explicitImplicitCaseRenameAssert(final Boolean dstExist,
      final Boolean shouldRenamePass,
      final AzureBlobFileSystem fs,
      final Path src,
      final Path dst) throws IOException {
    if (shouldRenamePass) {
      Assert.assertTrue(fs.rename(src, dst));
      if (dstExist) {
        Assert.assertTrue(fs.getAbfsStore()
            .getBlobProperty(new Path(dst, src.getName()),
                Mockito.mock(TracingContext.class))
            .getIsDirectory());
      } else {
        Assert.assertTrue(fs.getAbfsStore()
            .getBlobProperty(dst, Mockito.mock(TracingContext.class))
            .getIsDirectory());
      }
    } else {
      Assert.assertFalse(fs.rename(src, dst));
      Assert.assertTrue(fs.getAbfsStore()
          .getListBlobs(src, null, Mockito.mock(TracingContext.class), null,
              false)
          .size() > 0);
      if (dstExist) {
        Assert.assertTrue(fs.getAbfsStore()
            .getListBlobs(dst, null, Mockito.mock(TracingContext.class), null,
                false)
            .size() > 0);
      }
    }
  }
}
