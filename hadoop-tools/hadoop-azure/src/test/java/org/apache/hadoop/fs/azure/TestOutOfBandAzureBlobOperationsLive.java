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

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.windowsazure.storage.blob.BlobOutputStream;
import com.microsoft.windowsazure.storage.blob.CloudBlockBlob;

public class TestOutOfBandAzureBlobOperationsLive {
  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  // scenario for this particular test described at MONARCH-HADOOP-764
  // creating a file out-of-band would confuse mkdirs("<oobfilesUncleFolder>")
  // eg oob creation of "user/<name>/testFolder/a/input/file"
  // Then wasb creation of "user/<name>/testFolder/a/output" fails
  @Test
  public void outOfBandFolder_uncleMkdirs() throws Exception {

    // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    // WASB driver methods prepend working directory implicitly.
    String workingDir = "user/"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/";

    CloudBlockBlob blob = testAccount.getBlobReference(workingDir
        + "testFolder1/a/input/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();
    assertTrue(fs.exists(new Path("testFolder1/a/input/file")));

    Path targetFolder = new Path("testFolder1/a/output");
    assertTrue(fs.mkdirs(targetFolder));
  }

  // scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_parentDelete() throws Exception {

    // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    // WASB driver methods prepend working directory implicitly.
    String workingDir = "user/"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
    CloudBlockBlob blob = testAccount.getBlobReference(workingDir
        + "testFolder2/a/input/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();
    assertTrue(fs.exists(new Path("testFolder2/a/input/file")));

    Path targetFolder = new Path("testFolder2/a/input");
    assertTrue(fs.delete(targetFolder, true));
  }

  @Test
  public void outOfBandFolder_rootFileDelete() throws Exception {

    CloudBlockBlob blob = testAccount.getBlobReference("fileY");
    BlobOutputStream s = blob.openOutputStream();
    s.close();
    assertTrue(fs.exists(new Path("/fileY")));
    assertTrue(fs.delete(new Path("/fileY"), true));
  }

  @Test
  public void outOfBandFolder_firstLevelFolderDelete() throws Exception {

    CloudBlockBlob blob = testAccount.getBlobReference("folderW/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();
    assertTrue(fs.exists(new Path("/folderW")));
    assertTrue(fs.exists(new Path("/folderW/file")));
    assertTrue(fs.delete(new Path("/folderW"), true));
  }

  // scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_siblingCreate() throws Exception {

    // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    // WASB driver methods prepend working directory implicitly.
    String workingDir = "user/"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
    CloudBlockBlob blob = testAccount.getBlobReference(workingDir
        + "testFolder3/a/input/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();
    assertTrue(fs.exists(new Path("testFolder3/a/input/file")));

    Path targetFile = new Path("testFolder3/a/input/file2");
    FSDataOutputStream s2 = fs.create(targetFile);
    s2.close();
  }

  // scenario for this particular test described at MONARCH-HADOOP-764
  // creating a new file in the root folder
  @Test
  public void outOfBandFolder_create_rootDir() throws Exception {
    Path targetFile = new Path("/newInRoot");
    FSDataOutputStream s2 = fs.create(targetFile);
    s2.close();
  }

  // scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_rename() throws Exception {

    // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    // WASB driver methods prepend working directory implicitly.
    String workingDir = "user/"
        + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
    CloudBlockBlob blob = testAccount.getBlobReference(workingDir
        + "testFolder4/a/input/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();

    Path srcFilePath = new Path("testFolder4/a/input/file");
    assertTrue(fs.exists(srcFilePath));

    Path destFilePath = new Path("testFolder4/a/input/file2");
    fs.rename(srcFilePath, destFilePath);
  }

  // Verify that you can rename a file which is the only file in an implicit folder in the
  // WASB file system.
  // scenario for this particular test described at MONARCH-HADOOP-892
  @Test
  public void outOfBandSingleFile_rename() throws Exception {

    //NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    //       WASB driver methods prepend working directory implicitly.
    String workingDir = "user/" + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
    CloudBlockBlob blob = testAccount.getBlobReference(workingDir + "testFolder5/a/input/file");
    BlobOutputStream s = blob.openOutputStream();
    s.close();

    Path srcFilePath = new Path("testFolder5/a/input/file");
    assertTrue(fs.exists(srcFilePath));

    Path destFilePath = new Path("testFolder5/file2");
    fs.rename(srcFilePath, destFilePath);
  }

  // WASB must force explicit parent directories in create, delete, mkdirs, rename.
  // scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_rename_rootLevelFiles() throws Exception {

    // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
    // WASB driver methods prepend working directory implicitly.
    CloudBlockBlob blob = testAccount.getBlobReference("fileX");
    BlobOutputStream s = blob.openOutputStream();
    s.close();

    Path srcFilePath = new Path("/fileX");
    assertTrue(fs.exists(srcFilePath));

    Path destFilePath = new Path("/fileXrename");
    fs.rename(srcFilePath, destFilePath);
  }
}
