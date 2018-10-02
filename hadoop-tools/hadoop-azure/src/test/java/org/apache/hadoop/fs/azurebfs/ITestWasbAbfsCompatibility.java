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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertDeleted;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsDirectory;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertMkdirs;
import static org.apache.hadoop.fs.contract.ContractTestUtils.assertPathExists;

/**
 * Test compatibility between ABFS client and WASB client.
 */
public class ITestWasbAbfsCompatibility extends AbstractAbfsIntegrationTest {
  private static final String WASB_TEST_CONTEXT = "wasb test file";
  private static final String ABFS_TEST_CONTEXT = "abfs test file";
  private static final String TEST_CONTEXT = "THIS IS FOR TEST";

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestWasbAbfsCompatibility.class);

  public ITestWasbAbfsCompatibility() throws Exception {
    Assume.assumeFalse("Emulator is not supported", isIPAddress());
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Test
  public void testListFileStatus() throws Exception {
    // crate file using abfs
    AzureBlobFileSystem fs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse(fs.getIsNamespaceEnabled());

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path path1 = new Path("/testfiles/~12/!008/3/abFsTestfile");
    try(FSDataOutputStream abfsStream = fs.create(path1, true)) {
      abfsStream.write(ABFS_TEST_CONTEXT.getBytes());
      abfsStream.flush();
      abfsStream.hsync();
    }

    // create file using wasb
    Path path2 = new Path("/testfiles/~12/!008/3/nativeFsTestfile");
    LOG.info("{}", wasb.getUri());
    try(FSDataOutputStream nativeFsStream = wasb.create(path2, true)) {
      nativeFsStream.write(WASB_TEST_CONTEXT.getBytes());
      nativeFsStream.flush();
      nativeFsStream.hsync();
    }
    // list file using abfs and wasb
    FileStatus[] abfsFileStatus = fs.listStatus(new Path("/testfiles/~12/!008/3/"));
    FileStatus[] nativeFsFileStatus = wasb.listStatus(new Path("/testfiles/~12/!008/3/"));

    assertEquals(2, abfsFileStatus.length);
    assertEquals(2, nativeFsFileStatus.length);
  }

  @Test
  public void testReadFile() throws Exception {
    boolean[] createFileWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readFileWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse(abfs.getIsNamespaceEnabled());

    NativeAzureFileSystem wasb = getWasbFileSystem();

    for (int i = 0; i< 4; i++) {
      Path path = new Path("/testReadFile/~12/!008/testfile" + i);
      final FileSystem createFs = createFileWithAbfs[i] ? abfs : wasb;

      // Write
      try(FSDataOutputStream nativeFsStream = createFs.create(path, true)) {
        nativeFsStream.write(TEST_CONTEXT.getBytes());
        nativeFsStream.flush();
        nativeFsStream.hsync();
      }

      // Check file status
      ContractTestUtils.assertIsFile(createFs, path);

      // Read
      final FileSystem readFs = readFileWithAbfs[i] ? abfs : wasb;

      try(BufferedReader br =new BufferedReader(new InputStreamReader(readFs.open(path)))) {
        String line = br.readLine();
        assertEquals("Wrong text from " + readFs,
            TEST_CONTEXT, line);
      }

      // Remove file
      assertDeleted(readFs, path, true);
    }
  }

  @Test
  public void testDir() throws Exception {
    boolean[] createDirWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readDirWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse(abfs.getIsNamespaceEnabled());

    NativeAzureFileSystem wasb = getWasbFileSystem();

    for (int i = 0; i < 4; i++) {
      Path path = new Path("/testDir/t" + i);
      //create
      final FileSystem createFs = createDirWithAbfs[i] ? abfs : wasb;
      assertTrue(createFs.mkdirs(path));
      //check
      assertPathExists(createFs, "Created dir not found with " + createFs, path);
      //read
      final FileSystem readFs = readDirWithAbfs[i] ? abfs : wasb;
      assertPathExists(readFs, "Created dir not found with " + readFs,
          path);
      assertIsDirectory(readFs, path);
      assertDeleted(readFs, path, true);
    }
  }


  @Test
  public void testUrlConversion(){
    String abfsUrl = "abfs://abcde-1111-1111-1111-1111@xxxx.dfs.xxx.xxx.xxxx.xxxx";
    String wabsUrl = "wasb://abcde-1111-1111-1111-1111@xxxx.blob.xxx.xxx.xxxx.xxxx";
    assertEquals(abfsUrl, wasbUrlToAbfsUrl(wabsUrl));
    assertEquals(wabsUrl, abfsUrlToWasbUrl(abfsUrl));
  }

  @Test
  public void testSetWorkingDirectory() throws Exception {
    //create folders
    AzureBlobFileSystem abfs = getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse(abfs.getIsNamespaceEnabled());

    NativeAzureFileSystem wasb = getWasbFileSystem();

    Path d1d4 = new Path("/d1/d2/d3/d4");
    assertMkdirs(abfs, d1d4);

    //set working directory to path1
    Path path1 = new Path("/d1/d2");
    wasb.setWorkingDirectory(path1);
    abfs.setWorkingDirectory(path1);
    assertEquals(path1, wasb.getWorkingDirectory());
    assertEquals(path1, abfs.getWorkingDirectory());

    //set working directory to path2
    Path path2 = new Path("d3/d4");
    wasb.setWorkingDirectory(path2);
    abfs.setWorkingDirectory(path2);

    Path path3 = d1d4;
    assertEquals(path3, wasb.getWorkingDirectory());
    assertEquals(path3, abfs.getWorkingDirectory());
  }
}
