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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azure.NativeAzureFileSystem;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * Test compatibility between ABFS client and WASB client.
 */
public class ITestWasbAbfsCompatibility extends DependencyInjectedTest {
  private static final String WASB_TEST_CONTEXT = "wasb test file";
  private static final String ABFS_TEST_CONTEXT = "abfs test file";
  private static final String TEST_CONTEXT = "THIS IS FOR TEST";

  public ITestWasbAbfsCompatibility() throws Exception {
    super();

    Assume.assumeFalse(this.isEmulator());
  }

  @Test
  public void testListFileStatus() throws Exception {
    // crate file using abfs
    AzureBlobFileSystem fs = this.getFileSystem();
    NativeAzureFileSystem wasb = this.getWasbFileSystem();

    Path path1 = new Path("/testfiles/~12/!008/3/abFsTestfile");
    FSDataOutputStream abfsStream = fs.create(path1, true);
    abfsStream.write(ABFS_TEST_CONTEXT.getBytes());
    abfsStream.flush();
    abfsStream.hsync();
    abfsStream.close();

    // create file using wasb
    Path path2 = new Path("/testfiles/~12/!008/3/nativeFsTestfile");
    System.out.println(wasb.getUri());
    FSDataOutputStream nativeFsStream = wasb.create(path2, true);
    nativeFsStream.write(WASB_TEST_CONTEXT.getBytes());
    nativeFsStream.flush();
    nativeFsStream.hsync();
    nativeFsStream.close();
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

    AzureBlobFileSystem abfs = this.getFileSystem();
    NativeAzureFileSystem wasb = this.getWasbFileSystem();

    FileSystem fs;
    BufferedReader br = null;
    for (int i = 0; i< 4; i++) {
      try {
        Path path = new Path("/testfiles/~12/!008/testfile" + i);
        if (createFileWithAbfs[i]) {
          fs = abfs;
        } else {
          fs = wasb;
        }

        // Write
        FSDataOutputStream nativeFsStream = fs.create(path, true);
        nativeFsStream.write(TEST_CONTEXT.getBytes());
        nativeFsStream.flush();
        nativeFsStream.hsync();
        nativeFsStream.close();

        // Check file status
        assertEquals(true, fs.exists(path));
        assertEquals(false, fs.getFileStatus(path).isDirectory());

        // Read
        if (readFileWithAbfs[i]) {
          fs = abfs;
        } else {
          fs = wasb;
        }
        FSDataInputStream inputStream = fs.open(path);
        br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();
        assertEquals(TEST_CONTEXT, line);

        // Remove file
        fs.delete(path, true);
        assertFalse(fs.exists(path));
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (br != null) {
          br.close();
        }
      }
    }
  }

  @Test
  public void testDir() throws Exception {
    boolean[] createDirWithAbfs = new boolean[]{false, true, false, true};
    boolean[] readDirWithAbfs = new boolean[]{false, true, true, false};

    AzureBlobFileSystem abfs = this.getFileSystem();
    NativeAzureFileSystem wasb = this.getWasbFileSystem();

    FileSystem fs;
    for (int i = 0; i < 4; i++) {
      Path path = new Path("/testDir/t" + i);
      //create
      if (createDirWithAbfs[i]) {
        fs = abfs;
      } else {
        fs = wasb;
      }
      assertTrue(fs.mkdirs(path));
      //check
      assertTrue(fs.exists(path));
      //read
      if (readDirWithAbfs[i]) {
        fs = abfs;
      } else {
        fs = wasb;
      }
      assertTrue(fs.exists(path));
      FileStatus dirStatus = fs.getFileStatus(path);
      assertTrue(dirStatus.isDirectory());
      fs.delete(path, true);
      assertFalse(fs.exists(path));
    }
  }


  @Test
  public void testUrlConversion(){
    String abfsUrl = "abfs://abcde-1111-1111-1111-1111@xxxx.dfs.xxx.xxx.xxxx.xxxx";
    String wabsUrl = "wasb://abcde-1111-1111-1111-1111@xxxx.blob.xxx.xxx.xxxx.xxxx";
    Assert.assertEquals(abfsUrl, wasbUrlToAbfsUrl(wabsUrl));
    Assert.assertEquals(wabsUrl, abfsUrlToWasbUrl(abfsUrl));
  }

  @Test
  public void testSetWorkingDirectory() throws Exception {
    //create folders
    AzureBlobFileSystem abfs = this.getFileSystem();
    NativeAzureFileSystem wasb = this.getWasbFileSystem();

    assertTrue(abfs.mkdirs(new Path("/d1/d2/d3/d4")));

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

    Path path3 = new Path("/d1/d2/d3/d4");
    assertEquals(path3, wasb.getWorkingDirectory());
    assertEquals(path3, abfs.getWorkingDirectory());
  }
}