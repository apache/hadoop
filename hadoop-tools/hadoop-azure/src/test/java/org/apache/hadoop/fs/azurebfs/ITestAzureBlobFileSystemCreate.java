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
import java.util.EnumSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.GenericTestUtils;

import static org.apache.hadoop.fs.contract.ContractTestUtils.assertIsFile;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

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
   * {@link AbfsOutputStream#incrementWriteOps()}
   *
   * @throws Exception
   */
  @Test
  public void testWriteOpsMetric() throws Exception {
    describe("Test to see correct population of write operations in Abfs");
    final AzureBlobFileSystem fs = getFileSystem();
    Path smallFile = new Path("testOneCall");
    Path largeFile = new Path("testLargeCalls");
    String testWriteOps = "test";
    FileSystem.Statistics statistics = fs.getFsStatistics();
    statistics.reset();

    //Test for zero write operation
    Assert.assertEquals(0, statistics.getWriteOps());

    FSDataOutputStream outForOneCall = fs.create(smallFile);
    statistics.reset();
    //Writing "test" 1 time
    outForOneCall
        .write(testWriteOps.getBytes(), 0, testWriteOps.getBytes().length);

    //Test for one write operation
    Assert.assertEquals(1, statistics.getWriteOps());

    outForOneCall.close();
    //validating Content of file
    Assert.assertEquals(true, validateContent(fs, smallFile,
        testWriteOps.getBytes()));

    String largeFileValidationString = "";
    FSDataOutputStream outForLargeCalls = fs.create(largeFile);
    statistics.reset();
    //Writing "test" 1000 times
    for (int i = 0; i < 1000; i++) {
      outForLargeCalls.write(testWriteOps.getBytes(), 0,
          testWriteOps.getBytes().length);

      //Creating Validation string of "test" 1000 times
      largeFileValidationString += testWriteOps;
    }

    //Test for thousand write operations
    Assert.assertEquals(1000, statistics.getWriteOps());

    outForLargeCalls.close();
    //Validating if actually "test" is being written thousand times in largeFile
    Assert.assertEquals(true, validateContent(fs, largeFile,
        largeFileValidationString.getBytes()));
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
    fs.mkdirs(TEST_FOLDER_PATH);
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
    } catch (FileNotFoundException fnfe) {
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

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testFilterFSWriteAfterClose() throws Throwable {
    final AzureBlobFileSystem fs = getFileSystem();
    Path testPath = new Path(TEST_FOLDER_PATH, TEST_CHILD_FILE);
    FSDataOutputStream out = fs.create(testPath);
    intercept(FileNotFoundException.class,
        () -> {
          try (FilterOutputStream fos = new FilterOutputStream(out)) {
            fos.write('a');
            fos.flush();
            out.hsync();
            fs.delete(testPath, false);
            // trigger the first failure
            throw intercept(FileNotFoundException.class,
                () -> {
              fos.write('b');
              out.hsync();
              return "hsync didn't raise an IOE";
            });
          }
        });
  }

}
