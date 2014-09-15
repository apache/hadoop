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

package org.apache.hadoop.fs.s3a;

import static org.junit.Assume.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;

/**
 *  Tests a live S3 system. If you keys and bucket aren't specified, all tests 
 *  are marked as passed 
 *  
 *  This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from 
 *  TestCase which uses the old Junit3 runner that doesn't ignore assumptions 
 *  properly making it impossible to skip the tests if we don't have a valid
 *  bucket.
 **/
public class S3AFileSystemContractBaseTest extends FileSystemContractBaseTest {
  private static final int TEST_BUFFER_SIZE = 128;
  private static final int MODULUS = 128;

  protected static final Logger LOG = LoggerFactory.getLogger(S3AFileSystemContractBaseTest.class);

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    URI testURI = URI.create(conf.get("test.fs.s3a.name"));
    
    boolean liveTest = testURI != null && !testURI.equals("s3a:///");
    
    // This doesn't work with our JUnit 3 style test cases, so instead we'll 
    // make this whole class not run by default
    assumeTrue(liveTest);
    
    fs = new S3AFileSystem();
    fs.initialize(testURI, conf);
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
	if (fs != null) {
	  fs.delete(path("/tests3a"), true);
	}
	super.tearDown();
  }

  @Test(timeout = 10000)
  public void testMkdirs() throws IOException {
    // No trailing slash
    assertTrue(fs.mkdirs(path("/tests3a/a")));
    assertTrue(fs.exists(path("/tests3a/a")));

    // With trailing slash
    assertTrue(fs.mkdirs(path("/tests3a/b/")));
    assertTrue(fs.exists(path("/tests3a/b/")));

    // Two levels deep
    assertTrue(fs.mkdirs(path("/tests3a/c/a/")));
    assertTrue(fs.exists(path("/tests3a/c/a/")));

    // Mismatched slashes
    assertTrue(fs.exists(path("/tests3a/c/a")));
  }


  @Test(timeout=20000)
  public void testDelete() throws IOException {
    // Test deleting an empty directory
    assertTrue(fs.mkdirs(path("/tests3a/d")));
    assertTrue(fs.delete(path("/tests3a/d"), true));
    assertFalse(fs.exists(path("/tests3a/d")));

    // Test deleting a deep empty directory
    assertTrue(fs.mkdirs(path("/tests3a/e/f/g/h")));
    assertTrue(fs.delete(path("/tests3a/e/f/g"), true));
    assertFalse(fs.exists(path("/tests3a/e/f/g/h")));
    assertFalse(fs.exists(path("/tests3a/e/f/g")));
    assertTrue(fs.exists(path("/tests3a/e/f")));

    // Test delete of just a file
    writeFile(path("/tests3a/f/f/file"), 1000);
    assertTrue(fs.exists(path("/tests3a/f/f/file")));
    assertTrue(fs.delete(path("/tests3a/f/f/file"), false));
    assertFalse(fs.exists(path("/tests3a/f/f/file")));


    // Test delete of a path with files in various directories
    writeFile(path("/tests3a/g/h/i/file"), 1000);
    assertTrue(fs.exists(path("/tests3a/g/h/i/file")));
    writeFile(path("/tests3a/g/h/j/file"), 1000);
    assertTrue(fs.exists(path("/tests3a/g/h/j/file")));
    try {
      assertFalse(fs.delete(path("/tests3a/g/h"), false));
      fail("Expected delete to fail with recursion turned off");
    } catch (IOException e) {}
    assertTrue(fs.exists(path("/tests3a/g/h/j/file")));
    assertTrue(fs.delete(path("/tests3a/g/h"), true));
    assertFalse(fs.exists(path("/tests3a/g/h/j")));
  }


  @Test(timeout = 3600000)
  public void testOpenCreate() throws IOException {
    try {
      createAndReadFileTest(1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      createAndReadFileTest(5 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      createAndReadFileTest(20 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    /*
    Enable to test the multipart upload
    try {
      createAndReadFileTest((long)6 * 1024 * 1024 * 1024);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    */
  }

  @Test(timeout = 1200000)
  public void testRenameFile() throws IOException {
    Path srcPath = path("/tests3a/a/srcfile");

    final OutputStream outputStream = fs.create(srcPath, false);
    generateTestData(outputStream, 11 * 1024 * 1024);
    outputStream.close();

    assertTrue(fs.exists(srcPath));

    Path dstPath = path("/tests3a/b/dstfile");

    assertFalse(fs.rename(srcPath, dstPath));
    assertTrue(fs.mkdirs(dstPath.getParent()));
    assertTrue(fs.rename(srcPath, dstPath));
    assertTrue(fs.exists(dstPath));
    assertFalse(fs.exists(srcPath));
    assertTrue(fs.exists(srcPath.getParent()));
  }


  @Test(timeout = 10000)
  public void testRenameDirectory() throws IOException {
    Path srcPath = path("/tests3a/a");

    assertTrue(fs.mkdirs(srcPath));
    writeFile(new Path(srcPath, "b/testfile"), 1024);

    Path nonEmptyPath = path("/tests3a/nonempty");
    writeFile(new Path(nonEmptyPath, "b/testfile"), 1024);

    assertFalse(fs.rename(srcPath, nonEmptyPath));

    Path dstPath = path("/tests3a/b");
    assertTrue(fs.rename(srcPath, dstPath));
    assertFalse(fs.exists(srcPath));
    assertTrue(fs.exists(new Path(dstPath, "b/testfile")));
  }


  @Test(timeout=10000)
  public void testSeek() throws IOException {
    Path path = path("/tests3a/testfile.seek");
    writeFile(path, TEST_BUFFER_SIZE * 10);


    FSDataInputStream inputStream = fs.open(path, TEST_BUFFER_SIZE);
    inputStream.seek(inputStream.getPos() + MODULUS);

    testReceivedData(inputStream, TEST_BUFFER_SIZE * 10 - MODULUS);
  }

  /**
   * Creates and reads a file with the given size in S3. The test file is 
   * generated according to a specific pattern.
   * During the read phase the incoming data stream is also checked against this pattern.
   *
   * @param fileSize
   * the size of the file to be generated in bytes
   * @throws IOException
   * thrown if an I/O error occurs while writing or reading the test file
   */
  private void createAndReadFileTest(final long fileSize) throws IOException {
    final String objectName = UUID.randomUUID().toString();
    final Path objectPath = new Path("/tests3a/", objectName);

    // Write test file to S3
    final OutputStream outputStream = fs.create(objectPath, false);
    generateTestData(outputStream, fileSize);
    outputStream.close();

    // Now read the same file back from S3
    final InputStream inputStream = fs.open(objectPath);
    testReceivedData(inputStream, fileSize);
    inputStream.close();

    // Delete test file
    fs.delete(objectPath, false);
  }


  /**
   * Receives test data from the given input stream and checks the size of the 
   * data as well as the pattern inside the received data.
   *
   * @param inputStream
   * the input stream to read the test data from
   * @param expectedSize
   * the expected size of the data to be read from the input stream in bytes
   * @throws IOException
   * thrown if an error occurs while reading the data
   */
  private void testReceivedData(final InputStream inputStream, 
    final long expectedSize) throws IOException {
    final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];

    long totalBytesRead = 0;
    int nextExpectedNumber = 0;
    while (true) {
      final int bytesRead = inputStream.read(testBuffer);
      if (bytesRead < 0) {
        break;
      }

      totalBytesRead += bytesRead;

      for (int i = 0; i < bytesRead; ++i) {
        if (testBuffer[i] != nextExpectedNumber) {
          throw new IOException("Read number " + testBuffer[i] + " but expected "
                                + nextExpectedNumber);
        }

        ++nextExpectedNumber;

        if (nextExpectedNumber == MODULUS) {
          nextExpectedNumber = 0;
        }
      }
    }

    if (totalBytesRead != expectedSize) {
      throw new IOException("Expected to read " + expectedSize + 
                            " bytes but only received " + totalBytesRead);
    }
  }


  /**
   * Generates test data of the given size according to some specific pattern
   * and writes it to the provided output stream.
   *
   * @param outputStream
   * the output stream to write the data to
   * @param size
   * the size of the test data to be generated in bytes
   * @throws IOException
   * thrown if an error occurs while writing the data
   */
  private void generateTestData(final OutputStream outputStream, 
    final long size) throws IOException {

    final byte[] testBuffer = new byte[TEST_BUFFER_SIZE];
    for (int i = 0; i < testBuffer.length; ++i) {
      testBuffer[i] = (byte) (i % MODULUS);
    }

    long bytesWritten = 0;
    while (bytesWritten < size) {

      final long diff = size - bytesWritten;
      if (diff < testBuffer.length) {
        outputStream.write(testBuffer, 0, (int)diff);
        bytesWritten += diff;
      } else {
        outputStream.write(testBuffer);
        bytesWritten += testBuffer.length;
      }
    }
  }

  private void writeFile(Path name, int fileSize) throws IOException {
    final OutputStream outputStream = fs.create(name, false);
    generateTestData(outputStream, fileSize);
    outputStream.close();
  }
}
