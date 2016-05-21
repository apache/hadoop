/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.contract.s3a.S3AContract;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.fs.contract.ContractTestUtils.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;

/**
 * Test S3A Failure translation, including a functional test
 * generating errors during stream IO.
 */
public class TestS3AFailureHandling extends AbstractFSContractTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3AFailureHandling.class);

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new S3AContract(conf);
  }

  @Test
  public void testReadFileChanged() throws Throwable {
    describe("overwrite a file with a shorter one during a read, seek");
    final int fullLength = 8192;
    final byte[] fullDataset = dataset(fullLength, 'a', 32);
    final int shortLen = 4096;
    final byte[] shortDataset = dataset(shortLen, 'A', 32);
    final FileSystem fs = getFileSystem();
    final Path testpath = path("readFileToChange.txt");
    // initial write
    writeDataset(fs, testpath, fullDataset, fullDataset.length, 1024, false);
    try(FSDataInputStream instream = fs.open(testpath)) {
      instream.seek(fullLength - 16);
      assertTrue("no data to read", instream.read() >= 0);
      // overwrite
      writeDataset(fs, testpath, shortDataset, shortDataset.length, 1024, true);
      // here the file length is less. Probe the file to see if this is true,
      // with a spin and wait
      eventually(30 *1000, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          assertEquals(shortLen, fs.getFileStatus(testpath).getLen());
          return null;
        }
      });
      // here length is shorter. Assuming it has propagated to all replicas,
      // the position of the input stream is now beyond the EOF.
      // An attempt to seek backwards to a position greater than the
      // short length will raise an exception from AWS S3, which must be
      // translated into an EOF

      instream.seek(shortLen + 1024);
      int c = instream.read();
      assertIsEOF("read()", c);

      byte[] buf = new byte[256];

      assertIsEOF("read(buffer)", instream.read(buf));
      assertIsEOF("read(offset)",
          instream.read(instream.getPos(), buf, 0, buf.length));

      // now do a block read fully, again, backwards from the current pos
      try {
        instream.readFully(shortLen + 512, buf);
        fail("Expected readFully to fail");
      } catch (EOFException expected) {
        LOG.debug("Expected EOF: ", expected);
      }

      assertIsEOF("read(offset)",
          instream.read(shortLen + 510, buf, 0, buf.length));

      // seek somewhere useful
      instream.seek(shortLen - 256);

      // delete the file. Reads must fail
      fs.delete(testpath, false);

      try {
        int r = instream.read();
        fail("Expected an exception, got " + r);
      } catch (FileNotFoundException e) {
        // expected
      }

      try {
        instream.readFully(2048, buf);
        fail("Expected readFully to fail");
      } catch (FileNotFoundException e) {
        // expected
      }

    }
  }

  /**
   * Assert that a read operation returned an EOF value.
   * @param operation specific operation
   * @param readResult result
   */
  private void assertIsEOF(String operation, int readResult) {
    assertEquals("Expected EOF from "+ operation
        + "; got char " + (char) readResult, -1, readResult);
  }

  @Test
  public void test404isNotFound() throws Throwable {
    verifyTranslated(FileNotFoundException.class, createS3Exception(404));
  }

  protected Exception verifyTranslated(Class clazz,
      AmazonClientException exception) throws Exception {
    return verifyExceptionClass(clazz,
        translateException("test", "/", exception));
  }

  @Test
  public void test401isNotPermittedFound() throws Throwable {
    verifyTranslated(AccessDeniedException.class,
        createS3Exception(401));
  }

  protected AmazonS3Exception createS3Exception(int code) {
    AmazonS3Exception source = new AmazonS3Exception("");
    source.setStatusCode(code);
    return source;
  }

  @Test
  public void testGenericS3Exception() throws Throwable {
    // S3 exception of no known type
    AWSS3IOException ex = (AWSS3IOException)verifyTranslated(
        AWSS3IOException.class,
        createS3Exception(451));
    assertEquals(451, ex.getStatusCode());
  }

  @Test
  public void testGenericServiceS3Exception() throws Throwable {
    // service exception of no known type
    AmazonServiceException ase = new AmazonServiceException("unwind");
    ase.setStatusCode(500);
    AWSServiceIOException ex = (AWSServiceIOException)verifyTranslated(
        AWSServiceIOException.class,
        ase);
    assertEquals(500, ex.getStatusCode());
  }

  @Test
  public void testGenericClientException() throws Throwable {
    // Generic Amazon exception
    verifyTranslated(AWSClientIOException.class,
        new AmazonClientException(""));
  }

}
