/*
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

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.Charset;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.impl.NoopSpan;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;

import static java.lang.Math.min;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests S3AInputStream retry behavior on read failure.
 * These tests are for validating expected behavior of retrying the
 * S3AInputStream read() and read(b, off, len), it tests that the read should
 * reopen the input stream and retry the read when IOException is thrown
 * during the read process.
 */
public class TestS3AInputStreamRetry extends AbstractS3AMockTest {

  private static final String INPUT = "ab";

  @Test
  public void testInputStreamReadRetryForException() throws IOException {
    S3AInputStream s3AInputStream = getMockedS3AInputStream();

    assertEquals("'a' from the test input stream 'ab' should be the first " +
        "character being read", INPUT.charAt(0), s3AInputStream.read());
    assertEquals("'b' from the test input stream 'ab' should be the second " +
        "character being read", INPUT.charAt(1), s3AInputStream.read());
  }

  @Test
  public void testInputStreamReadLengthRetryForException() throws IOException {
    byte[] result = new byte[INPUT.length()];
    S3AInputStream s3AInputStream = getMockedS3AInputStream();
    s3AInputStream.read(result, 0, INPUT.length());

    assertArrayEquals(
        "The read result should equals to the test input stream content",
        INPUT.getBytes(), result);
  }

  @Test
  public void testInputStreamReadFullyRetryForException() throws IOException {
    byte[] result = new byte[INPUT.length()];
    S3AInputStream s3AInputStream = getMockedS3AInputStream();
    s3AInputStream.readFully(0, result);

    assertArrayEquals(
        "The read result should equals to the test input stream content",
        INPUT.getBytes(), result);
  }

  private S3AInputStream getMockedS3AInputStream() {
    Path path = new Path("test-path");
    String eTag = "test-etag";
    String versionId = "test-version-id";
    String owner = "test-owner";

    S3AFileStatus s3AFileStatus = new S3AFileStatus(
        INPUT.length(), 0, path, INPUT.length(), owner, eTag, versionId);

    S3ObjectAttributes s3ObjectAttributes = new S3ObjectAttributes(
        fs.getBucket(),
        path,
        fs.pathToKey(path),
        fs.getS3EncryptionAlgorithm(),
        new EncryptionSecrets().getEncryptionKey(),
        eTag,
        versionId,
        INPUT.length());

    S3AReadOpContext s3AReadOpContext = fs.createReadContext(
        s3AFileStatus, S3AInputPolicy.Normal,
        ChangeDetectionPolicy.getPolicy(fs.getConf()), 100, NoopSpan.INSTANCE);

    return new S3AInputStream(
        s3AReadOpContext,
        s3ObjectAttributes,
        getMockedInputStreamCallback());
  }

  /**
   * Get mocked InputStreamCallbacks where we return mocked S3Object.
   *
   * @return mocked object.
   */
  private S3AInputStream.InputStreamCallbacks getMockedInputStreamCallback() {
    return new S3AInputStream.InputStreamCallbacks() {

      private final S3Object mockedS3Object = getMockedS3Object();
      private Integer mockedS3ObjectIndex = 0;

      @Override
      public S3Object getObject(GetObjectRequest request) {
        // Set s3 client to return mocked s3object with defined read behavior.
        mockedS3ObjectIndex++;
        // open() -> lazySeek() -> reopen()
        //        -> getObject (mockedS3ObjectIndex=1) -> getObjectContent(objectInputStreamBad1)
        // read() -> objectInputStreamBad1 throws exception
        //        -> onReadFailure -> close wrappedStream
        //  -> retry(1) -> wrappedStream==null -> reopen -> getObject (mockedS3ObjectIndex=2)
        //        -> getObjectContent(objectInputStreamBad2)-> objectInputStreamBad2
        //        -> wrappedStream.read -> objectInputStreamBad2 throws exception
        //        -> onReadFailure -> close wrappedStream
        //  -> retry(2) -> wrappedStream==null -> reopen
        //        -> getObject (mockedS3ObjectIndex=3) throws exception
        //  -> retry(3) -> wrappedStream==null -> reopen -> getObject (mockedS3ObjectIndex=4)
        //        -> getObjectContent(objectInputStreamGood)-> objectInputStreamGood
        //        -> wrappedStream.read
        if (mockedS3ObjectIndex == 3) {
          throw new SdkClientException("Failed to get S3Object");
        }
        return mockedS3Object;
      }

      @Override
      public GetObjectRequest newGetRequest(String key) {
        return new GetObjectRequest(fs.getBucket(), key);
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Get mocked S3Object that returns bad input stream on the initial of
   * getObjectContent calls.
   *
   * @return mocked object.
   */
  private S3Object getMockedS3Object() {
    S3ObjectInputStream objectInputStreamBad1 = getMockedInputStream(true);
    S3ObjectInputStream objectInputStreamBad2 = getMockedInputStream(true);
    S3ObjectInputStream objectInputStreamGood = getMockedInputStream(false);

    return new S3Object() {
      private final S3ObjectInputStream[] inputStreams =
          {objectInputStreamBad1, objectInputStreamBad2, objectInputStreamGood};

      private Integer inputStreamIndex = 0;

      @Override
      public S3ObjectInputStream getObjectContent() {
        // Set getObjectContent behavior:
        // Returns bad stream twice, and good stream afterwards.
        inputStreamIndex++;
        return inputStreams[min(inputStreamIndex, inputStreams.length) - 1];
      }

      @Override
      public ObjectMetadata getObjectMetadata() {
        // Set getObjectMetadata behavior: returns dummy metadata
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setHeader("ETag", "test-etag");
        return metadata;
      }
    };
  }

  /**
   * Get mocked S3ObjectInputStream where we can trigger IOException to
   * simulate the read failure.
   *
   * @param triggerFailure true when a failure injection is enabled.
   * @return mocked object.
   */
  private S3ObjectInputStream getMockedInputStream(boolean triggerFailure) {
    return new S3ObjectInputStream(
        IOUtils.toInputStream(INPUT, Charset.defaultCharset()), null) {

      private final IOException exception =
          new SSLException(new SocketException("Connection reset"));

      @Override
      public int read() throws IOException {
        int result = super.read();
        if (triggerFailure) {
          throw exception;
        }
        return result;
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        int result = super.read(b, off, len);
        if (triggerFailure) {
          throw exception;
        }
        return result;
      }
    };
  }
}
