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
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.impl.NoopSpan;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.util.functional.CallableRaisingIOE;


import static java.lang.Math.min;
import static org.apache.hadoop.util.functional.FutureIO.eval;
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
        s3AFileStatus,
        NoopSpan.INSTANCE);

    return new S3AInputStream(
        s3AReadOpContext,
        s3ObjectAttributes,
        getMockedInputStreamCallback(),
        s3AReadOpContext.getS3AStatisticsContext().newInputStreamStatistics(),
            null);
  }

  /**
   * Get mocked InputStreamCallbacks where we return mocked S3Object.
   *
   * @return mocked object.
   */
  private S3AInputStream.InputStreamCallbacks getMockedInputStreamCallback() {
    GetObjectResponse objectResponse = GetObjectResponse.builder()
        .eTag("test-etag")
        .build();

    ResponseInputStream<GetObjectResponse>[] responseInputStreams =
        new ResponseInputStream[] {
            getMockedInputStream(objectResponse, true),
            getMockedInputStream(objectResponse, true),
            getMockedInputStream(objectResponse, false)
        };

    return new S3AInputStream.InputStreamCallbacks() {
      private Integer mockedS3ObjectIndex = 0;

      @Override
      public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
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
          throw AwsServiceException.builder()
              .message("Failed to get S3Object")
              .awsErrorDetails(AwsErrorDetails.builder().errorCode("test-code").build())
              .build();
        }
        return responseInputStreams[min(mockedS3ObjectIndex, responseInputStreams.length) - 1];
      }

      @Override
      public GetObjectRequest.Builder newGetRequestBuilder(String key) {
        return GetObjectRequest.builder().bucket(fs.getBucket()).key(key);
      }

      @Override
      public <T> CompletableFuture<T> submit(final CallableRaisingIOE<T> operation) {
        return eval(operation);
      }

      @Override
      public void close() {
      }
    };
  }

  /**
   * Get mocked ResponseInputStream<GetObjectResponse> where we can trigger IOException to
   * simulate the read failure.
   *
   * @param triggerFailure true when a failure injection is enabled.
   * @return mocked object.
   */
  private ResponseInputStream<GetObjectResponse> getMockedInputStream(
      GetObjectResponse objectResponse, boolean triggerFailure) {

    FilterInputStream inputStream =
        new FilterInputStream(IOUtils.toInputStream(INPUT, StandardCharsets.UTF_8)) {

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

    return new ResponseInputStream(objectResponse,
        AbortableInputStream.create(inputStream, () -> {}));
  }
}
