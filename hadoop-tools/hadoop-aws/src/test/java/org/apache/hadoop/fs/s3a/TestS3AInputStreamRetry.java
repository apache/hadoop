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
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.junit.Test;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.audit.impl.NoopSpan;
import org.apache.hadoop.fs.s3a.auth.delegation.EncryptionSecrets;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectReadParameters;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamCallbacks;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.http.NoHttpResponseException;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.requestRange;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.sdkClientException;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_416_RANGE_NOT_SATISFIABLE;
import static org.apache.hadoop.util.functional.FutureIO.eval;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests S3AInputStream retry behavior on read failure.
 * <p>
 * These tests are for validating expected behavior of retrying the
 * S3AInputStream read() and read(b, off, len), it tests that the read should
 * reopen the input stream and retry the read when IOException is thrown
 * during the read process.
 * <p>
 * This includes handling of out of range requests.
 */
public class TestS3AInputStreamRetry extends AbstractS3AMockTest {

  /**
   * Test input stream content: charAt(x) == hex value of x.
   */
  private static final String INPUT = "012345678ABCDEF";

  /**
   * Status code to raise by default.
   */
  public static final int STATUS = 0;

  @Test
  public void testInputStreamReadRetryForException() throws IOException {
    S3AInputStream s3AInputStream = getMockedS3AInputStream(failingInputStreamCallbacks(
        awsServiceException(STATUS)));
    assertEquals("'0' from the test input stream should be the first " +
        "character being read", INPUT.charAt(0), s3AInputStream.read());
    assertEquals("'1' from the test input stream should be the second " +
        "character being read", INPUT.charAt(1), s3AInputStream.read());
  }

  @Test
  public void testInputStreamReadLengthRetryForException() throws IOException {
    byte[] result = new byte[INPUT.length()];
    S3AInputStream s3AInputStream = getMockedS3AInputStream(
        failingInputStreamCallbacks(awsServiceException(STATUS)));
    s3AInputStream.read(result, 0, INPUT.length());

    assertArrayEquals(
        "The read result should equals to the test input stream content",
        INPUT.getBytes(), result);
  }

  @Test
  public void testInputStreamReadFullyRetryForException() throws IOException {
    byte[] result = new byte[INPUT.length()];
    S3AInputStream s3AInputStream = getMockedS3AInputStream(failingInputStreamCallbacks(
        awsServiceException(STATUS)));
    s3AInputStream.readFully(0, result);

    assertArrayEquals(
        "The read result should equals to the test input stream content",
        INPUT.getBytes(), result);
  }

  /**
   * Seek and read repeatedly with every second GET failing with {@link NoHttpResponseException}.
   * This should be effective in simulating {@code reopen()} failures caused by network problems.
   */
  @Test
  public void testReadMultipleSeeksNoHttpResponse() throws Throwable {
    final RuntimeException ex = sdkClientException(new NoHttpResponseException("no response"));
    // fail on even reads
    S3AInputStream stream = getMockedS3AInputStream(
        maybeFailInGetCallback(ex, (index) -> (index % 2 == 0)));
    // 10 reads with repeated failures.
    for (int i = 0; i < 10; i++) {
      stream.seek(0);
      final int r = stream.read();
      assertReadValueMatchesOffset(r, 0, "read attempt " + i + " of " + stream);
    }
  }

  /**
   * Seek and read repeatedly with every second GET failing with {@link NoHttpResponseException}.
   * This should be effective in simulating {@code reopen()} failures caused by network problems.
   */
  @Test
  public void testReadMultipleSeeksStreamClosed() throws Throwable {
    final RuntimeException ex = sdkClientException(new NoHttpResponseException("no response"));
    // fail on even reads
    S3AInputStream stream = getMockedS3AInputStream(
        maybeFailInGetCallback(ex, (index) -> (index % 2 == 0)));
    // 10 reads with repeated failures.
    for (int i = 0; i < 10; i++) {
      stream.seek(0);
      final int r = stream.read();
      assertReadValueMatchesOffset(r, 0, "read attempt " + i + " of " + stream);
    }
  }

  /**
   * Assert that the result of read() matches the char at the expected offset.
   * @param r read result
   * @param pos pos in stream
   * @param text text for error string.
   */
  private static void assertReadValueMatchesOffset(
      final int r, final int pos, final String text) {
    Assertions.assertThat(r)
        .describedAs("read() at %d of %s", pos, text)
        .isGreaterThan(-1);
    Assertions.assertThat(Character.toString((char) r))
        .describedAs("read() at %d of %s", pos, text)
        .isEqualTo(String.valueOf(INPUT.charAt(pos)));
  }

  /**
   * Create a mocked input stream for a given callback.
   * @param streamCallback callback to use on GET calls
   * @return a stream.
   */
  private S3AInputStream getMockedS3AInputStream(
      ObjectInputStreamCallbacks streamCallback) {
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

    ObjectReadParameters parameters = new ObjectReadParameters()
        .withCallbacks(streamCallback)
        .withObjectAttributes(s3ObjectAttributes)
        .withContext(s3AReadOpContext)
        .withStreamStatistics(
            s3AReadOpContext.getS3AStatisticsContext().newInputStreamStatistics())
        .withBoundedThreadPool(null);

    return new S3AInputStream(parameters);
  }

  /**
   * Create mocked InputStreamCallbacks which returns a mocked S3Object and fails on
   * the third invocation.
   * This is the original mock stream used in this test suite; the failure logic and stream
   * selection has been factored out to support different failure modes.
   * @param ex exception to raise on failure
   * @return mocked object.
   */
  private ObjectInputStreamCallbacks failingInputStreamCallbacks(
      final RuntimeException ex) {

    GetObjectResponse objectResponse = GetObjectResponse.builder()
        .eTag("test-etag")
        .build();

    final SSLException ioe = new SSLException(new SocketException("Connection reset"));

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

    return mockInputStreamCallback(ex,
        attempt -> 3 == attempt,
        attempt ->  mockedInputStream(objectResponse, attempt < 3, ioe));
  }

  /**
   * Create mocked InputStreamCallbacks which returns a mocked S3Object and fails
   * when the the predicate indicates that it should.
   * The stream response itself does not fail.
   * @param ex exception to raise on failure
   * @return mocked object.
   */
  private ObjectInputStreamCallbacks maybeFailInGetCallback(
      final RuntimeException ex,
      final Function<Integer, Boolean> failurePredicate) {
    GetObjectResponse objectResponse = GetObjectResponse.builder()
        .eTag("test-etag")
        .build();

    return mockInputStreamCallback(ex,
        failurePredicate,
        attempt -> mockedInputStream(objectResponse, false, null));
  }

 /**
  * Create mocked InputStreamCallbacks which returns a mocked S3Object.
  * Raises the given runtime exception if the failure predicate returns true;
  * the stream factory returns the input stream for the given attempt.
  * @param ex exception to raise on failure
  * @param failurePredicate predicate which, when true, triggers a failure on the given attempt.
  * @param streamFactory factory for the stream to return on the given attempt.
  * @return mocked object.
  */
  private ObjectInputStreamCallbacks mockInputStreamCallback(
      final RuntimeException ex,
      final Function<Integer, Boolean> failurePredicate,
      final Function<Integer, ResponseInputStream<GetObjectResponse>> streamFactory) {


    return new ObjectInputStreamCallbacks() {
      private int attempt = 0;

      @Override
      public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
        attempt++;

        if (failurePredicate.apply(attempt)) {
          throw ex;
        }
        final Pair<Long, Long> r = requestRange(request.range());
        final int start = r.getLeft().intValue();
        final int end = r.getRight().intValue();
        if (start < 0 || end < 0 || start > end) {
          // not satisfiable
          throw awsServiceException(SC_416_RANGE_NOT_SATISFIABLE);
        }

        final ResponseInputStream<GetObjectResponse> stream = streamFactory.apply(attempt);

        // skip the given number of bytes from the start of the array; no-op if 0.
        try {
          stream.skip(start);
        } catch (IOException e) {
          throw sdkClientException(e);
        }
        return stream;
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
   * Create an AwsServiceException with the given status code.
   *
   * @param status HTTP status code
   * @return an exception.
   */
  private static AwsServiceException awsServiceException(int status) {
    return AwsServiceException.builder()
        .message("Failed to get S3Object")
        .statusCode(status)
        .awsErrorDetails(AwsErrorDetails.builder().errorCode("test-code").build())
        .build();
  }

  /**
   * Get mocked ResponseInputStream<GetObjectResponse> where we can trigger IOException to
   * simulate the read failure.
   *
   * @param triggerFailure true when a failure injection is enabled in read()
   * @param ioe exception to raise
   * @return mocked object.
   */
  private ResponseInputStream<GetObjectResponse> mockedInputStream(
      GetObjectResponse objectResponse,
      boolean triggerFailure,
      final IOException ioe) {

    FilterInputStream inputStream =
        new FilterInputStream(IOUtils.toInputStream(INPUT, StandardCharsets.UTF_8)) {

          @Override
          public int read() throws IOException {
            int result = super.read();
            if (triggerFailure) {
              throw ioe;
            }
            return result;
          }

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            int result = super.read(b, off, len);
            if (triggerFailure) {
              throw ioe;
            }
            return result;
          }
        };

    return new ResponseInputStream(objectResponse,
        AbortableInputStream.create(inputStream, () -> {}));
  }
}
