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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.impl.ChangeDetectionPolicy;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/**
 * Tests S3AInputStream retry behavior on read failure.
 * These tests are for validating expected behavior of retrying the S3AInputStream
 * read() and read(b, off, len), it tests that the read should reopen the input stream and retry
 * the read when IOException is thrown during the read process.
 */
public class TestS3AInputStreamRetry extends AbstractS3AMockTest {

  String input = "ab";

  @Test
  public void testInputStreamReadRetryForException() throws IOException {
    S3AInputStream s3AInputStream = getMockedS3AInputStream();
    assertEquals(input.charAt(0), s3AInputStream.read());
    assertEquals(input.charAt(1), s3AInputStream.read());
  }

  @Test
  public void testInputStreamReadRetryLengthForException() throws IOException {
    byte[] result = new byte[input.length()];
    S3AInputStream s3AInputStream = getMockedS3AInputStream();
    assertEquals(input.length(), s3AInputStream.read(result, 0, input.length()));
    assertEquals(input.charAt(0), result[0]);
    assertEquals(input.charAt(1), result[1]);
  }

  private S3AInputStream getMockedS3AInputStream() {
    Path path = new Path("test-path");
    String eTag = "test-etag";
    String versionId = "test-version-id";
    String owner = "test-owner";

    // Set the fs s3Client with custom spied object
    AmazonS3 client = getMockedAmazonS3Client();
    fs.setAmazonS3Client(client);

    S3AFileStatus s3AFileStatus =
      new S3AFileStatus(input.length(), 0, path, input.length(), owner, eTag, versionId);
    AuditSpan auditSpan = mock(AuditSpan.class);

    return new S3AInputStream(
      fs.createReadContext(
        s3AFileStatus,
        S3AInputPolicy.Normal,
        ChangeDetectionPolicy.getPolicy(fs.getConf()),
        100,
        auditSpan),
      fs.createObjectAttributes(s3AFileStatus),
      fs.createInputStreamCallbacks(auditSpan));
  }

  private AmazonS3 getMockedAmazonS3Client() {
    AmazonS3 client = spy(fs.getAmazonS3Client());
    S3Object s3Object = getMockedS3Object();

    // Set s3 client to return mocked s3object with already defined read behavior
    doReturn(s3Object)
      .when(client)
      .getObject(any(GetObjectRequest.class));

    return client;
  }

  private S3Object getMockedS3Object() {
    S3Object s3Object = mock(S3Object.class);

    // Set getObjectMetadata behavior: returns dummy metadata
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setHeader("ETag", "test-etag");
    doReturn(metadata)
      .when(s3Object)
      .getObjectMetadata();

    // Set getObjectContent behavior: returns bad stream twice, and good stream afterwards
    S3ObjectInputStream objectInputStreamBad1 = spy(new S3ObjectInputStream(
      IOUtils.toInputStream(input, Charset.defaultCharset()), null));
    S3ObjectInputStream objectInputStreamBad2 = spy(new S3ObjectInputStream(
      IOUtils.toInputStream(input, Charset.defaultCharset()), null));
    S3ObjectInputStream objectInputStreamGood = new S3ObjectInputStream(
      IOUtils.toInputStream(input, Charset.defaultCharset()), null);

    Exception exception = new SSLException(new SocketException("Connection reset"));
    Arrays.asList(objectInputStreamBad1, objectInputStreamBad2).forEach(
      badObjectStream -> {
        try {
          // Bad stream will throw connection reset on the first read
          Supplier<S3ObjectInputStream> getStreamAnswer = () ->
            doAnswer(invocation -> {
              // Simulate the scenario when connection reset exception is happened after read
              invocation.callRealMethod();
              throw exception;
            }).doCallRealMethod().when(badObjectStream);

          // Apply this behavior on read() and read(b, off, len)
          getStreamAnswer.get().read();
          getStreamAnswer.get().read(any(), anyInt(), anyInt());

        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    );

    doReturn(objectInputStreamBad1)
      .doReturn(objectInputStreamBad2)
      .doReturn(objectInputStreamGood)
      .when(s3Object)
      .getObjectContent();

    return s3Object;
  }
}
