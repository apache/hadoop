/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.fs.s3a.prefetch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import org.apache.hadoop.fs.impl.prefetch.Validate;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.fs.s3a.impl.streams.ObjectInputStreamCallbacks;
import org.apache.hadoop.util.functional.CallableRaisingIOE;

/**
 * A mock s3 file with some fault injection.
 */
class MockS3ARemoteObject extends S3ARemoteObject {

  private byte[] contents;

  // If true, throws IOException on open request just once.
  // That allows test code to validate behavior related to retries.
  private boolean throwExceptionOnOpen;

  private static final String BUCKET = "bucket";

  private static final String KEY = "key";

  MockS3ARemoteObject(int size) {
    this(size, false);
  }

  MockS3ARemoteObject(int size, boolean throwExceptionOnOpen) {
    super(
        S3APrefetchFakes.createReadContext(null, KEY, size),
        S3APrefetchFakes.createObjectAttributes(BUCKET, KEY, size),
        S3APrefetchFakes.createInputStreamCallbacks(BUCKET),
        EmptyS3AStatisticsContext.EMPTY_INPUT_STREAM_STATISTICS,
        S3APrefetchFakes.createChangeTracker(BUCKET, KEY, size)
    );

    this.throwExceptionOnOpen = throwExceptionOnOpen;
    this.contents = new byte[size];
    for (int b = 0; b < size; b++) {
      this.contents[b] = byteAtOffset(b);
    }
  }

  @Override
  public ResponseInputStream<GetObjectResponse> openForRead(long offset, int size)
      throws IOException {
    Validate.checkLessOrEqual(offset, "offset", size(), "size()");
    Validate.checkLessOrEqual(size, "size", size() - offset, "size() - offset");

    if (throwExceptionOnOpen) {
      throwExceptionOnOpen = false;
      throw new IOException("Throwing because throwExceptionOnOpen is true ");
    }
    int bufSize = (int) Math.min(size, size() - offset);
    GetObjectResponse objectResponse = GetObjectResponse.builder().build();
    return new ResponseInputStream(objectResponse,
        AbortableInputStream.create(new ByteArrayInputStream(contents,
            (int) offset, bufSize), () -> {}));
  }

  @Override
  public void close(ResponseInputStream<GetObjectResponse> inputStream,
      int numRemainingBytes) {
    // do nothing since we do not use a real S3 stream.
  }

  public static byte byteAtOffset(int offset) {
    return (byte) (offset % 128);
  }

  public static ObjectInputStreamCallbacks createClient(String bucketName) {
    return new ObjectInputStreamCallbacks() {
      @Override
      public ResponseInputStream<GetObjectResponse> getObject(
          GetObjectRequest request) {
        return null;
      }

      @Override
      public <T> CompletableFuture<T> submit(CallableRaisingIOE<T> operation) {
        return null;
      }

      @Override
      public GetObjectRequest.Builder newGetRequestBuilder(String key) {
        return GetObjectRequest.builder().bucket(bucketName).key(key);
      }

      @Override
      public void close() {
      }
    };
  }
}
