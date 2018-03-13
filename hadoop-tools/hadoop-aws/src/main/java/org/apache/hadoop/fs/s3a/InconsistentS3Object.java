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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.internal.AmazonS3ExceptionBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around S3Object so we can do failure injection on
 * getObjectContent() and S3ObjectInputStream.
 * See also {@link InconsistentAmazonS3Client}.
 */
@SuppressWarnings({"NonSerializableFieldInSerializableClass", "serial"})
public class InconsistentS3Object extends S3Object {

  // This should be configurable, probably.
  public static final int MAX_READ_FAILURES = 100;

  private static int readFailureCounter = 0;
  private transient S3Object wrapped;
  private transient FailureInjectionPolicy policy;
  private final static transient Logger LOG = LoggerFactory.getLogger(
      InconsistentS3Object.class);

  public InconsistentS3Object(S3Object wrapped, FailureInjectionPolicy policy) {
    this.wrapped = wrapped;
    this.policy = policy;
  }

  @Override
  public S3ObjectInputStream getObjectContent() {
    return new InconsistentS3InputStream(wrapped.getObjectContent());
  }

  @Override
  public String toString() {
    return "InconsistentS3Object wrapping: " + wrapped.toString();
  }

  @Override
  public ObjectMetadata getObjectMetadata() {
    return wrapped.getObjectMetadata();
  }

  @Override
  public void setObjectMetadata(ObjectMetadata metadata) {
    wrapped.setObjectMetadata(metadata);
  }

  @Override
  public void setObjectContent(S3ObjectInputStream objectContent) {
    wrapped.setObjectContent(objectContent);
  }

  @Override
  public void setObjectContent(InputStream objectContent) {
    wrapped.setObjectContent(objectContent);
  }

  @Override
  public String getBucketName() {
    return wrapped.getBucketName();
  }

  @Override
  public void setBucketName(String bucketName) {
    wrapped.setBucketName(bucketName);
  }

  @Override
  public String getKey() {
    return wrapped.getKey();
  }

  @Override
  public void setKey(String key) {
    wrapped.setKey(key);
  }

  @Override
  public String getRedirectLocation() {
    return wrapped.getRedirectLocation();
  }

  @Override
  public void setRedirectLocation(String redirectLocation) {
    wrapped.setRedirectLocation(redirectLocation);
  }

  @Override
  public Integer getTaggingCount() {
    return wrapped.getTaggingCount();
  }

  @Override
  public void setTaggingCount(Integer taggingCount) {
    wrapped.setTaggingCount(taggingCount);
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

  @Override
  public boolean isRequesterCharged() {
    return wrapped.isRequesterCharged();
  }

  @Override
  public void setRequesterCharged(boolean isRequesterCharged) {
    wrapped.setRequesterCharged(isRequesterCharged);
  }

  private AmazonS3Exception mockException(String msg, int httpResponse) {
    AmazonS3ExceptionBuilder builder = new AmazonS3ExceptionBuilder();
    builder.setErrorMessage(msg);
    builder.setStatusCode(httpResponse); // this is the important part
    builder.setErrorCode(String.valueOf(httpResponse));
    return builder.build();
  }

  /**
   * Insert a failiure injection point for a read call.
   * @throw IOException, as codepath is on InputStream, not other SDK call.
   */
  private void readFailpoint(int off, int len) throws IOException {
    if (shouldInjectFailure(getKey())) {
      String error = String.format(
          "read(b, %d, %d) on key %s failed: injecting error %d/%d" +
              " for test.", off, len, getKey(), readFailureCounter,
          MAX_READ_FAILURES);
      throw new FileNotFoundException(error);
    }
  }

  /**
   * Insert a failiure injection point for an InputStream skip() call.
   * @throw IOException, as codepath is on InputStream, not other SDK call.
   */
  private void skipFailpoint(long len) throws IOException {
    if (shouldInjectFailure(getKey())) {
      String error = String.format(
          "skip(%d) on key %s failed: injecting error %d/%d for test.",
          len, getKey(), readFailureCounter, MAX_READ_FAILURES);
      throw new FileNotFoundException(error);
    }
  }

  private boolean shouldInjectFailure(String key) {
    if (policy.shouldDelay(key) &&
        readFailureCounter < MAX_READ_FAILURES) {
      readFailureCounter++;
      return true;
    }
    return false;
  }

  /**
   * Wraps S3ObjectInputStream and implements failure injection.
   */
  protected class InconsistentS3InputStream extends S3ObjectInputStream {
    private S3ObjectInputStream wrapped;

    public InconsistentS3InputStream(S3ObjectInputStream wrapped) {
      // seems awkward to have the stream wrap itself.
      super(wrapped, wrapped.getHttpRequest());
      this.wrapped = wrapped;
    }

    @Override
    public void abort() {
      wrapped.abort();
    }

    @Override
    public int available() throws IOException {
      return wrapped.available();
    }

    @Override
    public void close() throws IOException {
      wrapped.close();
    }

    @Override
    public long skip(long n) throws IOException {
      skipFailpoint(n);
      return wrapped.skip(n);
    }

    @Override
    public int read() throws IOException {
      LOG.debug("read() for key {}", getKey());
      readFailpoint(0, 1);
      return wrapped.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      LOG.debug("read(b, {}, {}) for key {}", off, len, getKey());
      readFailpoint(off, len);
      return wrapped.read(b, off, len);
    }

  }
}
