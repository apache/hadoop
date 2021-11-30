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

package org.apache.hadoop.fs.s3a.read;

import org.apache.hadoop.fs.common.Validate;
import org.apache.hadoop.fs.common.Io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Ecapsulates low level interactions with S3 object on AWS.
 */
public class S3File implements Closeable {
  // Client used for accessing S3 objects.
  private final AmazonS3 client;

  private final String bucket;
  private final String key;

  // Size of S3 file.
  private long size;

  // Maps a stream returned by openForRead() to the associated S3 object.
  // That allows us to close the object when closing the stream.
  private Map<InputStream, S3Object> s3Objects;

  /**
   * Creates an instance of {@link S3File}.
   *
   * @param client AWS S3 client instance.
   * @param bucket the bucket to read from.
   * @param key the key to read from.
   */
  public S3File(AmazonS3 client, String bucket, String key) {
    this(client, bucket, key, -1);
  }

  /**
   * Creates an instance of {@link S3File}.
   *
   * @param client AWS S3 client instance.
   * @param bucket the bucket to read from.
   * @param key the key to read from.
   * @param size the size of this file. it should be passed in if pre-known
   *        to avoid an additional network call. If size is not known,
   *        pass a negative number so that size can be obtained if needed.
   */
  public S3File(AmazonS3 client, String bucket, String key, long size) {
    Validate.checkNotNull(client, "client");
    Validate.checkNotNullAndNotEmpty(bucket, "bucket");
    Validate.checkNotNullAndNotEmpty(key, "key");

    this.client = client;
    this.bucket = bucket;
    this.key = key;
    this.size = size;
    this.s3Objects = new IdentityHashMap();
  }

  public String getPath() {
    return String.format("s3://%s/%s", this.bucket, this.key);
  }

  /**
   * Gets the size of this file.
   * Its value is cached once obtained from AWS.
   */
  public long size() throws IOException {
    if (size < 0) {
      size = client.getObjectMetadata(bucket, key).getContentLength();
    }
    return size;
  }

  /**
   * Opens a section of the file for reading.
   *
   * @param offset Start offset (0 based) of the section to read.
   * @param size Size of the section to read.
   */
  public InputStream openForRead(long offset, int size) throws IOException {
    Validate.checkLessOrEqual(offset, "offset", size(), "size()");
    Validate.checkLessOrEqual(size, "size", size() - offset, "size() - offset");

    GetObjectRequest request =
        new GetObjectRequest(bucket, key).withRange(offset, offset + size - 1);
    S3Object obj = client.getObject(request);
    InputStream stream = obj.getObjectContent();
    synchronized (this.s3Objects) {
      this.s3Objects.put(stream, obj);
    }
    return stream;
  }

  public void close(InputStream inputStream) {
    S3Object obj;
    synchronized (this.s3Objects) {
      obj = this.s3Objects.get(inputStream);
      if (obj == null) {
        throw new IllegalArgumentException("inputStream not found");
      }
      this.s3Objects.remove(inputStream);
    }

    Io.closeIgnoringIoException(inputStream);
    Io.closeIgnoringIoException(obj);
  }

  @Override
  public synchronized void close() {
    List<InputStream> streams = new ArrayList(this.s3Objects.keySet());
    for (InputStream stream : streams) {
      this.close(stream);
    }
  }
}
