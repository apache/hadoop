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

package org.apache.hadoop.fs.s3a.multipart;

import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;

/**
 * An adapter between {@link AbortableInputStream} and
 * {@link S3ObjectInputStream}.
 */
public class AbortableS3ObjectInputStream extends AbortableInputStream {

  private final S3ObjectInputStream s3ObjectInputStream;

  public AbortableS3ObjectInputStream(S3ObjectInputStream s3ObjectInputStream) {
    this.s3ObjectInputStream = s3ObjectInputStream;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return s3ObjectInputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return s3ObjectInputStream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return s3ObjectInputStream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return s3ObjectInputStream.available();
  }

  @Override
  public void close() throws IOException {
    s3ObjectInputStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    s3ObjectInputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    s3ObjectInputStream.reset();
  }

  @Override
  public boolean markSupported() {
    return s3ObjectInputStream.markSupported();
  }

  @Override
  public void abort() {
    s3ObjectInputStream.abort();
  }

  @Override
  public int read() throws IOException {
    return s3ObjectInputStream.read();
  }
}
