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

import org.apache.hadoop.fs.common.BufferData;

import com.amazonaws.services.s3.AmazonS3;
import com.twitter.util.FuturePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provides an {@link InputStream} that allows reading from an S3 file.
 * The entire file is read into memory before reads can begin.
 *
 * Use of this class is recommended only for small files that can fit
 * entirely in memory.
 */
public class S3InMemoryInputStream extends S3InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(S3InMemoryInputStream.class);

  private ByteBuffer buffer;

  public S3InMemoryInputStream(
      FuturePool futurePool,
      String bucket,
      String key,
      long fileSize,
      AmazonS3 client) {
    super(futurePool, (int) fileSize, bucket, key, fileSize, client);
    this.buffer = ByteBuffer.allocate((int) fileSize);
    LOG.debug("Created in-memory input stream for {} (size = {})", this.name, fileSize);
  }

  @Override
  protected boolean ensureCurrentBuffer() {
    if (this.closed) {
      return false;
    }

    if (this.blockData.fileSize == 0) {
      return false;
    }

    if (!this.fpos.isValid()) {
      try {
        S3Reader reader = new S3Reader(this.s3File);
        this.buffer.clear();
        int numBytesRead = reader.read(buffer, 0, this.buffer.capacity());
        if (numBytesRead < 0) {
          return false;
        }
        BufferData data = new BufferData(0, buffer);
        this.fpos.setData(data, 0, this.seekTargetPos);
      } catch (IOException e) {
        LOG.error("ensureCurrentBuffer", e);
        throw new RuntimeException(e);
      }
    }

    return this.fpos.buffer().hasRemaining();
  }
}
