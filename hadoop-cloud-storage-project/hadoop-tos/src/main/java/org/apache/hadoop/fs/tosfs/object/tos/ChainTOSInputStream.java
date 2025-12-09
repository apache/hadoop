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

package org.apache.hadoop.fs.tosfs.object.tos;

import org.apache.hadoop.fs.tosfs.common.Chain;
import org.apache.hadoop.util.Preconditions;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChainTOSInputStream extends InputStream {
  private final Chain<TOSInputStream> chain;
  private final TOS.GetObjectFactory factory;
  private final String key;
  private long curOff;
  private final long endOff; // range end offset (inclusive)
  private final long maxDrainByteSize;
  private final int maxInputStreamRetries;

  private int readBytes;
  private long skipped;
  private byte[] objChecksum = null;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ChainTOSInputStream(
      TOS.GetObjectFactory factory,
      String key,
      long startOff,
      long endOff,
      long maxDrainByteSize,
      int maxInputStreamRetries) {
    this.factory = factory;
    this.key = key;
    this.curOff = startOff;
    this.endOff = endOff;
    this.maxDrainByteSize = maxDrainByteSize;
    this.maxInputStreamRetries = maxInputStreamRetries;
    this.chain = createChain();
    Preconditions.checkNotNull(objChecksum, "Checksum should not be null.");
  }

  private Chain<TOSInputStream> createChain() {
    Chain.Builder<TOSInputStream> builder = Chain.<TOSInputStream>builder()
        .shouldContinue(e -> !(e instanceof EOFException));

    for (int i = 0; i <= maxInputStreamRetries; i++) {
      builder.addLast(() -> {
        GetObjectOutput output = factory.create(key, curOff, endOff);

        // Note: If there are some IO errors occur, the ChainTOSInputStream will create a new
        // stream in the chain to continue reading object data, we need to record the checksum
        // during first open object stream, and ensure the checksum of object stream won't be
        // changed if opening object many times within the lifecycle of the chained stream in case
        // the underlying object is changed.
        if (objChecksum == null) {
          // Init the stream checksum.
          objChecksum = output.checksum();
        }
        return new TOSInputStream(output, curOff, endOff, maxDrainByteSize, objChecksum);
      });
    }

    try {
      return builder.build();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    skipped = 0;
    return chain.run(stream -> {
      long skip = stream.skip(n - skipped);

      curOff += skip;
      skipped += skip;
      return skipped;
    });
  }

  @Override
  public int read() throws IOException {
    return chain.run(stream -> {
      int ret = stream.read();
      curOff++;
      return ret;
    });
  }

  @Override
  public int available() throws IOException {
    return chain.run(InputStream::available);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    readBytes = 0;
    return chain.run(in -> {
      int read = in.read(b, off + readBytes, len - readBytes);

      readBytes += read;
      curOff += read;
      return readBytes;
    });
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      chain.close();
    }
  }

  public byte[] checksum() {
    return objChecksum;
  }
}
