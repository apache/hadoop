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

import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.thirdparty.com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

class TOSInputStream extends InputStream {
  private static final Logger LOG = LoggerFactory.getLogger(TOSInputStream.class);

  private final GetObjectOutput output;
  private final InputStream stream;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private long curOff;
  private final long endOff; // range end offset (inclusive)
  private final long maxDrainByteSize;

  TOSInputStream(GetObjectOutput output, long startOff, long endOff, long maxDrainByteSize,
      byte[] expectedChecksum) throws IOException {
    this.output = output;
    this.stream = output.verifiedContent(expectedChecksum);
    this.curOff = startOff;
    this.endOff = endOff;
    this.maxDrainByteSize = maxDrainByteSize;
  }

  @Override
  public int read() throws IOException {
    int b = stream.read();
    curOff += 1;
    return b;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int readed = 0;
    int n;
    do {
      n = stream.read(b, off + readed, len - readed);
      if (n > 0) {
        readed += n;
      }
    } while (n > 0);

    if (readed == 0) {
      return n;
    } else {
      curOff += readed;
      return readed;
    }
  }

  // Only visible for testing.
  GetObjectOutput getObjectOutput() {
    return output;
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      if (endOff >= 0) {
        // The unread bytes is known. we just skip the bytes if gap <= expected drain size (to reuse
        // the socket conn), otherwise we force close the socket conn without reading any bytes in
        // the future.
        long gap = endOff - curOff + 1;
        if (gap <= maxDrainByteSize) {
          // The close will try to drain bytes internally.
          stream.close();
        } else {
          CommonUtils.runQuietly(output::forceClose, false);
        }

      } else {
        // The unread bytes is unknown, we try to read the expected drain bytes to see if it's EOF
        // now. If EOF then just close the stream to reuse the socket conn, otherwise close the
        // connection directly for saving draining time.
        try {
          ByteStreams.skipFully(stream, maxDrainByteSize);
        } catch (Exception e) {
          if (e instanceof EOFException) {
            LOG.debug("Stream is EOF now, just close the stream to reuse the socket connection.");
            stream.close();
          } else {
            LOG.debug("Stream skipFully encountered exception, force close the socket connection.",
                e);
            // Force close the socket connection.
            CommonUtils.runQuietly(output::forceClose, false);
          }
          return;
        }

        // Force close the socket connection.
        CommonUtils.runQuietly(output::forceClose, false);
      }
    }
  }
}
