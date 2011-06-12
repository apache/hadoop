/**
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

package org.apache.hadoop.raid;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.IOUtils;

public class RaidUtils {
  public static void readTillEnd(InputStream in, byte[] buf, boolean eofOK)
    throws IOException {
    int toRead = buf.length;
    int numRead = 0;
    while (numRead < toRead) {
      int nread = in.read(buf, numRead, toRead - numRead);
      if (nread < 0) {
        if (eofOK) {
          // EOF hit, fill with zeros
          Arrays.fill(buf, numRead, toRead, (byte)0);
          numRead = toRead;
        } else {
          // EOF hit, throw.
          throw new IOException("Premature EOF");
        }
      } else {
        numRead += nread;
      }
    }
  }

  public static void copyBytes(
    InputStream in, OutputStream out, byte[] buf, long count)
    throws IOException {
    for (long bytesRead = 0; bytesRead < count; ) {
      int toRead = Math.min(buf.length, (int)(count - bytesRead));
      IOUtils.readFully(in, buf, 0, toRead);
      bytesRead += toRead;
      out.write(buf, 0, toRead);
    }
  }

  public static class ZeroInputStream extends InputStream
	    implements Seekable, PositionedReadable {
    private long endOffset;
    private long pos;

    public ZeroInputStream(long endOffset) {
      this.endOffset = endOffset;
      this.pos = 0;
    }

    @Override
    public int read() throws IOException {
      if (pos < endOffset) {
        pos++;
        return 0;
      }
      return -1;
    }

    @Override
    public int available() throws IOException {
      return (int)(endOffset - pos);
    }

    @Override
    public long getPos() throws IOException {
      return pos;
    }

    @Override
    public void seek(long seekOffset) throws IOException {
      if (seekOffset < endOffset) {
        pos = seekOffset;
      } else {
        throw new IOException("Illegal Offset" + pos);
      }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      return count;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
      int count = 0;
      for (; position < endOffset && count < length; position++) {
        buffer[offset + count] = 0;
        count++;
      }
      if (count < length) {
        throw new IOException("Premature EOF");
      }
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }
}
