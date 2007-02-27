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
package org.apache.hadoop.fs;

import java.io.*;

import org.apache.hadoop.conf.Configuration;

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file. */
public class FSDataOutputStream extends DataOutputStream {
  private static class PositionCache extends FilterOutputStream {
    long position;

    public PositionCache(OutputStream out) throws IOException {
      super(out);
    }

    // This is the only write() method called by BufferedOutputStream, so we
    // trap calls to it in order to cache the position.
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
  }

  private static class Buffer extends BufferedOutputStream {
    public Buffer(PositionCache out, int bufferSize) throws IOException {
      super(out, bufferSize);
    }

    public long getPos() throws IOException {
      return ((PositionCache)out).getPos() + this.count;
    }

    // optimized version of write(int)
    public void write(int b) throws IOException {
      if (count >= buf.length) {
        super.write(b);
      } else {
        buf[count++] = (byte)b;
      }
    }
  }

  public FSDataOutputStream(OutputStream out, int bufferSize)
  throws IOException {
    super(new Buffer(new PositionCache(out), bufferSize));
  }
  
  public FSDataOutputStream(OutputStream out, Configuration conf)
  throws IOException {
    this(out, conf.getInt("io.file.buffer.size", 4096));
  }

  public long getPos() throws IOException {
    return ((Buffer)out).getPos();
  }
}
