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

/** Utility that wraps a {@link OutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file. */
public class FSDataOutputStream extends DataOutputStream {
  private OutputStream wrappedStream;

  private static class PositionCache extends FilterOutputStream {
    long position;

    public PositionCache(OutputStream out) throws IOException {
      super(out);
    }

    public void write(int b) throws IOException {
      out.write(b);
      position++;
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public void close() throws IOException {
      flush();
      out.close();
    }
  }

  public FSDataOutputStream(OutputStream out)
    throws IOException {
    super(new PositionCache(out));
    wrappedStream = out;
  }
  
  public long getPos() throws IOException {
    return ((PositionCache)out).getPos();
  }

  public void close() throws IOException {
    flush();
    out.close();
  }

  // Returns the underlying output stream. This is used by unit tests.
  public OutputStream getWrappedStream() {
    return wrappedStream;
  }
}
