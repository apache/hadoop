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

import org.apache.hadoop.conf.*;

/** Utility that wraps a {@link FSInputStream} in a {@link DataInputStream}
 * and buffers input through a {@link BufferedInputStream}. */
public class FSDataInputStream extends DataInputStream
    implements Seekable, PositionedReadable {

  /** Cache the file position.  This improves performance significantly.*/
  private static class PositionCache extends FilterInputStream {
    long position;

    public PositionCache(FSInputStream in) throws IOException {
      super(in);
    }

    // This is the only read() method called by BufferedInputStream, so we trap
    // calls to it in order to cache the position.
    public int read(byte b[], int off, int len) throws IOException {
      int result;
      if( (result = in.read(b, off, len)) > 0 )
        position += result;
      return result;
    }

    public void seek(long desired) throws IOException {
      ((FSInputStream)in).seek(desired);          // seek underlying stream
      position = desired;                         // update position
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      return ((FSInputStream)in).read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      ((FSInputStream)in).readFully(position, buffer, offset, length);
    }
  }

  /** Buffer input.  This improves performance significantly.*/
  private static class Buffer extends BufferedInputStream {
    public Buffer(PositionCache in, int bufferSize)
      throws IOException {
      super(in, bufferSize);
    }

    public void seek(long desired) throws IOException {
      long end = ((PositionCache)in).getPos();
      long start = end - this.count;
      int avail = this.count - this.pos;
      if (desired >= start && desired < end && avail > 0) {
        this.pos = (int)(desired - start);        // can position within buffer
      } else {
        this.count = 0;                           // invalidate buffer
        this.pos = 0;
        ((PositionCache)in).seek(desired);
      }
    }
      
    public long getPos() throws IOException {     // adjust for buffer
      return ((PositionCache)in).getPos() - (this.count - this.pos);
    }

    // optimized version of read()
    public int read() throws IOException {
      if (pos >= count)
        return super.read();
      return buf[pos++] & 0xff;
    }

    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      return ((PositionCache)in).read(position, buffer, offset, length);
    }
    
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      ((PositionCache)in).readFully(position, buffer, offset, length);
    }
  }

  protected FSInputStream inStream;
  
  public FSDataInputStream(FSInputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", 4096));
  }
  
  public FSDataInputStream(FSInputStream in, int bufferSize)
    throws IOException {
    super( new Buffer(new PositionCache(in), bufferSize) );
    this.inStream = in;
  }
  
  public synchronized void seek(long desired) throws IOException {
    ((Buffer)in).seek(desired);
  }

  public long getPos() throws IOException {
    return ((Buffer)in).getPos();
  }
  
  public int read(long position, byte[] buffer, int offset, int length)
  throws IOException {
    return ((Buffer)in).read(position, buffer, offset, length);
  }
  
  public void readFully(long position, byte[] buffer, int offset, int length)
  throws IOException {
    ((Buffer)in).readFully(position, buffer, offset, length);
  }
  
  public void readFully(long position, byte[] buffer)
  throws IOException {
    ((Buffer)in).readFully(position, buffer, 0, buffer.length);
  }
  
  public boolean seekToNewSource(long targetPos) throws IOException {
    return inStream.seekToNewSource(targetPos); 
  }
}
