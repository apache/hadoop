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
package org.apache.hadoop.crypto;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestCryptoStreams extends CryptoStreamsTestBase {
  /**
   * Data storage.
   * {@link #getOutputStream(int)} will write to this buf.
   * {@link #getInputStream(int)} will read from this buf.
   */
  private byte[] buf;
  private int bufLen;
  
  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new Configuration();
    codec = CryptoCodec.getInstance(conf);
  }
  
  @AfterClass
  public static void shutdown() throws Exception {
  }
  
  @Override
  protected OutputStream getOutputStream(int bufferSize, byte[] key, byte[] iv) 
      throws IOException {
    DataOutputBuffer out = new DataOutputBuffer() {
      @Override
      public void flush() throws IOException {
        buf = getData();
        bufLen = getLength();
      }
      @Override
      public void close() throws IOException {
        buf = getData();
        bufLen = getLength();
      }
    };
    return new CryptoOutputStream(new FakeOutputStream(out),
        codec, bufferSize, key, iv);
  }
  
  @Override
  protected InputStream getInputStream(int bufferSize, byte[] key, byte[] iv) 
      throws IOException {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(buf, 0, bufLen);
    return new CryptoInputStream(new FakeInputStream(in), codec, bufferSize, 
        key, iv);
  }
  
  private class FakeOutputStream extends OutputStream 
      implements Syncable, CanSetDropBehind{
    private final byte[] oneByteBuf = new byte[1];
    private final DataOutputBuffer out;
    private boolean closed;
    
    public FakeOutputStream(DataOutputBuffer out) {
      this.out = out;
    }
    
    @Override
    public void write(byte b[], int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return;
      }
      
      checkStream();
      
      out.write(b, off, len);
    }
    
    @Override
    public void flush() throws IOException {
      checkStream();
      out.flush();
    }
    
    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      
      out.close();
      closed = true;
    }

    @Override
    public void write(int b) throws IOException {
      oneByteBuf[0] = (byte)(b & 0xff);
      write(oneByteBuf, 0, oneByteBuf.length);
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException,
        UnsupportedOperationException {
    }

    @Override
    public void hflush() throws IOException {
      checkStream();
      flush();
    }

    @Override
    public void hsync() throws IOException {
      checkStream();
      flush();
    }
    
    private void checkStream() throws IOException {
      if (closed) {
        throw new IOException("Stream is closed!");
      }
    }
  }
  
  public static class FakeInputStream extends InputStream implements 
      Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor, 
      CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess {
    private final byte[] oneByteBuf = new byte[1];
    private int pos = 0;
    private final byte[] data;
    private final int length;
    private boolean closed = false;

    public FakeInputStream(DataInputBuffer in) {
      data = in.getData();
      length = in.getLength();
    }
    
    @Override
    public void seek(long pos) throws IOException {
      if (pos > length) {
        throw new IOException("Cannot seek after EOF.");
      }
      if (pos < 0) {
        throw new IOException("Cannot seek to negative offset.");
      }
      checkStream();
      this.pos = (int)pos;
    }
    
    @Override
    public long getPos() throws IOException {
      return pos;
    }
    
    @Override
    public int available() throws IOException {
      return length - pos;
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      
      checkStream();
      
      if (pos < length) {
        int n = (int) Math.min(len, length - pos);
        System.arraycopy(data, pos, b, off, n);
        pos += n;
        return n;
      }
      
      return -1;
    }
    
    private void checkStream() throws IOException {
      if (closed) {
        throw new IOException("Stream is closed!");
      }
    }
    
    @Override
    public int read(ByteBuffer buf) throws IOException {
      checkStream();
      if (pos < length) {
        int n = (int) Math.min(buf.remaining(), length - pos);
        if (n > 0) {
          buf.put(data, pos, n);
        }
        pos += n;
        return n;
      }
      return -1;
    }
    
    @Override
    public long skip(long n) throws IOException {
      checkStream();
      if ( n > 0 ) {
        if( n + pos > length ) {
          n = length - pos;
        }
        pos += n;
        return n;
      }
      return n < 0 ? -1 : 0;
    }
    
    @Override
    public void close() throws IOException {
      closed = true;
    }

    @Override
    public int read(long position, byte[] b, int off, int len)
        throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      
      if (position > length) {
        throw new IOException("Cannot read after EOF.");
      }
      if (position < 0) {
        throw new IOException("Cannot read to negative offset.");
      }
      
      checkStream();
      
      if (position < length) {
        int n = (int) Math.min(len, length - position);
        System.arraycopy(data, (int)position, b, off, n);
        return n;
      }
      
      return -1;
    }

    @Override
    public void readFully(long position, byte[] b, int off, int len)
        throws IOException {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return;
      }
      
      if (position > length) {
        throw new IOException("Cannot read after EOF.");
      }
      if (position < 0) {
        throw new IOException("Cannot read to negative offset.");
      }
      
      checkStream();
      
      if (position + len > length) {
        throw new EOFException("Reach the end of stream.");
      }
      
      System.arraycopy(data, (int)position, b, off, len);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
        EnumSet<ReadOption> opts) throws IOException,
        UnsupportedOperationException {
      if (bufferPool == null) {
        throw new IOException("Please specify buffer pool.");
      }
      ByteBuffer buffer = bufferPool.getBuffer(true, maxLength);
      int pos = buffer.position();
      int n = read(buffer);
      if (n >= 0) {
        buffer.position(pos);
        return buffer;
      }
      
      return null;
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
      
    }

    @Override
    public void setReadahead(Long readahead) throws IOException,
        UnsupportedOperationException {
    }

    @Override
    public void setDropBehind(Boolean dropCache) throws IOException,
        UnsupportedOperationException {
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
      return null;
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      if (targetPos > length) {
        throw new IOException("Attempted to read past end of file.");
      }
      if (targetPos < 0) {
        throw new IOException("Cannot seek after EOF.");
      }
      checkStream();
      this.pos = (int)targetPos;
      return false;
    }

    @Override
    public int read() throws IOException {
      int ret = read( oneByteBuf, 0, 1 );
      return ( ret <= 0 ) ? -1 : (oneByteBuf[0] & 0xff);
    }
  }
}
