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
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.CanUnbuffer;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.StreamCapabilitiesPolicy;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.util.StringUtils;

/**
 * CryptoInputStream decrypts data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The decryption is buffer based. The key points of the decryption
 * are (1) calculating the counter and (2) padding through stream position:
 * <p>
 * counter = base + pos/(algorithm blocksize); 
 * padding = pos%(algorithm blocksize); 
 * <p>
 * The underlying stream offset is maintained as state.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CryptoInputStream extends FilterInputStream implements 
    Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor, 
    CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess, 
    ReadableByteChannel, CanUnbuffer, StreamCapabilities {
  private final byte[] oneByteBuf = new byte[1];
  private final CryptoCodec codec;
  private final Decryptor decryptor;
  private final int bufferSize;
  
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at 
   * to inBuffer.limit().
   */
  private ByteBuffer inBuffer;
  
  /**
   * The decrypted data buffer. The data starts at outBuffer.position() and 
   * ends at outBuffer.limit();
   */
  private ByteBuffer outBuffer;
  private long streamOffset = 0; // Underlying stream offset.
  
  /**
   * Whether the underlying stream supports 
   * {@link org.apache.hadoop.fs.ByteBufferReadable}
   */
  private Boolean usingByteBufferRead = null;
  
  /**
   * Padding = pos%(algorithm blocksize); Padding is put into {@link #inBuffer} 
   * before any other data goes in. The purpose of padding is to put the input 
   * data at proper position.
   */
  private byte padding;
  private boolean closed;
  private final byte[] key;
  private final byte[] initIV;
  private byte[] iv;
  private final boolean isByteBufferReadable;
  private final boolean isReadableByteChannel;
  
  /** DirectBuffer pool */
  private final Queue<ByteBuffer> bufferPool = 
      new ConcurrentLinkedQueue<ByteBuffer>();
  /** Decryptor pool */
  private final Queue<Decryptor> decryptorPool = 
      new ConcurrentLinkedQueue<Decryptor>();
  
  public CryptoInputStream(InputStream in, CryptoCodec codec, 
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    this(in, codec, bufferSize, key, iv, 
        CryptoStreamUtils.getInputStreamOffset(in));
  }
  
  public CryptoInputStream(InputStream in, CryptoCodec codec,
      int bufferSize, byte[] key, byte[] iv, long streamOffset) throws IOException {
    super(in);
    CryptoStreamUtils.checkCodec(codec);
    this.bufferSize = CryptoStreamUtils.checkBufferSize(codec, bufferSize);
    this.codec = codec;
    this.key = key.clone();
    this.initIV = iv.clone();
    this.iv = iv.clone();
    this.streamOffset = streamOffset;
    isByteBufferReadable = in instanceof ByteBufferReadable;
    isReadableByteChannel = in instanceof ReadableByteChannel;
    inBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    outBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    decryptor = getDecryptor();
    resetStreamOffset(streamOffset);
  }
  
  public CryptoInputStream(InputStream in, CryptoCodec codec,
      byte[] key, byte[] iv) throws IOException {
    this(in, codec, CryptoStreamUtils.getBufferSize(codec.getConf()), key, iv);
  }
  
  public InputStream getWrappedStream() {
    return in;
  }
  
  /**
   * Decryption is buffer based.
   * If there is data in {@link #outBuffer}, then read it out of this buffer.
   * If there is no data in {@link #outBuffer}, then read more from the 
   * underlying stream and do the decryption.
   * @param b the buffer into which the decrypted data is read.
   * @param off the buffer offset.
   * @param len the maximum number of decrypted data bytes to read.
   * @return int the total number of decrypted data bytes read into the buffer.
   * @throws IOException
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }
    
    final int remaining = outBuffer.remaining();
    if (remaining > 0) {
      int n = Math.min(len, remaining);
      outBuffer.get(b, off, n);
      return n;
    } else {
      int n = 0;
      
      /*
       * Check whether the underlying stream is {@link ByteBufferReadable},
       * it can avoid bytes copy.
       */
      if (usingByteBufferRead == null) {
        if (isByteBufferReadable || isReadableByteChannel) {
          try {
            n = isByteBufferReadable ? 
                ((ByteBufferReadable) in).read(inBuffer) : 
                  ((ReadableByteChannel) in).read(inBuffer);
            usingByteBufferRead = Boolean.TRUE;
          } catch (UnsupportedOperationException e) {
            usingByteBufferRead = Boolean.FALSE;
          }
        } else {
          usingByteBufferRead = Boolean.FALSE;
        }
        if (!usingByteBufferRead) {
          n = readFromUnderlyingStream(inBuffer);
        }
      } else {
        if (usingByteBufferRead) {
          n = isByteBufferReadable ? ((ByteBufferReadable) in).read(inBuffer) : 
                ((ReadableByteChannel) in).read(inBuffer);
        } else {
          n = readFromUnderlyingStream(inBuffer);
        }
      }
      if (n <= 0) {
        return n;
      }
      
      streamOffset += n; // Read n bytes
      decrypt(decryptor, inBuffer, outBuffer, padding);
      padding = afterDecryption(decryptor, inBuffer, streamOffset, iv);
      n = Math.min(len, outBuffer.remaining());
      outBuffer.get(b, off, n);
      return n;
    }
  }
  
  /** Read data from underlying stream. */
  private int readFromUnderlyingStream(ByteBuffer inBuffer) throws IOException {
    final int toRead = inBuffer.remaining();
    final byte[] tmp = getTmpBuf();
    final int n = in.read(tmp, 0, toRead);
    if (n > 0) {
      inBuffer.put(tmp, 0, n);
    }
    return n;
  }
  
  private byte[] tmpBuf;
  private byte[] getTmpBuf() {
    if (tmpBuf == null) {
      tmpBuf = new byte[bufferSize];
    }
    return tmpBuf;
  }
  
  /**
   * Do the decryption using inBuffer as input and outBuffer as output.
   * Upon return, inBuffer is cleared; the decrypted data starts at 
   * outBuffer.position() and ends at outBuffer.limit();
   */
  private void decrypt(Decryptor decryptor, ByteBuffer inBuffer, 
      ByteBuffer outBuffer, byte padding) throws IOException {
    Preconditions.checkState(inBuffer.position() >= padding);
    if(inBuffer.position() == padding) {
      // There is no real data in inBuffer.
      return;
    }
    inBuffer.flip();
    outBuffer.clear();
    decryptor.decrypt(inBuffer, outBuffer);
    inBuffer.clear();
    outBuffer.flip();
    if (padding > 0) {
      /*
       * The plain text and cipher text have a 1:1 mapping, they start at the 
       * same position.
       */
      outBuffer.position(padding);
    }
  }
  
  /**
   * This method is executed immediately after decryption. Check whether 
   * decryptor should be updated and recalculate padding if needed. 
   */
  private byte afterDecryption(Decryptor decryptor, ByteBuffer inBuffer, 
      long position, byte[] iv) throws IOException {
    byte padding = 0;
    if (decryptor.isContextReset()) {
      /*
       * This code is generally not executed since the decryptor usually 
       * maintains decryption context (e.g. the counter) internally. However, 
       * some implementations can't maintain context so a re-init is necessary 
       * after each decryption call.
       */
      updateDecryptor(decryptor, position, iv);
      padding = getPadding(position);
      inBuffer.position(padding);
    }
    return padding;
  }
  
  private long getCounter(long position) {
    return position / codec.getCipherSuite().getAlgorithmBlockSize();
  }
  
  private byte getPadding(long position) {
    return (byte)(position % codec.getCipherSuite().getAlgorithmBlockSize());
  }
  
  /** Calculate the counter and iv, update the decryptor. */
  private void updateDecryptor(Decryptor decryptor, long position, byte[] iv) 
      throws IOException {
    final long counter = getCounter(position);
    codec.calculateIV(initIV, counter, iv);
    decryptor.init(key, iv);
  }
  
  /**
   * Reset the underlying stream offset; clear {@link #inBuffer} and 
   * {@link #outBuffer}. This Typically happens during {@link #seek(long)} 
   * or {@link #skip(long)}.
   */
  private void resetStreamOffset(long offset) throws IOException {
    streamOffset = offset;
    inBuffer.clear();
    outBuffer.clear();
    outBuffer.limit(0);
    updateDecryptor(decryptor, offset, iv);
    padding = getPadding(offset);
    inBuffer.position(padding); // Set proper position for input data.
  }
  
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    
    super.close();
    freeBuffers();
    codec.close();
    closed = true;
  }
  
  /** Positioned read. It is thread-safe */
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    try {
      final int n = ((PositionedReadable) in).read(position, buffer, offset, 
          length);
      if (n > 0) {
        // This operation does not change the current offset of the file
        decrypt(position, buffer, offset, n);
      }
      
      return n;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "positioned read.");
    }
  }
  
  /**
   * Decrypt length bytes in buffer starting at offset. Output is also put 
   * into buffer starting at offset. It is thread-safe.
   */
  private void decrypt(long position, byte[] buffer, int offset, int length) 
      throws IOException {
    ByteBuffer inBuffer = getBuffer();
    ByteBuffer outBuffer = getBuffer();
    Decryptor decryptor = null;
    try {
      decryptor = getDecryptor();
      byte[] iv = initIV.clone();
      updateDecryptor(decryptor, position, iv);
      byte padding = getPadding(position);
      inBuffer.position(padding); // Set proper position for input data.
      
      int n = 0;
      while (n < length) {
        int toDecrypt = Math.min(length - n, inBuffer.remaining());
        inBuffer.put(buffer, offset + n, toDecrypt);
        // Do decryption
        decrypt(decryptor, inBuffer, outBuffer, padding);
        
        outBuffer.get(buffer, offset + n, toDecrypt);
        n += toDecrypt;
        padding = afterDecryption(decryptor, inBuffer, position + n, iv);
      }
    } finally {
      returnBuffer(inBuffer);
      returnBuffer(outBuffer);
      returnDecryptor(decryptor);
    }
  }
  
  /** Positioned read fully. It is thread-safe */
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    try {
      ((PositionedReadable) in).readFully(position, buffer, offset, length);
      if (length > 0) {
        // This operation does not change the current offset of the file
        decrypt(position, buffer, offset, length);
      }
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "positioned readFully.");
    }
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }

  /** Seek to a position. */
  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK);
    }
    checkStream();
    try {
      /*
       * If data of target pos in the underlying stream has already been read 
       * and decrypted in outBuffer, we just need to re-position outBuffer.
       */
      if (pos <= streamOffset && pos >= (streamOffset - outBuffer.remaining())) {
        int forward = (int) (pos - (streamOffset - outBuffer.remaining()));
        if (forward > 0) {
          outBuffer.position(outBuffer.position() + forward);
        }
      } else {
        ((Seekable) in).seek(pos);
        resetStreamOffset(pos);
      }
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "seek.");
    }
  }
  
  /** Skip n bytes */
  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkArgument(n >= 0, "Negative skip length.");
    checkStream();
    
    if (n == 0) {
      return 0;
    } else if (n <= outBuffer.remaining()) {
      int pos = outBuffer.position() + (int) n;
      outBuffer.position(pos);
      return n;
    } else {
      /*
       * Subtract outBuffer.remaining() to see how many bytes we need to 
       * skip in the underlying stream. Add outBuffer.remaining() to the 
       * actual number of skipped bytes in the underlying stream to get the 
       * number of skipped bytes from the user's point of view.
       */
      n -= outBuffer.remaining();
      long skipped = in.skip(n);
      if (skipped < 0) {
        skipped = 0;
      }
      long pos = streamOffset + skipped;
      skipped += outBuffer.remaining();
      resetStreamOffset(pos);
      return skipped;
    }
  }

  /** Get underlying stream position. */
  @Override
  public long getPos() throws IOException {
    checkStream();
    // Equals: ((Seekable) in).getPos() - outBuffer.remaining()
    return streamOffset - outBuffer.remaining();
  }
  
  /** ByteBuffer read. */
  @Override
  public int read(ByteBuffer buf) throws IOException {
    checkStream();
    if (isByteBufferReadable || isReadableByteChannel) {
      final int unread = outBuffer.remaining();
      if (unread > 0) { // Have unread decrypted data in buffer.
        int toRead = buf.remaining();
        if (toRead <= unread) {
          final int limit = outBuffer.limit();
          outBuffer.limit(outBuffer.position() + toRead);
          buf.put(outBuffer);
          outBuffer.limit(limit);
          return toRead;
        } else {
          buf.put(outBuffer);
        }
      }
      
      final int pos = buf.position();
      final int n = isByteBufferReadable ? ((ByteBufferReadable) in).read(buf) : 
            ((ReadableByteChannel) in).read(buf);
      if (n > 0) {
        streamOffset += n; // Read n bytes
        decrypt(buf, n, pos);
      }
      
      if (n >= 0) {
        return unread + n;
      } else {
        if (unread == 0) {
          return -1;
        } else {
          return unread;
        }
      }
    } else {
      int n = 0;
      if (buf.hasArray()) {
        n = read(buf.array(), buf.position(), buf.remaining());
        if (n > 0) {
          buf.position(buf.position() + n);
        }
      } else {
        byte[] tmp = new byte[buf.remaining()];
        n = read(tmp);
        if (n > 0) {
          buf.put(tmp, 0, n);
        }
      }
      return n;
    }
  }
  
  /**
   * Decrypt all data in buf: total n bytes from given start position.
   * Output is also buf and same start position.
   * buf.position() and buf.limit() should be unchanged after decryption.
   */
  private void decrypt(ByteBuffer buf, int n, int start) 
      throws IOException {
    final int pos = buf.position();
    final int limit = buf.limit();
    int len = 0;
    while (len < n) {
      buf.position(start + len);
      buf.limit(start + len + Math.min(n - len, inBuffer.remaining()));
      inBuffer.put(buf);
      // Do decryption
      try {
        decrypt(decryptor, inBuffer, outBuffer, padding);
        buf.position(start + len);
        buf.limit(limit);
        len += outBuffer.remaining();
        buf.put(outBuffer);
      } finally {
        padding = afterDecryption(decryptor, inBuffer, streamOffset - (n - len), iv);
      }
    }
    buf.position(pos);
  }
  
  @Override
  public int available() throws IOException {
    checkStream();
    
    return in.available() + outBuffer.remaining();
  }

  @Override
  public boolean markSupported() {
    return false;
  }
  
  @Override
  public void mark(int readLimit) {
  }
  
  @Override
  public void reset() throws IOException {
    throw new IOException("Mark/reset not supported");
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    Preconditions.checkArgument(targetPos >= 0, 
        "Cannot seek to negative offset.");
    checkStream();
    try {
      boolean result = ((Seekable) in).seekToNewSource(targetPos);
      resetStreamOffset(targetPos);
      return result;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "seekToNewSource.");
    }
  }

  @Override
  public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
      EnumSet<ReadOption> opts) throws IOException,
      UnsupportedOperationException {
    checkStream();
    try {
      if (outBuffer.remaining() > 0) {
        // Have some decrypted data unread, need to reset.
        ((Seekable) in).seek(getPos());
        resetStreamOffset(getPos());
      }
      final ByteBuffer buffer = ((HasEnhancedByteBufferAccess) in).
          read(bufferPool, maxLength, opts);
      if (buffer != null) {
        final int n = buffer.remaining();
        if (n > 0) {
          streamOffset += buffer.remaining(); // Read n bytes
          final int pos = buffer.position();
          decrypt(buffer, n, pos);
        }
      }
      return buffer;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " + 
          "enhanced byte buffer access.");
    }
  }

  @Override
  public void releaseBuffer(ByteBuffer buffer) {
    try {
      ((HasEnhancedByteBufferAccess) in).releaseBuffer(buffer);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " + 
          "release buffer.");
    }
  }

  @Override
  public void setReadahead(Long readahead) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetReadahead) in).setReadahead(readahead);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "setting the readahead caching strategy.");
    }
  }

  @Override
  public void setDropBehind(Boolean dropCache) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetDropBehind) in).setDropBehind(dropCache);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not " +
          "support setting the drop-behind caching setting.");
    }
  }

  @Override
  public FileDescriptor getFileDescriptor() throws IOException {
    if (in instanceof HasFileDescriptor) {
      return ((HasFileDescriptor) in).getFileDescriptor();
    } else if (in instanceof FileInputStream) {
      return ((FileInputStream) in).getFD();
    } else {
      return null;
    }
  }
  
  @Override
  public int read() throws IOException {
    return (read(oneByteBuf, 0, 1) == -1) ? -1 : (oneByteBuf[0] & 0xff);
  }
  
  private void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }
  
  /** Get direct buffer from pool */
  private ByteBuffer getBuffer() {
    ByteBuffer buffer = bufferPool.poll();
    if (buffer == null) {
      buffer = ByteBuffer.allocateDirect(bufferSize);
    }
    
    return buffer;
  }
  
  /** Return direct buffer to pool */
  private void returnBuffer(ByteBuffer buf) {
    if (buf != null) {
      buf.clear();
      bufferPool.add(buf);
    }
  }
  
  /** Forcibly free the direct buffers. */
  private void freeBuffers() {
    CryptoStreamUtils.freeDB(inBuffer);
    CryptoStreamUtils.freeDB(outBuffer);
    cleanBufferPool();
  }
  
  /** Clean direct buffer pool */
  private void cleanBufferPool() {
    ByteBuffer buf;
    while ((buf = bufferPool.poll()) != null) {
      CryptoStreamUtils.freeDB(buf);
    }
  }
  
  /** Get decryptor from pool */
  private Decryptor getDecryptor() throws IOException {
    Decryptor decryptor = decryptorPool.poll();
    if (decryptor == null) {
      try {
        decryptor = codec.createDecryptor();
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }
    
    return decryptor;
  }
  
  /** Return decryptor to pool */
  private void returnDecryptor(Decryptor decryptor) {
    if (decryptor != null) {
      decryptorPool.add(decryptor);
    }
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  private void cleanDecryptorPool() {
    decryptorPool.clear();
  }

  @Override
  public void unbuffer() {
    cleanBufferPool();
    cleanDecryptorPool();
    StreamCapabilitiesPolicy.unbuffer(in);
  }

  @Override
  public boolean hasCapability(String capability) {
    switch (StringUtils.toLowerCase(capability)) {
    case StreamCapabilities.READAHEAD:
    case StreamCapabilities.DROPBEHIND:
    case StreamCapabilities.UNBUFFER:
    case StreamCapabilities.READBYTEBUFFER:
      return true;
    default:
      return false;
    }
  }
}
