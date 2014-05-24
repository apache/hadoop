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

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.CanSetReadahead;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.ByteBufferPool;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT;

import com.google.common.base.Preconditions;

/**
 * CryptoInputStream decrypts data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The decryption is buffer based. The key points of the decryption
 * are (1) calculating the counter and (2) padding through stream position:
 * <p/>
 * counter = base + pos/(algorithm blocksize); 
 * padding = pos%(algorithm blocksize); 
 * <p/>
 * The underlying stream offset is maintained as state.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CryptoInputStream extends FilterInputStream implements 
    Seekable, PositionedReadable, ByteBufferReadable, HasFileDescriptor, 
    CanSetDropBehind, CanSetReadahead, HasEnhancedByteBufferAccess {
  private static final int MIN_BUFFER_SIZE = 512;
  private static final byte[] oneByteBuf = new byte[1];
  private final CryptoCodec codec;
  private final Decryptor decryptor;
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
   * Whether underlying stream supports 
   * {@link #org.apache.hadoop.fs.ByteBufferReadable}
   */
  private Boolean usingByteBufferRead = null;
  /**
   * Padding = pos%(algorithm blocksize); Padding is put into {@link #inBuffer} 
   * before any other data goes in. The purpose of padding is to put input data
   * at proper position.
   */
  private byte padding;
  private boolean closed;
  private final byte[] key;
  private final byte[] initIV;
  private byte[] iv;
  
  public CryptoInputStream(InputStream in, CryptoCodec codec, 
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    super(in);
    Preconditions.checkArgument(bufferSize >= MIN_BUFFER_SIZE, 
        "Minimum value of buffer size is 512.");
    this.key = key;
    this.initIV = iv;
    this.iv = iv.clone();
    inBuffer = ByteBuffer.allocateDirect(bufferSize);
    outBuffer = ByteBuffer.allocateDirect(bufferSize);
    outBuffer.limit(0);
    this.codec = codec;
    try {
      decryptor = codec.getDecryptor();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    if (in instanceof Seekable) {
      streamOffset = ((Seekable) in).getPos();
    }
    updateDecryptor();
  }
  
  public CryptoInputStream(InputStream in, CryptoCodec codec,
      byte[] key, byte[] iv) throws IOException {
    this(in, codec, getBufferSize(codec.getConf()), key, iv);
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
    
    int remaining = outBuffer.remaining();
    if (remaining > 0) {
      int n = Math.min(len, remaining);
      outBuffer.get(b, off, n);
      return n;
    } else {
      int n = 0;
      /**
       * Check whether the underlying stream is {@link ByteBufferReadable},
       * it can avoid bytes copy.
       */
      if (usingByteBufferRead == null) {
        if (in instanceof ByteBufferReadable) {
          try {
            n = ((ByteBufferReadable) in).read(inBuffer);
            usingByteBufferRead = Boolean.TRUE;
          } catch (UnsupportedOperationException e) {
            usingByteBufferRead = Boolean.FALSE;
          }
        }
        if (!usingByteBufferRead.booleanValue()) {
          n = readFromUnderlyingStream();
        }
      } else {
        if (usingByteBufferRead.booleanValue()) {
          n = ((ByteBufferReadable) in).read(inBuffer);
        } else {
          n = readFromUnderlyingStream();
        }
      }
      if (n <= 0) {
        return n;
      }
      
      streamOffset += n; // Read n bytes
      decrypt();
      n = Math.min(len, outBuffer.remaining());
      outBuffer.get(b, off, n);
      return n;
    }
  }
  
  // Read data from underlying stream.
  private int readFromUnderlyingStream() throws IOException {
    int toRead = inBuffer.remaining();
    byte[] tmp = getTmpBuf();
    int n = in.read(tmp, 0, toRead);
    if (n > 0) {
      inBuffer.put(tmp, 0, n);
    }
    return n;
  }
  
  private byte[] tmpBuf;
  private byte[] getTmpBuf() {
    if (tmpBuf == null) {
      tmpBuf = new byte[inBuffer.capacity()];
    }
    return tmpBuf;
  }
  
  /**
   * Do the decryption using {@link #inBuffer} as input and {@link #outBuffer} 
   * as output.
   */
  private void decrypt() throws IOException {
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
      /**
       * The plain text and cipher text have 1:1 mapping, they start at same 
       * position.
       */
      outBuffer.position(padding);
      padding = 0;
    }
    if (decryptor.isContextReset()) {
      /**
       * Typically we will not get here. To improve performance in CTR mode,
       * we rely on the decryptor maintaining context, for example calculating 
       * the counter. Unfortunately, some bad implementations can't maintain 
       * context so we need to re-init after doing decryption.
       */
      updateDecryptor();
    }
  }
  
  /**
   * Update the {@link #decryptor}. Calculate the counter and {@link #padding}.
   */
  private void updateDecryptor() throws IOException {
    long counter = streamOffset / codec.getAlgorithmBlockSize();
    padding = (byte)(streamOffset % codec.getAlgorithmBlockSize());
    inBuffer.position(padding); // Set proper position for input data.
    codec.calculateIV(initIV, counter, iv);
    decryptor.init(key, iv);
  }
  
  /**
   * Reset the underlying stream offset; and clear {@link #inBuffer} and 
   * {@link #outBuffer}. Typically this happens when doing {@link #seek(long)} 
   * or {@link #skip(long)}.
   */
  private void resetStreamOffset(long offset) throws IOException {
    streamOffset = offset;
    inBuffer.clear();
    outBuffer.clear();
    outBuffer.limit(0);
    updateDecryptor();
  }
  
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    
    super.close();
    freeBuffers();
    closed = true;
  }
  
  /**
   * Free the direct buffer manually.
   */
  private void freeBuffers() {
    sun.misc.Cleaner inBufferCleaner =
        ((sun.nio.ch.DirectBuffer) inBuffer).cleaner();
    inBufferCleaner.clean();
    sun.misc.Cleaner outBufferCleaner =
        ((sun.nio.ch.DirectBuffer) outBuffer).cleaner();
    outBufferCleaner.clean();
  }
  
  // Positioned read.
  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    try {
      int n = ((PositionedReadable) in).read(position, buffer, offset, length);
      if (n > 0) {
        /** 
         * Since this operation does not change the current offset of a file, 
         * streamOffset should be not changed and we need to restore the 
         * decryptor and outBuffer after decryption.
         */
        decrypt(position, buffer, offset, length);
      }
      
      return n;
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not support " +
          "positioned read.");
    }
  }
  
  /**
   * Decrypt given length of data in buffer: start from offset.
   * Output is also buffer and start from same offset. Restore the 
   * {@link #decryptor} and {@link #outBuffer} after decryption.
   */
  private void decrypt(long position, byte[] buffer, int offset, int length) 
      throws IOException {
    
    byte[] tmp = getTmpBuf();
    int unread = outBuffer.remaining();
    if (unread > 0) { // Cache outBuffer
      outBuffer.get(tmp, 0, unread);
    }
    long curOffset = streamOffset;
    resetStreamOffset(position);
    
    int n = 0;
    while (n < length) {
      int toDecrypt = Math.min(length - n, inBuffer.remaining());
      inBuffer.put(buffer, offset + n, toDecrypt);
      // Do decryption
      decrypt();
      outBuffer.get(buffer, offset + n, toDecrypt);
      n += toDecrypt;
    }
    
    // After decryption
    resetStreamOffset(curOffset);
    if (unread > 0) { // Restore outBuffer
      outBuffer.clear();
      outBuffer.put(tmp, 0, unread);
      outBuffer.flip();
    }
  }
  
  // Positioned read fully.
  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    checkStream();
    try {
      ((PositionedReadable) in).readFully(position, buffer, offset, length);
      if (length > 0) {
        /** 
         * Since this operation does not change the current offset of a file, 
         * streamOffset should be not changed and we need to restore the decryptor 
         * and outBuffer after decryption.
         */
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

  // Seek to a position.
  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkArgument(pos >= 0, "Cannot seek to negative offset.");
    checkStream();
    try {
      // If target pos we have already read and decrypt.
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
  
  // Skip n bytes
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
      /**
       * Subtract outBuffer.remaining() to see how many bytes we need to 
       * skip in underlying stream. We get real skipped bytes number of 
       * underlying stream then add outBuffer.remaining() to get skipped
       * bytes number from user's view.
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

  // Get underlying stream position.
  @Override
  public long getPos() throws IOException {
    checkStream();
    // Equals: ((Seekable) in).getPos() - outBuffer.remaining()
    return streamOffset - outBuffer.remaining();
  }
  
  // ByteBuffer read.
  @Override
  public int read(ByteBuffer buf) throws IOException {
    checkStream();
    if (in instanceof ByteBufferReadable) {
      int unread = outBuffer.remaining();
      if (unread > 0) { // Have unread decrypted data in buffer.
        int toRead = buf.remaining();
        if (toRead <= unread) {
          int limit = outBuffer.limit();
          outBuffer.limit(outBuffer.position() + toRead);
          buf.put(outBuffer);
          outBuffer.limit(limit);
          return toRead;
        } else {
          buf.put(outBuffer);
        }
      }
      
      int pos = buf.position();
      int n = ((ByteBufferReadable) in).read(buf);
      if (n > 0) {
        streamOffset += n; // Read n bytes
        decrypt(buf, n, pos);
      }
      return n;
    }

    throw new UnsupportedOperationException("ByteBuffer read unsupported " +
        "by input stream.");
  }
  
  /**
   * Decrypt all data in buf: total n bytes from given start position.
   * Output is also buf and same start position.
   * buf.position() and buf.limit() should be unchanged after decryption.
   */
  private void decrypt(ByteBuffer buf, int n, int start) 
      throws IOException {
    int pos = buf.position();
    int limit = buf.limit();
    int len = 0;
    while (len < n) {
      buf.position(start + len);
      buf.limit(start + len + Math.min(n - len, inBuffer.remaining()));
      inBuffer.put(buf);
      // Do decryption
      decrypt();
      
      buf.position(start + len);
      buf.limit(limit);
      len += outBuffer.remaining();
      buf.put(outBuffer);
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
      ByteBuffer buffer = ((HasEnhancedByteBufferAccess) in).
          read(bufferPool, maxLength, opts);
      if (buffer != null) {
        int n = buffer.remaining();
        if (n > 0) {
          streamOffset += buffer.remaining(); // Read n bytes
          int pos = buffer.position();
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
  
  private static int getBufferSize(Configuration conf) {
    return conf.getInt(HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, 
        HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);
  }
}
