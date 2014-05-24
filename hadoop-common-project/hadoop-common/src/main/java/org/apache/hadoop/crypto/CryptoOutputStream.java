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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.Syncable;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT;

import com.google.common.base.Preconditions;

/**
 * CryptoOutputStream encrypts data. It is not thread-safe. AES CTR mode is
 * required in order to ensure that the plain text and cipher text have a 1:1
 * mapping. The encryption is buffer based. The key points of the encryption are
 * (1) calculating counter and (2) padding through stream position.
 * <p/>
 * counter = base + pos/(algorithm blocksize); 
 * padding = pos%(algorithm blocksize); 
 * <p/>
 * The underlying stream offset is maintained as state.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CryptoOutputStream extends FilterOutputStream implements 
    Syncable, CanSetDropBehind {
  private static final int MIN_BUFFER_SIZE = 512;
  private static final byte[] oneByteBuf = new byte[1];
  private final CryptoCodec codec;
  private final Encryptor encryptor;
  /**
   * Input data buffer. The data starts at inBuffer.position() and ends at 
   * inBuffer.limit().
   */
  private ByteBuffer inBuffer;
  /**
   * Encrypted data buffer. The data starts at outBuffer.position() and ends at 
   * outBuffer.limit();
   */
  private ByteBuffer outBuffer;
  private long streamOffset = 0; // Underlying stream offset.
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
  
  public CryptoOutputStream(OutputStream out, CryptoCodec codec, 
      int bufferSize, byte[] key, byte[] iv) throws IOException {
    this(out, codec, bufferSize, key, iv, 0);
  }
  
  public CryptoOutputStream(OutputStream out, CryptoCodec codec, 
      int bufferSize, byte[] key, byte[] iv, long streamOffset) 
      throws IOException {
    super(out);
    Preconditions.checkArgument(bufferSize >= MIN_BUFFER_SIZE, 
        "Minimum value of buffer size is 512.");
    this.key = key;
    this.initIV = iv;
    this.iv = iv.clone();
    inBuffer = ByteBuffer.allocateDirect(bufferSize);
    outBuffer = ByteBuffer.allocateDirect(bufferSize);
    this.streamOffset = streamOffset;
    this.codec = codec;
    try {
      encryptor = codec.getEncryptor();
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    updateEncryptor();
  }
  
  public CryptoOutputStream(OutputStream out, CryptoCodec codec, 
      byte[] key, byte[] iv) throws IOException {
    this(out, codec, key, iv, 0);
  }
  
  public CryptoOutputStream(OutputStream out, CryptoCodec codec, 
      byte[] key, byte[] iv, long streamOffset) throws IOException {
    this(out, codec, getBufferSize(codec.getConf()), key, iv, streamOffset);
  }
  
  public OutputStream getWrappedStream() {
    return out;
  }
  
  /**
   * Encryption is buffer based.
   * If there is enough room in {@link #inBuffer}, then write to this buffer.
   * If {@link #inBuffer} is full, then do encryption and write data to the
   * underlying stream.
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @throws IOException
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || off > b.length || 
        len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    while (len > 0) {
      int remaining = inBuffer.remaining();
      if (len < remaining) {
        inBuffer.put(b, off, len);
        len = 0;
      } else {
        inBuffer.put(b, off, remaining);
        off += remaining;
        len -= remaining;
        encrypt();
      }
    }
  }
  
  /**
   * Do the encryption, input is {@link #inBuffer} and output is 
   * {@link #outBuffer}.
   */
  private void encrypt() throws IOException {
    Preconditions.checkState(inBuffer.position() >= padding);
    if (inBuffer.position() == padding) {
      // There is no real data in the inBuffer.
      return;
    }
    inBuffer.flip();
    outBuffer.clear();
    encryptor.encrypt(inBuffer, outBuffer);
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
    int len = outBuffer.remaining();
    /**
     * If underlying stream supports {@link ByteBuffer} write in future, needs
     * refine here. 
     */
    final byte[] tmp = getTmpBuf();
    outBuffer.get(tmp, 0, len);
    out.write(tmp, 0, len);
    
    streamOffset += len;
    if (encryptor.isContextReset()) {
      /**
       * We will generally not get here.  For CTR mode, to improve
       * performance, we rely on the encryptor maintaining context, for
       * example to calculate the counter.  But some bad implementations
       * can't maintain context, and need us to re-init after doing
       * encryption.
       */
      updateEncryptor();
    }
  }
  
  /**
   * Update the {@link #encryptor}: calculate counter and {@link #padding}.
   */
  private void updateEncryptor() throws IOException {
    long counter = streamOffset / codec.getAlgorithmBlockSize();
    padding = (byte)(streamOffset % codec.getAlgorithmBlockSize());
    inBuffer.position(padding); // Set proper position for input data.
    codec.calculateIV(initIV, counter, iv);
    encryptor.init(key, iv);
  }
  
  private byte[] tmpBuf;
  private byte[] getTmpBuf() {
    if (tmpBuf == null) {
      tmpBuf = new byte[outBuffer.capacity()];
    }
    return tmpBuf;
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
  
  /**
   * To flush, we need to encrypt the data in buffer and write to underlying
   * stream, then do the flush.
   */
  @Override
  public void flush() throws IOException {
    checkStream();
    encrypt();
    super.flush();
  }
  
  @Override
  public void write(int b) throws IOException {
    oneByteBuf[0] = (byte)(b & 0xff);
    write(oneByteBuf, 0, oneByteBuf.length);
  }
  
  private void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }
  
  @Override
  public void setDropBehind(Boolean dropCache) throws IOException,
      UnsupportedOperationException {
    try {
      ((CanSetDropBehind) out).setDropBehind(dropCache);
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("This stream does not " +
          "support setting the drop-behind caching.");
    }
  }

  @Override
  public void hflush() throws IOException {
    flush();
    if (out instanceof Syncable) {
      ((Syncable)out).hflush();
    }
  }

  @Override
  public void hsync() throws IOException {
    flush();
    if (out instanceof Syncable) {
      ((Syncable)out).hsync();
    }
  }
  
  private static int getBufferSize(Configuration conf) {
    return conf.getInt(HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_KEY, 
        HADOOP_SECURITY_CRYPTO_BUFFER_SIZE_DEFAULT);
  }
}
