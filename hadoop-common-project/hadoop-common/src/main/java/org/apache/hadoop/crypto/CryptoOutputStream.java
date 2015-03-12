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
import org.apache.hadoop.fs.CanSetDropBehind;
import org.apache.hadoop.fs.Syncable;

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
 *
 * Note that while some of this class' methods are synchronized, this is just to
 * match the threadsafety behavior of DFSOutputStream. See HADOOP-11710.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CryptoOutputStream extends FilterOutputStream implements 
    Syncable, CanSetDropBehind {
  private final byte[] oneByteBuf = new byte[1];
  private final CryptoCodec codec;
  private final Encryptor encryptor;
  private final int bufferSize;
  
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
    CryptoStreamUtils.checkCodec(codec);
    this.bufferSize = CryptoStreamUtils.checkBufferSize(codec, bufferSize);
    this.codec = codec;
    this.key = key.clone();
    this.initIV = iv.clone();
    this.iv = iv.clone();
    inBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    outBuffer = ByteBuffer.allocateDirect(this.bufferSize);
    this.streamOffset = streamOffset;
    try {
      encryptor = codec.createEncryptor();
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
    this(out, codec, CryptoStreamUtils.getBufferSize(codec.getConf()), 
        key, iv, streamOffset);
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
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    checkStream();
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || off > b.length || 
        len > b.length - off) {
      throw new IndexOutOfBoundsException();
    }
    while (len > 0) {
      final int remaining = inBuffer.remaining();
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
      /*
       * The plain text and cipher text have a 1:1 mapping, they start at the 
       * same position.
       */
      outBuffer.position(padding);
      padding = 0;
    }
    final int len = outBuffer.remaining();
    
    /*
     * If underlying stream supports {@link ByteBuffer} write in future, needs
     * refine here. 
     */
    final byte[] tmp = getTmpBuf();
    outBuffer.get(tmp, 0, len);
    out.write(tmp, 0, len);
    
    streamOffset += len;
    if (encryptor.isContextReset()) {
      /*
       * This code is generally not executed since the encryptor usually
       * maintains encryption context (e.g. the counter) internally. However,
       * some implementations can't maintain context so a re-init is necessary
       * after each encryption call.
       */
      updateEncryptor();
    }
  }
  
  /** Update the {@link #encryptor}: calculate counter and {@link #padding}. */
  private void updateEncryptor() throws IOException {
    final long counter =
        streamOffset / codec.getCipherSuite().getAlgorithmBlockSize();
    padding =
        (byte)(streamOffset % codec.getCipherSuite().getAlgorithmBlockSize());
    inBuffer.position(padding); // Set proper position for input data.
    codec.calculateIV(initIV, counter, iv);
    encryptor.init(key, iv);
  }
  
  private byte[] tmpBuf;
  private byte[] getTmpBuf() {
    if (tmpBuf == null) {
      tmpBuf = new byte[bufferSize];
    }
    return tmpBuf;
  }
  
  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      super.close();
      freeBuffers();
    } finally {
      closed = true;
    }
  }
  
  /**
   * To flush, we need to encrypt the data in the buffer and write to the 
   * underlying stream, then do the flush.
   */
  @Override
  public synchronized void flush() throws IOException {
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
  @Deprecated
  public void sync() throws IOException {
    hflush();
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
  
  /** Forcibly free the direct buffers. */
  private void freeBuffers() {
    CryptoStreamUtils.freeDB(inBuffer);
    CryptoStreamUtils.freeDB(outBuffer);
  }
}
