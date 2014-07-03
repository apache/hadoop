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

import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.NativeCodeLoader;

import com.google.common.base.Preconditions;

/**
 * OpenSSL cipher using JNI.
 * Currently only AES-CTR is supported. It's flexible to add 
 * other crypto algorithms/modes.
 */
@InterfaceAudience.Private
public final class OpensslCipher {
  private static final Log LOG =
      LogFactory.getLog(OpensslCipher.class.getName());
  public static final int ENCRYPT_MODE = 1;
  public static final int DECRYPT_MODE = 0;
  
  /** Currently only support AES/CTR/NoPadding. */
  public static final int AES_CTR = 0;
  public static final int PADDING_NOPADDING = 0;
  
  private long context = 0;
  private final int algorithm;
  private final int padding;
  
  private static boolean nativeCipherLoaded = false;
  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsOpenssl()) {
      try {
        initIDs();
        nativeCipherLoaded = true;
      } catch (Throwable t) {
        LOG.error("Failed to load OpenSSL Cipher.", t);
      }
    }
  }
  
  public static boolean isNativeCodeLoaded() {
    return nativeCipherLoaded;
  }
  
  private OpensslCipher(long context, int algorithm, int padding) {
    this.context = context;
    this.algorithm = algorithm;
    this.padding = padding;
  }
  
  /**
   * Return an <code>OpensslCipher<code> object that implements the specified
   * algorithm.
   * 
   * @param algorithm currently only supports {@link #AES_CTR}
   * @param padding currently only supports {@link #PADDING_NOPADDING}
   * @return OpensslCipher an <code>OpensslCipher<code> object 
   * @throws NoSuchAlgorithmException
   * @throws NoSuchPaddingException
   */
  public static final OpensslCipher getInstance(int algorithm, 
      int padding) throws NoSuchAlgorithmException, NoSuchPaddingException {
    long context = initContext(algorithm, padding);
    return new OpensslCipher(context, algorithm, padding);
  }
  
  /**
   * Initialize this cipher with a key and IV.
   * 
   * @param mode {@link #ENCRYPT_MODE} or {@link #DECRYPT_MODE}
   * @param key crypto key
   * @param iv crypto iv
   */
  public void init(int mode, byte[] key, byte[] iv) {
    context = init(context, mode, algorithm, padding, key, iv);
  }
  
  /**
   * Continues a multiple-part encryption or decryption operation. The data
   * is encrypted or decrypted, depending on how this cipher was initialized.
   * <p/>
   * 
   * All <code>input.remaining()</code> bytes starting at 
   * <code>input.position()</code> are processed. The result is stored in
   * the output buffer.
   * <p/>
   * 
   * Upon return, the input buffer's position will be equal to its limit;
   * its limit will not have changed. The output buffer's position will have
   * advanced by n, when n is the value returned by this method; the output
   * buffer's limit will not have changed.
   * <p/>
   * 
   * If <code>output.remaining()</code> bytes are insufficient to hold the
   * result, a <code>ShortBufferException</code> is thrown.
   * 
   * @param input the input ByteBuffer
   * @param output the output ByteBuffer
   * @return int number of bytes stored in <code>output</code>
   * @throws ShortBufferException if there is insufficient space in the
   * output buffer
   */
  public int update(ByteBuffer input, ByteBuffer output) 
      throws ShortBufferException {
    checkState();
    Preconditions.checkArgument(input.isDirect() && output.isDirect(), 
        "Direct buffers are required.");
    int len = update(context, input, input.position(), input.remaining(),
        output, output.position(), output.remaining());
    input.position(input.limit());
    output.position(output.position() + len);
    return len;
  }
  
  /**
   * Finishes a multiple-part operation. The data is encrypted or decrypted,
   * depending on how this cipher was initialized.
   * <p/>
   * 
   * The result is stored in the output buffer. Upon return, the output buffer's
   * position will have advanced by n, where n is the value returned by this
   * method; the output buffer's limit will not have changed.
   * <p/>
   * 
   * If <code>output.remaining()</code> bytes are insufficient to hold the result,
   * a <code>ShortBufferException</code> is thrown.
   * <p/>
   * 
   * Upon finishing, this method resets this cipher object to the state it was
   * in when previously initialized. That is, the object is available to encrypt
   * or decrypt more data.
   * <p/>
   * 
   * If any exception is thrown, this cipher object need to be reset before it 
   * can be used again.
   * 
   * @param output the output ByteBuffer
   * @return int number of bytes stored in <code>output</code>
   * @throws ShortBufferException
   * @throws IllegalBlockSizeException
   * @throws BadPaddingException
   */
  public int doFinal(ByteBuffer output) throws ShortBufferException, 
      IllegalBlockSizeException, BadPaddingException {
    checkState();
    Preconditions.checkArgument(output.isDirect(), "Direct buffer is required.");
    int len = doFinal(context, output, output.position(), output.remaining());
    output.position(output.position() + len);
    return len;
  }
  
  /** Forcibly clean the context. */
  public void clean() {
    if (context != 0) {
      clean(context);
      context = 0;
    }
  }

  /** Check whether context is initialized. */
  private void checkState() {
    Preconditions.checkState(context != 0);
  }
  
  @Override
  protected void finalize() throws Throwable {
    clean();
  }

  private native static void initIDs();
  
  private native static long initContext(int alg, int padding);
  
  private native long init(long context, int mode, int alg, int padding, 
      byte[] key, byte[] iv);
  
  private native int update(long context, ByteBuffer input, int inputOffset, 
      int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength);
  
  private native int doFinal(long context, ByteBuffer output, int offset, 
      int maxOutputLength);
  
  private native void clean(long context);
  
  public native static String getLibraryName();
}
