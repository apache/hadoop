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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Decryptor {
  
  /**
   * Initialize the decryptor and the internal decryption context. 
   * reset.
   * @param key decryption key.
   * @param iv decryption initialization vector
   * @throws IOException if initialization fails
   */
  public void init(byte[] key, byte[] iv) throws IOException;
  
  /**
   * Indicate whether the decryption context is reset.
   * <p>
   * Certain modes, like CTR, require a different IV depending on the 
   * position in the stream. Generally, the decryptor maintains any necessary
   * context for calculating the IV and counter so that no reinit is necessary 
   * during the decryption. Reinit before each operation is inefficient.
   * @return boolean whether context is reset.
   */
  public boolean isContextReset();
  
  /**
   * This presents a direct interface decrypting with direct ByteBuffers.
   * <p>
   * This function does not always decrypt the entire buffer and may potentially
   * need to be called multiple times to process an entire buffer. The object 
   * may hold the decryption context internally.
   * <p>
   * Some implementations may require sufficient space in the destination 
   * buffer to decrypt the entire input buffer.
   * <p>
   * Upon return, inBuffer.position() will be advanced by the number of bytes
   * read and outBuffer.position() by bytes written. Implementations should 
   * not modify inBuffer.limit() and outBuffer.limit().
   * <p>
   * @param inBuffer a direct {@link ByteBuffer} to read from. inBuffer may 
   * not be null and inBuffer.remaining() must be {@literal >} 0
   * @param outBuffer a direct {@link ByteBuffer} to write to. outBuffer may 
   * not be null and outBuffer.remaining() must be {@literal >} 0
   * @throws IOException if decryption fails
   */
  public void decrypt(ByteBuffer inBuffer, ByteBuffer outBuffer) 
      throws IOException;
}
