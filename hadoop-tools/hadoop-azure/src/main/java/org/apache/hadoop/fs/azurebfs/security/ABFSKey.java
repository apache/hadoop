/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.security;

import javax.crypto.SecretKey;
import java.util.Arrays;

/**
 * Implementation of SecretKey that would be used by EncryptionAdapter object,
 * implementations of encryptionContextProvider to maintain the byteArrays of
 * encryptionContext and encryptionKey.
 */
public final class ABFSKey implements SecretKey {
  private byte[] bytes;

  public ABFSKey(byte[] bytes) {
    if (bytes != null) {
      this.bytes = bytes.clone();
    }
  }

  @Override
  public String getAlgorithm() {
    return null;
  }

  @Override
  public String getFormat() {
    return null;
  }

  /**
   * This method to be called by implementations of EncryptionContextProvider interface.
   * Method returns clone of the original bytes array to prevent findbugs flags.
   */
  @Override
  public byte[] getEncoded() {
    if (bytes == null) {
      return null;
    }
    return bytes.clone();
  }

  @Override
  public void destroy() {
    Arrays.fill(bytes, (byte) 0);
  }
}
