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

package org.apache.hadoop.fs.azurebfs.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;

import static org.apache.hadoop.fs.azurebfs.security.EncodingHelper.getBase64EncodedString;

/**
 * Class manages the encryptionContext and encryptionKey that needs to be added
 * to the headers to server request if Customer-Encryption-Context is enabled in
 * the configuration.
 * <br>
 * For fileCreation, the object helps in creating encryptionContext through the
 * implementation of EncryptionContextProvider.
 * <br>
 * For all operations, the object helps in converting encryptionContext to
 * encryptionKey through the implementation of EncryptionContextProvider.
 */
public class ContextProviderEncryptionAdapter extends ContextEncryptionAdapter {
  private final String path;
  private final ABFSKey encryptionContext;
  private ABFSKey encryptionKey;
  private final EncryptionContextProvider provider;

  /**
   * Following constructor called when the encryptionContext of file is known.
   * The server shall send encryptionContext as a String, the constructor shall
   * convert the string into a byte-array. The converted byte-array would be used
   * by the implementation of EncryptionContextProvider to create byte-array of
   * encryptionKey.
   * @param provider developer's implementation of {@link EncryptionContextProvider}
   * @param path Path for which encryptionContext and encryptionKeys to be stored
   * in the object
   * @param encryptionContext encryptionContext for the path stored in the backend
   * @throws IOException throws back the exception it receives from the
   * {@link ContextProviderEncryptionAdapter#computeKeys()} method call.
   */
  public ContextProviderEncryptionAdapter(EncryptionContextProvider provider, String path,
      byte[] encryptionContext) throws IOException {
    this.provider = provider;
    this.path = path;
    Objects.requireNonNull(encryptionContext,
        "Encryption context should not be null.");
    this.encryptionContext = new ABFSKey(Base64.getDecoder().decode(encryptionContext));
    Arrays.fill(encryptionContext, (byte) 0);
    computeKeys();
  }

  /**
   * Following constructor called in case of createPath. Since, the path is not
   * on the server, encryptionContext is not there for the path. Implementation
   * of the EncryptionContextProvider would be used to create encryptionContext
   * from the path.
   * @param provider developer's implementation of {@link EncryptionContextProvider}
   * @param path file path for which encryptionContext and encryptionKeys to be
   * created and stored
   * @throws IOException throws back the exception it receives from the method call
   * to {@link EncryptionContextProvider} object.
   */
  public ContextProviderEncryptionAdapter(EncryptionContextProvider provider, String path)
      throws IOException {
    this.provider = provider;
    this.path = path;
    encryptionContext = provider.getEncryptionContext(path);
    Objects.requireNonNull(encryptionContext,
        "Encryption context should not be null.");
    computeKeys();
  }

  private void computeKeys() throws IOException {
    encryptionKey = provider.getEncryptionKey(path, encryptionContext);
    Objects.requireNonNull(encryptionKey, "Encryption key should not be null.");
  }

  @Override
  public String getEncodedKey() {
    return getBase64EncodedString(encryptionKey.getEncoded());
  }

  @Override
  public String getEncodedKeySHA() {
    return getBase64EncodedString(EncodingHelper.getSHA256Hash(encryptionKey.getEncoded()));
  }

  @Override
  public String getEncodedContext() {
    return getBase64EncodedString(encryptionContext.getEncoded());
  }

  @Override
  public void destroy() {
    if (encryptionContext != null) {
      encryptionContext.destroy();
    }
    if (encryptionKey != null) {
      encryptionKey.destroy();
    }
  }
}
