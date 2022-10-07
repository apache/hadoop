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


import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;

import org.apache.hadoop.fs.azurebfs.extensions.EncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.util.Preconditions;

/**
 * Class manages the encryptionContext and encryptionKey that needs to be added
 * to the headers to server request if Customer-Encryption-Context is enabled in
 * the configuration.
 * <br>
 * For fileCreation, the object helps in creating encryptionContext.
 * <br>
 * For all operations, the object helps in converting encryptionContext to
 * encryptionKey through the implementation of EncryptionContextProvider.
 * */
public class EncryptionAdapter implements Destroyable {
  private final String path;
  private ABFSKey encryptionContext;
  private ABFSKey encryptionKey;
  private final EncryptionContextProvider provider;

  /**
   * Following constructor called when the encryptionContext of file is known.
   * The server shall send encryptionContext as a String, the constructor shall
   * convert the string into a byte-array. The converted byte-array would be used
   * by the implementation of EncryptionContextProvider to create byte-array of
   * encryptionKey.
   * */
  public EncryptionAdapter(EncryptionContextProvider provider, String path,
      byte[] encryptionContext) throws IOException {
    this.provider = provider;
    this.path = path;
    Preconditions.checkNotNull(encryptionContext,
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
   * */
  public EncryptionAdapter(EncryptionContextProvider provider, String path)
      throws IOException {
    this.provider = provider;
    this.path = path;
    computeKeys();
  }

  private void computeKeys() throws IOException {
    if (encryptionContext == null) {
      encryptionContext = provider.getEncryptionContext(path);
    }
    Preconditions.checkNotNull(encryptionContext,
            "Encryption context should not be null.");
    if (encryptionKey == null) {
      encryptionKey = provider.getEncryptionKey(path, encryptionContext);
    }
    Preconditions.checkNotNull(encryptionKey, "Encryption key should not be null.");
  }

  public String getEncodedKey() throws IOException {
    return EncodingHelper.getBase64EncodedString(encryptionKey.getEncoded());
  }

  public String getEncodedKeySHA() throws IOException {
    return EncodingHelper.getBase64EncodedString(EncodingHelper.getSHA256Hash(encryptionKey.getEncoded()));
  }

  public String getEncodedContext() throws IOException {
    return EncodingHelper.getBase64EncodedString(encryptionContext.getEncoded());
  }

  public void destroy() throws DestroyFailedException {
    if (encryptionContext != null) {
      encryptionContext.destroy();
    }
    if (encryptionKey != null) {
      encryptionKey.destroy();
    }
  }
}
