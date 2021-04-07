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
package org.apache.hadoop.security.ssl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of <code>X509KeyManager</code> that exposes a method,
 * {@link #loadFrom(Path)} to reload its configuration. Note that it is necessary
 * to implement the <code>X509ExtendedKeyManager</code> to properly delegate
 * the additional methods, otherwise the SSL handshake will fail.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReloadingX509KeystoreManager extends X509ExtendedKeyManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReloadingX509TrustManager.class);

  static final String RELOAD_ERROR_MESSAGE =
      "Could not load keystore (keep using existing one) : ";

  final private String type;
  final private String storePassword;
  final private String keyPassword;
  private AtomicReference<X509ExtendedKeyManager> keyManagerRef;

  /**
   * Construct a <code>Reloading509KeystoreManager</code>
   *
   * @param type type of keystore file, typically 'jks'.
   * @param location local path to the keystore file.
   * @param storePassword password of the keystore file.
   * @param keyPassword The password of the key.
   * @throws IOException
   * @throws GeneralSecurityException
   */
  public ReloadingX509KeystoreManager(String type, String location,
                                      String storePassword, String keyPassword)
      throws IOException, GeneralSecurityException {
    this.type = type;
    this.storePassword = storePassword;
    this.keyPassword = keyPassword;
    keyManagerRef = new AtomicReference<X509ExtendedKeyManager>();
    keyManagerRef.set(loadKeyManager(Paths.get(location)));
  }

  @Override
  public String chooseEngineClientAlias(String[] strings, Principal[] principals,
      SSLEngine sslEngine) {
    return keyManagerRef.get().chooseEngineClientAlias(strings, principals, sslEngine);
  }

  @Override
  public String chooseEngineServerAlias(String s, Principal[] principals,
      SSLEngine sslEngine) {
    return keyManagerRef.get().chooseEngineServerAlias(s, principals, sslEngine);
  }

  @Override
  public String[] getClientAliases(String s, Principal[] principals) {
    return keyManagerRef.get().getClientAliases(s, principals);
  }

  @Override
  public String chooseClientAlias(String[] strings, Principal[] principals,
      Socket socket) {
    return keyManagerRef.get().chooseClientAlias(strings, principals, socket);
  }

  @Override
  public String[] getServerAliases(String s, Principal[] principals) {
    return keyManagerRef.get().getServerAliases(s, principals);
  }

  @Override
  public String chooseServerAlias(String s, Principal[] principals,
      Socket socket) {
    return keyManagerRef.get().chooseServerAlias(s, principals, socket);
  }

  @Override
  public X509Certificate[] getCertificateChain(String s) {
    return keyManagerRef.get().getCertificateChain(s);
  }

  @Override
  public PrivateKey getPrivateKey(String s) {
    return keyManagerRef.get().getPrivateKey(s);
  }

  public ReloadingX509KeystoreManager loadFrom(Path path) {
    try {
      this.keyManagerRef.set(loadKeyManager(path));
    } catch (Exception ex) {
      // The Consumer.accept interface forces us to convert to unchecked
      throw new RuntimeException(ex);
    }
    return this;
  }

  private X509ExtendedKeyManager loadKeyManager(Path path)
      throws IOException, GeneralSecurityException {

    X509ExtendedKeyManager keyManager = null;
    KeyStore keystore = KeyStore.getInstance(type);

    try (InputStream is = Files.newInputStream(path)) {
      keystore.load(is, this.storePassword.toCharArray());
    }

    LOG.debug(" Loaded KeyStore: " + path.toFile().getAbsolutePath());

    KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(
        SSLFactory.SSLCERTIFICATE);
    keyMgrFactory.init(keystore,
        (keyPassword != null) ? keyPassword.toCharArray() : null);
    for (KeyManager candidate: keyMgrFactory.getKeyManagers()) {
      if (candidate instanceof X509ExtendedKeyManager) {
        keyManager = (X509ExtendedKeyManager)candidate;
        break;
      }
    }
    return keyManager;
  }
}
