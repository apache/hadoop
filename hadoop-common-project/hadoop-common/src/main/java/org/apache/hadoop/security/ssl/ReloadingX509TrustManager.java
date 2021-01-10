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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TrustManager} implementation that exposes a method, {@link #loadFrom(Path)}
 * to reload its configuration for example when the truststore file on disk changes.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ReloadingX509TrustManager implements X509TrustManager {

  static final Logger LOG =
      LoggerFactory.getLogger(ReloadingX509TrustManager.class);

  static final String RELOAD_ERROR_MESSAGE =
      "Could not load truststore (keep using existing one) : ";

  private String type;
  private String password;
  private AtomicReference<X509TrustManager> trustManagerRef;

  /**
   * Creates a reloadable trustmanager. The trustmanager reloads itself
   * if the underlying trustore file has changed.
   *
   * @param type type of truststore file, typically 'jks'.
   * @param location local path to the truststore file.
   * @param password password of the truststore file.
   * changed, in milliseconds.
   * @throws IOException thrown if the truststore could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be
   * initialized due to a security error.
   */
  public ReloadingX509TrustManager(String type, String location, String password)
    throws IOException, GeneralSecurityException {
    this.type = type;
    this.password = password;
    trustManagerRef = new AtomicReference<X509TrustManager>();
    trustManagerRef.set(loadTrustManager(Paths.get(location)));
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkClientTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown client chain certificate: " +
                                     chain[0].toString());
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
    throws CertificateException {
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      tm.checkServerTrusted(chain, authType);
    } else {
      throw new CertificateException("Unknown server chain certificate: " +
                                     chain[0].toString());
    }
  }

  private static final X509Certificate[] EMPTY = new X509Certificate[0];
  @Override
  public X509Certificate[] getAcceptedIssuers() {
    X509Certificate[] issuers = EMPTY;
    X509TrustManager tm = trustManagerRef.get();
    if (tm != null) {
      issuers = tm.getAcceptedIssuers();
    }
    return issuers;
  }

  public ReloadingX509TrustManager loadFrom(Path path) {
    try {
      this.trustManagerRef.set(loadTrustManager(path));
    } catch (Exception ex) {
      // The Consumer.accept interface forces us to convert to unchecked
      throw new RuntimeException(RELOAD_ERROR_MESSAGE, ex);
    }
    return this;
  }

  X509TrustManager loadTrustManager(Path path)
  throws IOException, GeneralSecurityException {
    X509TrustManager trustManager = null;
    KeyStore ks = KeyStore.getInstance(type);
    InputStream in = Files.newInputStream(path);
    try {
      ks.load(in, (password == null) ? null : password.toCharArray());
      LOG.debug("Loaded truststore '" + path + "'");
    } finally {
      in.close();
    }

    TrustManagerFactory trustManagerFactory = 
      TrustManagerFactory.getInstance(SSLFactory.SSLCERTIFICATE);
    trustManagerFactory.init(ks);
    TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
    for (TrustManager trustManager1 : trustManagers) {
      if (trustManager1 instanceof X509TrustManager) {
        trustManager = (X509TrustManager) trustManager1;
        break;
      }
    }
    return trustManager;
  }
}
