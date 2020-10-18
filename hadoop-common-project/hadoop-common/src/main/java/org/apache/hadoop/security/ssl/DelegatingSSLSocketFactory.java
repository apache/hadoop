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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SSLSocketFactory} that can delegate to various SSL implementations.
 * Specifically, either OpenSSL or JSSE can be used. OpenSSL offers better
 * performance than JSSE and is made available via the
 * <a href="https://github.com/wildfly/wildfly-openssl">wildlfy-openssl</a>
 * library.
 *
 * <p>
 *   The factory has several different modes of operation:
 *   <ul>
 *     <li>OpenSSL: Uses the wildly-openssl library to delegate to the
 *     system installed OpenSSL. If the wildfly-openssl integration is not
 *     properly setup, an exception is thrown.</li>
 *     <li>Default: Attempts to use the OpenSSL mode, if it cannot load the
 *     necessary libraries, it falls back to the Default_JSEE mode.</li>
 *     <li>Default_JSSE: Delegates to the JSSE implementation of SSL, but
 *     it disables the GCM cipher when running on Java 8.</li>
 *     <li>Default_JSSE_with_GCM: Delegates to the JSSE implementation of
 *     SSL with no modification to the list of enabled ciphers.</li>
 *   </ul>
 * </p>
 *
 * In order to load OpenSSL, applications must ensure the wildfly-openssl
 * artifact is on the classpath. Currently, only ABFS declares
 * wildfly-openssl as an explicit dependency.
 */
public final class DelegatingSSLSocketFactory extends SSLSocketFactory {

  /**
   * Default indicates Ordered, preferred OpenSSL, if failed to load then fall
   * back to Default_JSSE.
   *
   * <p>
   *   Default_JSSE is not truly the the default JSSE implementation because
   *   the GCM cipher is disabled when running on Java 8. However, the name
   *   was not changed in order to preserve backwards compatibility. Instead,
   *   a new mode called Default_JSSE_with_GCM delegates to the default JSSE
   *   implementation with no changes to the list of enabled ciphers.
   * </p>
   */
  public enum SSLChannelMode {
    OpenSSL,
    Default,
    Default_JSSE,
    Default_JSSE_with_GCM
  }

  private static DelegatingSSLSocketFactory instance = null;
  private static final Logger LOG = LoggerFactory.getLogger(
          DelegatingSSLSocketFactory.class);
  private String providerName;
  private SSLContext ctx;
  private String[] ciphers;
  private SSLChannelMode channelMode;

  // This should only be modified within the #initializeDefaultFactory
  // method which is synchronized
  private boolean openSSLProviderRegistered;

  /**
   * Initialize a singleton SSL socket factory.
   *
   * @param preferredMode applicable only if the instance is not initialized.
   * @throws IOException if an error occurs.
   */
  public static synchronized void initializeDefaultFactory(
      SSLChannelMode preferredMode) throws IOException {
    if (instance == null) {
      instance = new DelegatingSSLSocketFactory(preferredMode);
    }
  }

  /**
   * For testing only: reset the socket factory.
   */
  @VisibleForTesting
  public static synchronized void resetDefaultFactory() {
    LOG.info("Resetting default SSL Socket Factory");
    instance = null;
  }

  /**
   * Singleton instance of the SSLSocketFactory.
   *
   * SSLSocketFactory must be initialized with appropriate SSLChannelMode
   * using initializeDefaultFactory method.
   *
   * @return instance of the SSLSocketFactory, instance must be initialized by
   * initializeDefaultFactory.
   */
  public static DelegatingSSLSocketFactory getDefaultFactory() {
    return instance;
  }

  private DelegatingSSLSocketFactory(SSLChannelMode preferredChannelMode)
      throws IOException {
    try {
      initializeSSLContext(preferredChannelMode);
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new IOException(e);
    }

    // Get list of supported cipher suits from the SSL factory.
    SSLSocketFactory factory = ctx.getSocketFactory();
    String[] defaultCiphers = factory.getSupportedCipherSuites();
    String version = System.getProperty("java.version");

    ciphers = (channelMode == SSLChannelMode.Default_JSSE
        && version.startsWith("1.8"))
        ? alterCipherList(defaultCiphers) : defaultCiphers;

    providerName = ctx.getProvider().getName() + "-"
        + ctx.getProvider().getVersion();
  }

  private void initializeSSLContext(SSLChannelMode preferredChannelMode)
      throws NoSuchAlgorithmException, KeyManagementException, IOException {
    LOG.debug("Initializing SSL Context to channel mode {}",
        preferredChannelMode);
    switch (preferredChannelMode) {
    case Default:
      try {
        bindToOpenSSLProvider();
        channelMode = SSLChannelMode.OpenSSL;
      } catch (LinkageError | NoSuchAlgorithmException | RuntimeException e) {
        LOG.debug("Failed to load OpenSSL. Falling back to the JSSE default.",
            e);
        ctx = SSLContext.getDefault();
        channelMode = SSLChannelMode.Default_JSSE;
      }
      break;
    case OpenSSL:
      bindToOpenSSLProvider();
      channelMode = SSLChannelMode.OpenSSL;
      break;
    case Default_JSSE:
      ctx = SSLContext.getDefault();
      channelMode = SSLChannelMode.Default_JSSE;
      break;
    case Default_JSSE_with_GCM:
      ctx = SSLContext.getDefault();
      channelMode = SSLChannelMode.Default_JSSE_with_GCM;
      break;
    default:
      throw new IOException("Unknown channel mode: "
          + preferredChannelMode);
    }
  }

  /**
   * Bind to the OpenSSL provider via wildfly.
   * This MUST be the only place where wildfly classes are referenced,
   * so ensuring that any linkage problems only surface here where they may
   * be caught by the initialization code.
   */
  private void bindToOpenSSLProvider()
      throws NoSuchAlgorithmException, KeyManagementException {
    if (!openSSLProviderRegistered) {
      LOG.debug("Attempting to register OpenSSL provider");
      org.wildfly.openssl.OpenSSLProvider.register();
      openSSLProviderRegistered = true;
    }
    // Strong reference needs to be kept to logger until initialization of
    // SSLContext finished (see HADOOP-16174):
    java.util.logging.Logger logger = java.util.logging.Logger.getLogger(
        "org.wildfly.openssl.SSL");
    Level originalLevel = logger.getLevel();
    try {
      logger.setLevel(Level.WARNING);
      ctx = SSLContext.getInstance("openssl.TLS");
      ctx.init(null, null, null);
    } finally {
      logger.setLevel(originalLevel);
    }
  }

  public String getProviderName() {
    return providerName;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return ciphers.clone();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return ciphers.clone();
  }

  /**
   * Get the channel mode of this instance.
   * @return a channel mode.
   */
  public SSLChannelMode getChannelMode() {
    return channelMode;
  }

  public Socket createSocket() throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    return configureSocket(factory.createSocket());
  }

  @Override
  public Socket createSocket(Socket s, String host, int port,
                             boolean autoClose) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();

    return configureSocket(
        factory.createSocket(s, host, port, autoClose));
  }

  @Override
  public Socket createSocket(InetAddress address, int port,
                             InetAddress localAddress, int localPort)
      throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    return configureSocket(factory
        .createSocket(address, port, localAddress, localPort));
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost,
                             int localPort) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();

    return configureSocket(factory
        .createSocket(host, port, localHost, localPort));
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();

    return configureSocket(factory.createSocket(host, port));
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();

    return configureSocket(factory.createSocket(host, port));
  }

  private Socket configureSocket(Socket socket) {
    ((SSLSocket) socket).setEnabledCipherSuites(ciphers);
    return socket;
  }

  private String[] alterCipherList(String[] defaultCiphers) {

    ArrayList<String> preferredSuites = new ArrayList<>();

    // Remove GCM mode based ciphers from the supported list.
    for (int i = 0; i < defaultCiphers.length; i++) {
      if (defaultCiphers[i].contains("_GCM_")) {
        LOG.debug("Removed Cipher - {} from list of enabled SSLSocket ciphers",
                defaultCiphers[i]);
      } else {
        preferredSuites.add(defaultCiphers[i]);
      }
    }

    ciphers = preferredSuites.toArray(new String[0]);
    return ciphers;
  }
}
