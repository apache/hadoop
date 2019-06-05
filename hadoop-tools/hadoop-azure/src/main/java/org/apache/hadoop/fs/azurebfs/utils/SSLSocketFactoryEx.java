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

package org.apache.hadoop.fs.azurebfs.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.openssl.OpenSSLProvider;
import org.wildfly.openssl.SSL;


/**
 * Extension to use native OpenSSL library instead of JSSE for better
 * performance.
 *
 */
public final class SSLSocketFactoryEx extends SSLSocketFactory {

  /**
   * Default indicates Ordered, preferred OpenSSL, if failed to load then fall
   * back to Default_JSSE
   */
  public enum SSLChannelMode {
    OpenSSL,
    Default,
    Default_JSSE
  }

  private static SSLSocketFactoryEx instance = null;
  private static final Logger LOG = LoggerFactory.getLogger(
      SSLSocketFactoryEx.class);
  private String providerName;
  private SSLContext ctx;
  private String[] ciphers;
  private SSLChannelMode channelMode;

  /**
   * Initialize a singleton SSL socket factory.
   *
   * @param preferredMode applicable only if the instance is not initialized.
   * @throws IOException if an error occurs.
   */
  public static synchronized void initializeDefaultFactory(
      SSLChannelMode preferredMode) throws IOException {
    if (instance == null) {
      instance = new SSLSocketFactoryEx(preferredMode);
    }
  }

  /**
   * Singletone instance of the SSLSocketFactory.
   *
   * SSLSocketFactory must be initialized with appropriate SSLChannelMode
   * using initializeDefaultFactory method.
   *
   * @return instance of the SSLSocketFactory, instance must be initialized by
   * initializeDefaultFactory.
   */
  public static SSLSocketFactoryEx getDefaultFactory() {
    return instance;
  }

  static {
    OpenSSLProvider.register();
  }

  private SSLSocketFactoryEx(SSLChannelMode preferredChannelMode)
      throws IOException {
    try {
      initializeSSLContext(preferredChannelMode);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException(e);
    } catch (KeyManagementException e) {
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
      throws NoSuchAlgorithmException, KeyManagementException {
    switch (preferredChannelMode) {
      case Default:
        try {
          java.util.logging.Logger logger = java.util.logging.Logger.getLogger(SSL.class.getName());
          logger.setLevel(Level.WARNING);
          ctx = SSLContext.getInstance("openssl.TLS");
          ctx.init(null, null, null);
          // Strong reference needs to be kept to logger until initialization of SSLContext finished (see HADOOP-16174):
          logger.setLevel(Level.INFO);
          channelMode = SSLChannelMode.OpenSSL;
        } catch (NoSuchAlgorithmException e) {
          LOG.warn("Failed to load OpenSSL. Falling back to the JSSE default.");
          ctx = SSLContext.getDefault();
          channelMode = SSLChannelMode.Default_JSSE;
        }
        break;
      case OpenSSL:
        ctx = SSLContext.getInstance("openssl.TLS");
        ctx.init(null, null, null);
        channelMode = SSLChannelMode.OpenSSL;
        break;
      case Default_JSSE:
        ctx = SSLContext.getDefault();
        channelMode = SSLChannelMode.Default_JSSE;
        break;
      default:
        throw new AssertionError("Unknown channel mode: "
            + preferredChannelMode);
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

  public Socket createSocket() throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket();
    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(Socket s, String host, int port,
                             boolean autoClose) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(s, host, port, autoClose);

    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(InetAddress address, int port,
                             InetAddress localAddress, int localPort)
      throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory
        .createSocket(address, port, localAddress, localPort);

    configureSocket(ss);
    return ss;
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost,
                             int localPort) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory
        .createSocket(host, port, localHost, localPort);

    configureSocket(ss);

    return ss;
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(host, port);

    configureSocket(ss);

    return ss;
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    SSLSocketFactory factory = ctx.getSocketFactory();
    SSLSocket ss = (SSLSocket) factory.createSocket(host, port);

    configureSocket(ss);

    return ss;
  }

  private void configureSocket(SSLSocket ss) throws SocketException {
    ss.setEnabledCipherSuites(ciphers);
  }

  private String[] alterCipherList(String[] defaultCiphers) {

    ArrayList<String> preferredSuits = new ArrayList<>();

    // Remove GCM mode based ciphers from the supported list.
    for (int i = 0; i < defaultCiphers.length; i++) {
      if (defaultCiphers[i].contains("_GCM_")) {
        LOG.debug("Removed Cipher - " + defaultCiphers[i]);
      } else {
        preferredSuits.add(defaultCiphers[i]);
      }
    }

    ciphers = preferredSuits.toArray(new String[0]);
    return ciphers;
  }
}