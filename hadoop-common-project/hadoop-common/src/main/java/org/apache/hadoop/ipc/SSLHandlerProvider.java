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
package org.apache.hadoop.ipc;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * This class encapsulated the logic required to create a SSLHandler that can
 * be attached to a Netty Pipeline.
 */
public class SSLHandlerProvider {
  // The standard name of the requested protocol. E.g. TLS.
  private String protocol;
  // The standard name of the key manager algorithm. E.g. SunX509.
  private String algorithm;

  // The user supplied keystore.
  private String keystore;
  // The type of the key store. E.g. JKS (Java Key Store).
  private String keystore_type;
  // A password may be given to unlock the keystore.
  private String keystore_password;
  // The password for recovering keys in the KeyStore.
  private String keystore_cert_password;


  // The user supplied truststore.
  private String truststore;
  // The type of the key store. E.g. JKS (Java Key Store).
  private String truststore_type;
  // A password may be given to unlock the keystore.
  private String truststore_password;

  // Instances of this class represent a secure socket protocol implementation
  // which acts as a factory for Secure Socket Factories or SSLEngines.
  private SslContext sslContext = null;

  // SSLFactory
  SSLFactory sslFactory;

  // The below given configuration information is the same as the YARN SSL
  // parameters. This shall be reused for Hadoop RPC also.
  public static final String SSL_SERVER_RESOURCE_DEFAULT = "ssl-server.xml";
  public static final String SSL_CLIENT_RESOURCE_DEFAULT = "ssl-client.xml";

  public static final String KEYSTORE_LOCATION =
      "ssl.server.keystore.location";

  public static final String KEYSTORE_TYPE = "ssl.server.keystore.type";

  public static final String KEYSTORE_PASSWORD_KEY =
      "ssl.server.keystore.password";

  public static final String KEY_PASSWORD_KEY =
      "ssl.server.keystore.keypassword";

  public static final String TRUSTSTORE_LOCATION =
      "ssl.client.truststore.location";

  public static final String TRUSTSTORE_TYPE = "ssl.client.truststore.type";

  public static final String TRUSTSTORE_PASSWORD_KEY =
      "ssl.client.truststore.password";

  Configuration sslConf = new Configuration(false);

  /**
   * SSLHandlerProvider Constructor.
   *
   * @param ssl_config_location The location of the ssl-server.xml or the
   *                          ssl-client.xml files.
   * @param protocol The standard name of the requested protocol. E.g. TLS.
   * @param algorithm The standard name of the key manager algorithm.
   *                  E.g. SunX509.
   * @param client   True if we are creating a cliet certificate.
   */
  public SSLHandlerProvider(String ssl_config_location, String protocol,
                            String algorithm, boolean client)
      throws GeneralSecurityException, IOException {
    this.protocol = protocol;
    this.algorithm = algorithm;

    if (!client) {
      sslConf.set(SSLFactory.SSL_SERVER_CONF_KEY, ssl_config_location);
      sslConf = SSLFactory.readSSLConfiguration(sslConf, SSLFactory.Mode.SERVER);
      this.keystore = sslConf.get(KEYSTORE_LOCATION);
      // this.keystore_type = sslConf.get(KEYSTORE_TYPE);
      this.keystore_type = "JKS";
      this.keystore_password = sslConf.get(KEYSTORE_PASSWORD_KEY);
      this.keystore_cert_password = sslConf.get(KEY_PASSWORD_KEY);
    }
    else {
      sslConf.set(SSLFactory.SSL_CLIENT_CONF_KEY, ssl_config_location);
      sslConf = SSLFactory.readSSLConfiguration(sslConf, SSLFactory.Mode.CLIENT);
      this.truststore = sslConf.get(TRUSTSTORE_LOCATION);
      // this.truststore_type = sslConf.get(TRUSTSTORE_TYPE);
      this.truststore_type = "JKS";
      this.truststore_password = sslConf.get(TRUSTSTORE_PASSWORD_KEY);
    }
  }

  /**
   * SSLHandlerProvider Constructor.
   *
   * @param protocol The standard name of the requested protocol. E.g. TLS.
   * @param algorithm The standard name of the key manager algorithm.
   *                  E.g. SunX509.
   * @param keystore The user supplied keystore. This cannot be null.
   * @param keystore_type The type of the key store. E.g. JKS (Java Key Store).
   * @param keystore_password A password may be given to unlock the keystore.
   * @param keystore_cert_password The password for recovering keys in the KeyStore.
   * @param truststore The user supplied keystore. This cannot be null.
   * @param truststore_type The type of the key store. E.g. JKS (Java Key Store).
   * @param truststore_password A password may be given to unlock the keystore.
   */
  public SSLHandlerProvider(String protocol,
                            String algorithm,
                            String keystore,
                            String keystore_type,
                            String keystore_password,
                            String keystore_cert_password,
                            String truststore,
                            String truststore_type,
                            String truststore_password) {
    this.protocol = protocol;
    this.algorithm = algorithm;

    this.keystore = keystore;
    this.keystore_type = "JKS";
    this.keystore_password = keystore_password;
    this.keystore_cert_password = keystore_cert_password;

    this.truststore = truststore;
    this.truststore_type = truststore_type;
    this.truststore_password = truststore_password;
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   * @param conf Configuration instance
   * @param alias name of the credential to retreive
   * @return String credential value or null
   */
  static String getPassword(Configuration conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    }
    catch (IOException ioe) {
      password = null;
    }
    return password;
  }

  private KeyStore initStore(String keystore, String keystore_type,
                             String keystore_password) throws Exception {
    // Storage factory for cryptographic keys and certificates.
    KeyStore ks = null;

    // Used to read the provided keystore. If the keystore cannot be read, fail
    // the context initialization with an exception.
    InputStream ks_inputStream = null;

    try {
      ks_inputStream = new FileInputStream(keystore);
      if (ks_inputStream == null) {
        throw new Exception("Unable to access the provided keystore");
      }

      // Fetch a KeyStore instance of the specified type and load the provided
      // keystore into it.
      ks = KeyStore.getInstance(keystore_type);
      ks.load(ks_inputStream, keystore_password != null ?
          keystore_password.toCharArray() : null);
    } finally {
      ks_inputStream.close();
    }
    return ks;
  }

  /**
   * Initialize a Server SSL context.
   *
   * @throws Exception
   *  1. If the provided keystore is null
   *  2. If the provided keystore cannot be read.
   *
   */
  private void initSSLContext() throws Exception {
    // Uses SunX509 as the default key manager algorithm.
    if (algorithm == null) {
      algorithm = "SunX509";
    }

    KeyManagerFactory kmf = null;

    if (keystore != null) {

      KeyStore ks = initStore(keystore, keystore_type, keystore_password);
      // Initialize the KeyManagerFactory and setup the key managers for
      // initializing the SSL context.
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, keystore_cert_password != null ?
          keystore_cert_password.toCharArray() : null);
    }

    TrustManagerFactory tmf = null;

    if (truststore != null) {
      // Storage factory for cryptographic keys and certificates.
      KeyStore ts = initStore(truststore, truststore_type, truststore_password);

      // Initialize the TrustManagerFactory and setup the Trust managers for
      // initializing the SSL context.
      tmf = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      tmf.init(ts);
    }

    if (kmf != null)
      sslContext = SslContextBuilder.forServer(kmf).build();
    else if (tmf != null)
      sslContext = SslContextBuilder.forClient().trustManager(tmf).build();
    else
      throw new Exception("Both kmf and tmf cannot be null");
  }

  /**
   * Fetch the SSL Handler instance.
   *
   * @return alloc ByteBufAllocator Instance.
   *
   * @throws Exception If the initialization of the SSL Context failed.
   */
  public SslHandler getSSLHandler(ByteBufAllocator alloc) throws Exception {

    // Initialize the SSL Context for the Server.
    initSSLContext();

    if (sslContext == null)
      throw new Exception("SSL Context cannot be null");

    return sslContext.newHandler(alloc);
  }

  /**
   * Fetch the SSL Handler instance.
   *
   * @return SSLHandler Instance.
   *
   * @throws Exception If the initialization of the SSL Context failed.
   */
  public SslHandler getSSLHandler(SocketChannel channel) throws Exception {

    if (channel == null)
      throw new Exception("channel cannot be null");

    return getSSLHandler(channel.alloc());
  }
}