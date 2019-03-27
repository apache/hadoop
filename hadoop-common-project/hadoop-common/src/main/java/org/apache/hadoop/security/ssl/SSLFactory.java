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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Factory that creates SSLEngine and SSLSocketFactory instances using
 * Hadoop configuration information.
 * <p>
 * This SSLFactory uses a {@link ReloadingX509TrustManager} instance,
 * which reloads public keys if the truststore file changes.
 * <p>
 * This factory is used to configure HTTPS in Hadoop HTTP based endpoints, both
 * client and server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SSLFactory implements ConnectionConfigurator {
  static final Logger LOG = LoggerFactory.getLogger(SSLFactory.class);

  @InterfaceAudience.Private
  public enum Mode { CLIENT, SERVER }

  public static final String SSL_CLIENT_CONF_KEY = "hadoop.ssl.client.conf";
  public static final String SSL_CLIENT_CONF_DEFAULT = "ssl-client.xml";
  public static final String SSL_SERVER_CONF_KEY = "hadoop.ssl.server.conf";
  public static final String SSL_SERVER_CONF_DEFAULT = "ssl-server.xml";

  public static final String SSL_REQUIRE_CLIENT_CERT_KEY =
      "hadoop.ssl.require.client.cert";
  public static final boolean SSL_REQUIRE_CLIENT_CERT_DEFAULT = false;
  public static final String SSL_HOSTNAME_VERIFIER_KEY =
      "hadoop.ssl.hostname.verifier";
  public static final String SSL_ENABLED_PROTOCOLS_KEY =
      "hadoop.ssl.enabled.protocols";
  public static final String SSL_ENABLED_PROTOCOLS_DEFAULT =
      "TLSv1.1,TLSv1.2";

  public static final String SSL_SERVER_NEED_CLIENT_AUTH =
      "ssl.server.need.client.auth";
  public static final boolean SSL_SERVER_NEED_CLIENT_AUTH_DEFAULT = false;

  public static final String SSL_SERVER_KEYSTORE_LOCATION =
      "ssl.server.keystore.location";
  public static final String SSL_SERVER_KEYSTORE_PASSWORD =
      "ssl.server.keystore.password";
  public static final String SSL_SERVER_KEYSTORE_TYPE =
      "ssl.server.keystore.type";
  public static final String SSL_SERVER_KEYSTORE_TYPE_DEFAULT = "jks";
  public static final String SSL_SERVER_KEYSTORE_KEYPASSWORD =
      "ssl.server.keystore.keypassword";

  public static final String SSL_SERVER_TRUSTSTORE_LOCATION =
      "ssl.server.truststore.location";
  public static final String SSL_SERVER_TRUSTSTORE_PASSWORD =
      "ssl.server.truststore.password";
  public static final String SSL_SERVER_TRUSTSTORE_TYPE =
      "ssl.server.truststore.type";
  public static final String SSL_SERVER_TRUSTSTORE_TYPE_DEFAULT = "jks";

  public static final String SSL_SERVER_EXCLUDE_CIPHER_LIST =
      "ssl.server.exclude.cipher.list";

  public static final String SSLCERTIFICATE = IBM_JAVA?"ibmX509":"SunX509";

  public static final String KEYSTORES_FACTORY_CLASS_KEY =
    "hadoop.ssl.keystores.factory.class";

  private Configuration conf;
  private Mode mode;
  private boolean requireClientCert;
  private SSLContext context;
  // the java keep-alive cache relies on instance equivalence of the SSL socket
  // factory.  in many java versions, SSLContext#getSocketFactory always
  // returns a new instance which completely breaks the cache...
  private SSLSocketFactory socketFactory;
  private HostnameVerifier hostnameVerifier;
  private KeyStoresFactory keystoresFactory;

  private String[] enabledProtocols = null;
  private List<String> excludeCiphers;

  /**
   * Creates an SSLFactory.
   *
   * @param mode SSLFactory mode, client or server.
   * @param conf Hadoop configuration from where the SSLFactory configuration
   * will be read.
   */
  public SSLFactory(Mode mode, Configuration conf) {
    this.conf = conf;
    if (mode == null) {
      throw new IllegalArgumentException("mode cannot be NULL");
    }
    this.mode = mode;
    Configuration sslConf = readSSLConfiguration(conf, mode);

    requireClientCert = sslConf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY,
        SSL_REQUIRE_CLIENT_CERT_DEFAULT);

    Class<? extends KeyStoresFactory> klass
      = conf.getClass(KEYSTORES_FACTORY_CLASS_KEY,
                      FileBasedKeyStoresFactory.class, KeyStoresFactory.class);
    keystoresFactory = ReflectionUtils.newInstance(klass, sslConf);

    enabledProtocols = conf.getStrings(SSL_ENABLED_PROTOCOLS_KEY,
        SSL_ENABLED_PROTOCOLS_DEFAULT);
    excludeCiphers = Arrays.asList(
        sslConf.getTrimmedStrings(SSL_SERVER_EXCLUDE_CIPHER_LIST));
    if (LOG.isDebugEnabled()) {
      LOG.debug("will exclude cipher suites: {}",
          StringUtils.join(",", excludeCiphers));
    }
  }

  public static Configuration readSSLConfiguration(Configuration conf,
                                                   Mode mode) {
    Configuration sslConf = new Configuration(false);
    sslConf.setBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, conf.getBoolean(
        SSL_REQUIRE_CLIENT_CERT_KEY, SSL_REQUIRE_CLIENT_CERT_DEFAULT));
    String sslConfResource;
    if (mode == Mode.CLIENT) {
      sslConfResource = conf.get(SSL_CLIENT_CONF_KEY,
          SSL_CLIENT_CONF_DEFAULT);
    } else {
      sslConfResource = conf.get(SSL_SERVER_CONF_KEY,
          SSL_SERVER_CONF_DEFAULT);
    }
    sslConf.addResource(sslConfResource);
    return sslConf;
  }

  /**
   * Initializes the factory.
   *
   * @throws  GeneralSecurityException thrown if an SSL initialization error
   * happened.
   * @throws IOException thrown if an IO error happened while reading the SSL
   * configuration.
   */
  public void init() throws GeneralSecurityException, IOException {
    keystoresFactory.init(mode);
    context = SSLContext.getInstance("TLS");
    context.init(keystoresFactory.getKeyManagers(),
                 keystoresFactory.getTrustManagers(), null);
    context.getDefaultSSLParameters().setProtocols(enabledProtocols);
    if (mode == Mode.CLIENT) {
      socketFactory = context.getSocketFactory();
    }
    hostnameVerifier = getHostnameVerifier(conf);
  }

  private HostnameVerifier getHostnameVerifier(Configuration conf)
      throws GeneralSecurityException, IOException {
    return getHostnameVerifier(StringUtils.toUpperCase(
        conf.get(SSL_HOSTNAME_VERIFIER_KEY, "DEFAULT").trim()));
  }

  public static HostnameVerifier getHostnameVerifier(String verifier)
    throws GeneralSecurityException, IOException {
    HostnameVerifier hostnameVerifier;
    if (verifier.equals("DEFAULT")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT;
    } else if (verifier.equals("DEFAULT_AND_LOCALHOST")) {
      hostnameVerifier = SSLHostnameVerifier.DEFAULT_AND_LOCALHOST;
    } else if (verifier.equals("STRICT")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT;
    } else if (verifier.equals("STRICT_IE6")) {
      hostnameVerifier = SSLHostnameVerifier.STRICT_IE6;
    } else if (verifier.equals("ALLOW_ALL")) {
      hostnameVerifier = SSLHostnameVerifier.ALLOW_ALL;
    } else {
      throw new GeneralSecurityException("Invalid hostname verifier: " +
                                         verifier);
    }
    return hostnameVerifier;
  }

  /**
   * Releases any resources being used.
   */
  public void destroy() {
    keystoresFactory.destroy();
  }
  /**
   * Returns the SSLFactory KeyStoresFactory instance.
   *
   * @return the SSLFactory KeyStoresFactory instance.
   */
  public KeyStoresFactory getKeystoresFactory() {
    return keystoresFactory;
  }

  /**
   * Returns a configured SSLEngine.
   *
   * @return the configured SSLEngine.
   * @throws GeneralSecurityException thrown if the SSL engine could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLEngine createSSLEngine()
    throws GeneralSecurityException, IOException {
    SSLEngine sslEngine = context.createSSLEngine();
    if (mode == Mode.CLIENT) {
      sslEngine.setUseClientMode(true);
    } else {
      sslEngine.setUseClientMode(false);
      sslEngine.setNeedClientAuth(requireClientCert);
      disableExcludedCiphers(sslEngine);
    }
    sslEngine.setEnabledProtocols(enabledProtocols);
    return sslEngine;
  }

  private void disableExcludedCiphers(SSLEngine sslEngine) {
    String[] cipherSuites = sslEngine.getEnabledCipherSuites();

    ArrayList<String> defaultEnabledCipherSuites =
        new ArrayList<String>(Arrays.asList(cipherSuites));
    Iterator iterator = excludeCiphers.iterator();

    while(iterator.hasNext()) {
      String cipherName = (String)iterator.next();
      if(defaultEnabledCipherSuites.contains(cipherName)) {
        defaultEnabledCipherSuites.remove(cipherName);
        LOG.debug("Disabling cipher suite {}.", cipherName);
      }
    }

    cipherSuites = defaultEnabledCipherSuites.toArray(
        new String[defaultEnabledCipherSuites.size()]);
    sslEngine.setEnabledCipherSuites(cipherSuites);
  }

  /**
   * Returns a configured SSLServerSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLServerSocketFactory createSSLServerSocketFactory()
    throws GeneralSecurityException, IOException {
    if (mode != Mode.SERVER) {
      throw new IllegalStateException(
          "Factory is not in SERVER mode. Actual mode is " + mode.toString());
    }
    return context.getServerSocketFactory();
  }

  /**
   * Returns a configured SSLSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLSocketFactory createSSLSocketFactory()
    throws GeneralSecurityException, IOException {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException(
          "Factory is not in CLIENT mode. Actual mode is " + mode.toString());
    }
    return socketFactory;
  }

  /**
   * Returns the hostname verifier it should be used in HttpsURLConnections.
   *
   * @return the hostname verifier.
   */
  public HostnameVerifier getHostnameVerifier() {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException(
          "Factory is not in CLIENT mode. Actual mode is " + mode.toString());
    }
    return hostnameVerifier;
  }

  /**
   * Returns if client certificates are required or not.
   *
   * @return if client certificates are required or not.
   */
  public boolean isClientCertRequired() {
    return requireClientCert;
  }

  /**
   * If the given {@link HttpURLConnection} is an {@link HttpsURLConnection}
   * configures the connection with the {@link SSLSocketFactory} and
   * {@link HostnameVerifier} of this SSLFactory, otherwise does nothing.
   *
   * @param conn the {@link HttpURLConnection} instance to configure.
   * @return the configured {@link HttpURLConnection} instance.
   *
   * @throws IOException if an IO error occurred.
   */
  @Override
  public HttpURLConnection configure(HttpURLConnection conn)
    throws IOException {
    if (conn instanceof HttpsURLConnection) {
      HttpsURLConnection sslConn = (HttpsURLConnection) conn;
      try {
        sslConn.setSSLSocketFactory(createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      sslConn.setHostnameVerifier(getHostnameVerifier());
      conn = sslConn;
    }
    return conn;
  }
}
