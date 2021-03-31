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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.text.MessageFormat;
import java.util.Timer;

/**
 * {@link KeyStoresFactory} implementation that reads the certificates from
 * keystore files.
 * <p>
 * If either the truststore or the keystore certificates file changes, it
 * would be refreshed under the corresponding wrapper implementation -
 * {@link ReloadingX509KeystoreManager} or {@link ReloadingX509TrustManager}.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FileBasedKeyStoresFactory implements KeyStoresFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileBasedKeyStoresFactory.class);


  /**
   * The name of the timer thread monitoring file changes.
   */
  public static final String SSL_MONITORING_THREAD_NAME = "SSL Certificates Store Monitor";

  /**
   * The refresh interval used to check if either of the truststore or keystore
   * certificate file has changed.
   */
  public static final String SSL_STORES_RELOAD_INTERVAL_TPL_KEY =
    "ssl.{0}.stores.reload.interval";

  public static final String SSL_KEYSTORE_LOCATION_TPL_KEY =
    "ssl.{0}.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD_TPL_KEY =
    "ssl.{0}.keystore.password";
  public static final String SSL_KEYSTORE_KEYPASSWORD_TPL_KEY =
    "ssl.{0}.keystore.keypassword";
  public static final String SSL_KEYSTORE_TYPE_TPL_KEY =
    "ssl.{0}.keystore.type";

  public static final String SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY =
    "ssl.{0}.truststore.reload.interval";
  public static final String SSL_TRUSTSTORE_LOCATION_TPL_KEY =
    "ssl.{0}.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD_TPL_KEY =
    "ssl.{0}.truststore.password";
  public static final String SSL_TRUSTSTORE_TYPE_TPL_KEY =
    "ssl.{0}.truststore.type";
  public static final String SSL_EXCLUDE_CIPHER_LIST =
      "ssl.{0}.exclude.cipher.list";

  /**
   * Default format of the keystore files.
   */
  public static final String DEFAULT_KEYSTORE_TYPE = "jks";

  /**
   * The default time interval in milliseconds used to check if either
   * of the truststore or keystore certificates file has changed and needs reloading.
   */
  public static final int DEFAULT_SSL_STORES_RELOAD_INTERVAL = 10000;

  private Configuration conf;
  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;
  private ReloadingX509TrustManager trustManager;
  private Timer fileMonitoringTimer;


  private void createTrustManagersFromConfiguration(SSLFactory.Mode mode,
                                                    String truststoreType,
                                                    String truststoreLocation,
                                                    long storesReloadInterval)
      throws IOException, GeneralSecurityException {
    String passwordProperty = resolvePropertyName(mode,
        SSL_TRUSTSTORE_PASSWORD_TPL_KEY);
    String truststorePassword = getPassword(conf, passwordProperty, "");
    if (truststorePassword.isEmpty()) {
      // An empty trust store password is legal; the trust store password
      // is only required when writing to a trust store. Otherwise it's
      // an optional integrity check.
      truststorePassword = null;
    }

    // Check if obsolete truststore specific reload interval is present for backward compatible
    long truststoreReloadInterval =
        conf.getLong(
            resolvePropertyName(mode, SSL_TRUSTSTORE_RELOAD_INTERVAL_TPL_KEY),
            storesReloadInterval);

    if (LOG.isDebugEnabled()) {
      LOG.debug(mode.toString() + " TrustStore: " + truststoreLocation +
          ", reloading at " + truststoreReloadInterval + " millis.");
    }

    trustManager = new ReloadingX509TrustManager(
        truststoreType,
        truststoreLocation,
        truststorePassword);

    if (truststoreReloadInterval > 0) {
      fileMonitoringTimer.schedule(
          new FileMonitoringTimerTask(
              Paths.get(truststoreLocation),
              path -> trustManager.loadFrom(path),
              exception -> LOG.error(ReloadingX509TrustManager.RELOAD_ERROR_MESSAGE, exception)),
          truststoreReloadInterval,
          truststoreReloadInterval);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(mode.toString() + " Loaded TrustStore: " + truststoreLocation);
    }
    trustManagers = new TrustManager[]{trustManager};
  }

  /**
   * Implements logic of initializing the KeyManagers with the options
   * to reload keystores.
   * @param mode client or server
   * @param keystoreType The keystore type.
   * @param storesReloadInterval The interval to check if the keystore certificates
   *                             file has changed.
   */
  private void createKeyManagersFromConfiguration(SSLFactory.Mode mode,
                                                  String keystoreType, long storesReloadInterval)
      throws GeneralSecurityException, IOException {
    String locationProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_LOCATION_TPL_KEY);
    String keystoreLocation = conf.get(locationProperty, "");
    if (keystoreLocation.isEmpty()) {
      throw new GeneralSecurityException("The property '" + locationProperty +
          "' has not been set in the ssl configuration file.");
    }
    String passwordProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_PASSWORD_TPL_KEY);
    String keystorePassword = getPassword(conf, passwordProperty, "");
    if (keystorePassword.isEmpty()) {
      throw new GeneralSecurityException("The property '" + passwordProperty +
          "' has not been set in the ssl configuration file.");
    }
    String keyPasswordProperty =
        resolvePropertyName(mode, SSL_KEYSTORE_KEYPASSWORD_TPL_KEY);
    // Key password defaults to the same value as store password for
    // compatibility with legacy configurations that did not use a separate
    // configuration property for key password.
    String keystoreKeyPassword = getPassword(
        conf, keyPasswordProperty, keystorePassword);
    if (LOG.isDebugEnabled()) {
      LOG.debug(mode.toString() + " KeyStore: " + keystoreLocation);
    }

    ReloadingX509KeystoreManager keystoreManager =  new ReloadingX509KeystoreManager(
        keystoreType,
        keystoreLocation,
        keystorePassword,
        keystoreKeyPassword);

    if (storesReloadInterval > 0) {
      fileMonitoringTimer.schedule(
          new FileMonitoringTimerTask(
              Paths.get(keystoreLocation),
              path -> keystoreManager.loadFrom(path),
              exception -> LOG.error(ReloadingX509KeystoreManager.RELOAD_ERROR_MESSAGE, exception)),
          storesReloadInterval,
          storesReloadInterval);
    }

    keyManagers = new KeyManager[] { keystoreManager };
  }

  /**
   * Resolves a property name to its client/server version if applicable.
   * <p>
   * NOTE: This method is public for testing purposes.
   *
   * @param mode client/server mode.
   * @param template property name template.
   * @return the resolved property name.
   */
  @VisibleForTesting
  public static String resolvePropertyName(SSLFactory.Mode mode,
                                           String template) {
    return MessageFormat.format(
        template, StringUtils.toLowerCase(mode.toString()));
  }

  /**
   * Sets the configuration for the factory.
   *
   * @param conf the configuration for the factory.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Returns the configuration of the factory.
   *
   * @return the configuration of the factory.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Initializes the keystores of the factory.
   *
   * @param mode if the keystores are to be used in client or server mode.
   * @throws IOException thrown if the keystores could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the keystores could not be
   * initialized due to a security error.
   */
  @Override
  public void init(SSLFactory.Mode mode)
    throws IOException, GeneralSecurityException {

    boolean requireClientCert =
      conf.getBoolean(SSLFactory.SSL_REQUIRE_CLIENT_CERT_KEY,
          SSLFactory.SSL_REQUIRE_CLIENT_CERT_DEFAULT);

    long storesReloadInterval = conf.getLong(
        resolvePropertyName(mode, SSL_STORES_RELOAD_INTERVAL_TPL_KEY),
        DEFAULT_SSL_STORES_RELOAD_INTERVAL);

    fileMonitoringTimer = new Timer(SSL_MONITORING_THREAD_NAME, true);

    // certificate store
    String keystoreType =
        conf.get(resolvePropertyName(mode, SSL_KEYSTORE_TYPE_TPL_KEY),
                 DEFAULT_KEYSTORE_TYPE);

    if (requireClientCert || mode == SSLFactory.Mode.SERVER) {
      createKeyManagersFromConfiguration(mode, keystoreType, storesReloadInterval);
    } else {
      KeyStore keystore = KeyStore.getInstance(keystoreType);
      keystore.load(null, null);
      KeyManagerFactory keyMgrFactory = KeyManagerFactory
              .getInstance(SSLFactory.SSLCERTIFICATE);

      keyMgrFactory.init(keystore, null);
      keyManagers = keyMgrFactory.getKeyManagers();
    }

    //trust store
    String truststoreType =
      conf.get(resolvePropertyName(mode, SSL_TRUSTSTORE_TYPE_TPL_KEY),
               DEFAULT_KEYSTORE_TYPE);

    String locationProperty =
      resolvePropertyName(mode, SSL_TRUSTSTORE_LOCATION_TPL_KEY);
    String truststoreLocation = conf.get(locationProperty, "");
    if (!truststoreLocation.isEmpty()) {
      createTrustManagersFromConfiguration(mode, truststoreType, truststoreLocation, storesReloadInterval);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The property '" + locationProperty + "' has not been set, " +
            "no TrustStore will be loaded");
      }
      trustManagers = null;
    }
  }

  String getPassword(Configuration conf, String alias, String defaultPass) {
    String password = defaultPass;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    }
    catch (IOException ioe) {
      LOG.warn("Exception while trying to get password for alias " + alias +
          ": " + ioe.getMessage());
    }
    return password;
  }

  /**
   * Releases any resources being used.
   */
  @Override
  public synchronized void destroy() {
    if (trustManager != null) {
      fileMonitoringTimer.cancel();
      trustManager = null;
      keyManagers = null;
      trustManagers = null;
    }
  }

  /**
   * Returns the keymanagers for owned certificates.
   *
   * @return the keymanagers for owned certificates.
   */
  @Override
  public KeyManager[] getKeyManagers() {
    return keyManagers;
  }

  /**
   * Returns the trustmanagers for trusted certificates.
   *
   * @return the trustmanagers for trusted certificates.
   */
  @Override
  public TrustManager[] getTrustManagers() {
    return trustManagers;
  }

}
