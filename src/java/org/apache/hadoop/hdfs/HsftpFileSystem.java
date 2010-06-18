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

package org.apache.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * An implementation of a protocol for accessing filesystems over HTTPS. The
 * following implementation provides a limited, read-only interface to a
 * filesystem over HTTPS.
 * 
 * @see org.apache.hadoop.hdfs.server.namenode.ListPathsServlet
 * @see org.apache.hadoop.hdfs.server.namenode.FileDataServlet
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HsftpFileSystem extends HftpFileSystem {

  private static final long MM_SECONDS_PER_DAY = 1000 * 60 * 60 * 24;
  private volatile int ExpWarnDays = 0;

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setupSsl(conf);
    ExpWarnDays = conf.getInt("ssl.expiration.warn.days", 30);
  }

  /**
   * Set up SSL resources
   * 
   * @throws IOException
   */
  private static void setupSsl(Configuration conf) throws IOException {
    Configuration sslConf = new HdfsConfiguration(false);
    sslConf.addResource(conf.get(DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
                             DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_DEFAULT));
    FileInputStream fis = null;
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      KeyManager[] kms = null;
      TrustManager[] tms = null;
      if (sslConf.get("ssl.client.keystore.location") != null) {
        // initialize default key manager with keystore file and pass
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        KeyStore ks = KeyStore.getInstance(sslConf.get(
            "ssl.client.keystore.type", "JKS"));
        char[] ksPass = sslConf.get("ssl.client.keystore.password", "changeit")
            .toCharArray();
        fis = new FileInputStream(sslConf.get("ssl.client.keystore.location",
            "keystore.jks"));
        ks.load(fis, ksPass);
        kmf.init(ks, sslConf.get("ssl.client.keystore.keypassword", "changeit")
            .toCharArray());
        kms = kmf.getKeyManagers();
        fis.close();
        fis = null;
      }
      // initialize default trust manager with truststore file and pass
      if (sslConf.getBoolean("ssl.client.do.not.authenticate.server", false)) {
        // by pass trustmanager validation
        tms = new DummyTrustManager[] { new DummyTrustManager() };
      } else {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        KeyStore ts = KeyStore.getInstance(sslConf.get(
            "ssl.client.truststore.type", "JKS"));
        char[] tsPass = sslConf.get("ssl.client.truststore.password",
            "changeit").toCharArray();
        fis = new FileInputStream(sslConf.get("ssl.client.truststore.location",
            "truststore.jks"));
        ts.load(fis, tsPass);
        tmf.init(ts);
        tms = tmf.getTrustManagers();
      }
      sc.init(kms, tms, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      throw new IOException("Could not initialize SSLContext", e);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
  }

  @Override
  protected HttpURLConnection openConnection(String path, String query)
      throws IOException {
    try {
      final URL url = new URI("https", null, nnAddr.getHostName(), nnAddr
          .getPort(), path, query, null).toURL();
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      // bypass hostname verification
      conn.setHostnameVerifier(new DummyHostnameVerifier());
      conn.setRequestMethod("GET");
      conn.connect();

      // check cert expiration date
      final int warnDays = ExpWarnDays;
      if (warnDays > 0) { // make sure only check once
        ExpWarnDays = 0;
        long expTimeThreshold = warnDays * MM_SECONDS_PER_DAY
            + System.currentTimeMillis();
        X509Certificate[] clientCerts = (X509Certificate[]) conn
            .getLocalCertificates();
        if (clientCerts != null) {
          for (X509Certificate cert : clientCerts) {
            long expTime = cert.getNotAfter().getTime();
            if (expTime < expTimeThreshold) {
              StringBuilder sb = new StringBuilder();
              sb.append("\n Client certificate "
                  + cert.getSubjectX500Principal().getName());
              int dayOffSet = (int) ((expTime - System.currentTimeMillis()) / MM_SECONDS_PER_DAY);
              sb.append(" have " + dayOffSet + " days to expire");
              LOG.warn(sb.toString());
            }
          }
        }
      }
      return (HttpURLConnection) conn;
    } catch (URISyntaxException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  @Override
  public URI getUri() {
    try {
      return new URI("hsftp", null, nnAddr.getHostName(), nnAddr.getPort(),
          null, null, null);
    } catch (URISyntaxException e) {
      return null;
    }
  }

  /**
   * Dummy hostname verifier that is used to bypass hostname checking
   */
  protected static class DummyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  /**
   * Dummy trustmanager that is used to trust all server certificates
   */
  protected static class DummyTrustManager implements X509TrustManager {
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }

}
