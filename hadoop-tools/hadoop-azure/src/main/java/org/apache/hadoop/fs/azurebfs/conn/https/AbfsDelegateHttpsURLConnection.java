package org.apache.hadoop.fs.azurebfs.conn.https;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection;
import sun.net.www.protocol.https.DelegateHttpsURLConnection;

import org.apache.hadoop.fs.azurebfs.conn.AbfsHttpUrlConnection;

public class AbfsDelegateHttpsURLConnection extends
    AbstractDelegateHttpsURLConnection {

  public URLConnection httpsURLConnection = this;
  private static final Logger LOG = LoggerFactory.getLogger(
      AbfsDelegateHttpsURLConnection.class);

  javax.net.ssl.SSLSocketFactory sslSocketFactory;
  javax.net.ssl.HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

  public AbfsDelegateHttpsURLConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  @Override
  public javax.net.ssl.SSLSocketFactory getSSLSocketFactory() {
    return sslSocketFactory;
  }

  @Override
  public javax.net.ssl.HostnameVerifier getHostnameVerifier() {
    return hostnameVerifier;
  }

  /**
   * Sets the <code>SSLSocketFactory</code> to be used when this instance
   * creates sockets for secure https URL connections.
   * Ref: {@link HttpsURLConnection#setSSLSocketFactory(SSLSocketFactory)}
   *
   * @param sf the SSL socket factory
   * @throws IllegalArgumentException if the <code>SSLSocketFactory</code>
   *          parameter is null.
   * @throws SecurityException if a security manager exists and its
   *         <code>checkSetFactory</code> method does not allow
   *         a socket factory to be specified.
   * @see #getSSLSocketFactory()
   */
  public void setSSLSocketFactory(SSLSocketFactory sf) {
    if (sf == null) {
      throw new IllegalArgumentException(
          "no SSLSocketFactory specified");
    }

    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkSetFactory();
    }
    sslSocketFactory = sf;
  }

  @Override
  protected void plainConnect0() throws IOException {
    LOG.info("Going for connecting");
    long startTime = System.currentTimeMillis();
    super.plainConnect0();
    LOG.info("Connected in time: " + (System.currentTimeMillis() - startTime));
  }

  /*
   * Called by layered delegator's finalize() method to handle closing
   * the underlying object.
   */
  protected void finalize() throws Throwable {
    super.finalize();
  }
}
