package org.apache.hadoop.fs.azurebfs.conn;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import sun.net.www.http.HttpClient;
import sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection;
import sun.net.www.protocol.https.Handler;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;

public class AbfsHttpsUrlConnection extends
    AbstractDelegateHttpsURLConnection {

  private Boolean getOutputStreamFailed = false;

  SSLSocketFactory sslSocketFactory;
  HostnameVerifier hostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

  public Long timeTaken;
  public Boolean isFromCache = true;


  public void registerGetOutputStreamFailure() {
    getOutputStreamFailed = true;
  }

  private static Set<HttpClient> httpClientSet = new HashSet<>();

  public AbfsHttpsUrlConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  @Override
  public javax.net.ssl.SSLSocketFactory getSSLSocketFactory() {
    return sslSocketFactory;
  }

  @Override
  public void connect() throws IOException {
    Long start = System.currentTimeMillis();
    super.connect();
    timeTaken = System.currentTimeMillis() - start;
    isFromCache = http.isCachedConnection();
//    if(!httpClientSet.contains(http)) {
//      isFromCache = false;
//    }
//    httpClientSet.add(http);
  }



  @Override
  public javax.net.ssl.HostnameVerifier getHostnameVerifier() {
    return hostnameVerifier;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if(!getOutputStreamFailed) {
      return super.getInputStream();
    }
    return null;
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

  /*
   * Called by layered delegator's finalize() method to handle closing
   * the underlying object.
   */
  protected void finalize() throws Throwable {
    super.finalize();
  }
}
