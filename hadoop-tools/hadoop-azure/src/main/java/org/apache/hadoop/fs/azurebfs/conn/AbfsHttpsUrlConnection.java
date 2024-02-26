package org.apache.hadoop.fs.azurebfs.conn;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import sun.net.ProgressSource;
import sun.net.www.MessageHeader;
import sun.net.www.http.HttpClient;
import sun.net.www.http.KeepAliveCache;
import sun.net.www.http.PosterOutputStream;
import sun.net.www.protocol.http.HttpURLConnection;
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

  public static class AbfsHttpClient extends HttpClient {

    private HttpClient httpClient;

    String uuid = UUID.randomUUID().toString();

    @Override
    public int hashCode() {
      return uuid.hashCode();
    }

    public static void removeAllFromKac() {
      kac = new KeepAliveCache();
    }

    public AbfsHttpClient(HttpClient client) {
      httpClient = client;
    }
    public final static Stack<Integer> finishedStack = new Stack<>();


    @Override
    public boolean getHttpKeepAliveSet() {
      return httpClient.getHttpKeepAliveSet();
    }


    @Override
    public void closeIdleConnection() {
      httpClient.closeIdleConnection();
    }

    @Override
    public void openServer(String s, int i) throws IOException {
      httpClient.openServer(s, i);
    }

    @Override
    public boolean needsTunneling() {
      return httpClient.needsTunneling();
    }

    @Override
    public synchronized boolean isCachedConnection() {
      return httpClient.isCachedConnection();
    }

    @Override
    public void afterConnect() throws IOException, UnknownHostException {
      httpClient.afterConnect();
    }

    @Override
    public String getURLFile() throws IOException {
      return httpClient.getURLFile();
    }

    @Override
    public void writeRequests(MessageHeader messageHeader) {
      httpClient.writeRequests(messageHeader);
    }

    @Override
    public void writeRequests(MessageHeader messageHeader, PosterOutputStream posterOutputStream) throws IOException {
      httpClient.writeRequests(messageHeader, posterOutputStream);
    }

    @Override
    public void writeRequests(MessageHeader messageHeader, PosterOutputStream posterOutputStream, boolean b) throws IOException {
      httpClient.writeRequests(messageHeader, posterOutputStream, b);
    }

    @Override
    public boolean parseHTTP(MessageHeader messageHeader, ProgressSource progressSource, HttpURLConnection httpURLConnection) throws IOException {
      return httpClient.parseHTTP(messageHeader, progressSource, httpURLConnection);
    }

    @Override
    public synchronized InputStream getInputStream() {
      return httpClient.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() {
      return httpClient.getOutputStream();
    }

    @Override
    public String toString() {
      return httpClient.toString();
    }

    @Override
    public void setCacheRequest(CacheRequest cacheRequest) {
      httpClient.setCacheRequest(cacheRequest);
    }

    @Override
    public void setDoNotRetry(boolean b) {
      httpClient.setDoNotRetry(b);
    }

    @Override
    public void setIgnoreContinue(boolean b) {
      httpClient.setIgnoreContinue(b);
    }

    @Override
    public void closeServer() {
      httpClient.closeServer();
      clients.remove(this);
    }

    @Override
    public String getProxyHostUsed() {
      return httpClient.getProxyHostUsed();
    }

    @Override
    public int getProxyPortUsed() {
      return httpClient.getProxyPortUsed();
    }


    @Override
    public boolean serverIsOpen() {
      return httpClient.serverIsOpen();
    }

    @Override
    public void setConnectTimeout(int i) {
      httpClient.setConnectTimeout(i);
    }

    @Override
    public int getConnectTimeout() {
      return httpClient.getConnectTimeout();
    }

    @Override
    public void setReadTimeout(int i) {
      httpClient.setReadTimeout(i);
    }

    @Override
    public int getReadTimeout() {
      return httpClient.getReadTimeout();
    }

    @Override
    public void finished() {
      httpClient.finished();
      //finishedStack.push(1);
    }
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
    http = new AbfsHttpClient(http);
    if(!isFromCache) {
        clients.add((AbfsHttpClient) http);
    }
//    if(!httpClientSet.contains(http)) {
//      isFromCache = false;
//    }
//    httpClientSet.add(http);
  }

  public static void removeAll() {
    for(AbfsHttpClient client : clients) {
      client.closeServer();
    }
    AbfsHttpClient.removeAllFromKac();
  }

  public static final Set<AbfsHttpClient> clients = new HashSet<>();




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
