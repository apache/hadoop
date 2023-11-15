package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

public class AbfsApacheHttpClient {

  public static class AbfsHttpClientContext extends HttpClientContext {
    public Long connectTime;
    public Long readTime;
  }

  public static class AbfsApacheHttpConnection implements HttpClientConnection {

    private HttpClientConnection httpClientConnection;

    public AbfsApacheHttpConnection(HttpClientConnection clientConnection) {
      this.httpClientConnection = clientConnection;
    }

    @Override
    public void close() throws IOException {
      httpClientConnection.close();
    }

    @Override
    public boolean isOpen() {
      return httpClientConnection.isOpen();
    }

    @Override
    public boolean isStale() {
      return httpClientConnection.isStale();
    }

    @Override
    public void setSocketTimeout(final int timeout) {
      httpClientConnection.setSocketTimeout(timeout);
    }

    @Override
    public int getSocketTimeout() {
      return httpClientConnection.getSocketTimeout();
    }

    @Override
    public void shutdown() throws IOException {
      httpClientConnection.shutdown();
    }

    @Override
    public HttpConnectionMetrics getMetrics() {
      return httpClientConnection.getMetrics();
    }

    @Override
    public boolean isResponseAvailable(final int timeout) throws IOException {
      return httpClientConnection.isResponseAvailable(timeout);
    }

    @Override
    public void sendRequestHeader(final HttpRequest request)
        throws HttpException, IOException {
      httpClientConnection.sendRequestHeader(request);
    }

    @Override
    public void sendRequestEntity(final HttpEntityEnclosingRequest request)
        throws HttpException, IOException {
      httpClientConnection.sendRequestEntity(request);
    }

    @Override
    public HttpResponse receiveResponseHeader()
        throws HttpException, IOException {
      return httpClientConnection.receiveResponseHeader();
    }

    @Override
    public void receiveResponseEntity(final HttpResponse response)
        throws HttpException, IOException {
      httpClientConnection.receiveResponseEntity(response);
    }

    @Override
    public void flush() throws IOException {
      httpClientConnection.flush();
    }
  }

  public static class AbfsConnRequest implements ConnectionRequest {

    private ConnectionRequest connectionRequest;

    public AbfsConnRequest(ConnectionRequest connectionRequest) {
      this.connectionRequest = connectionRequest;
    }

    @Override
    public HttpClientConnection get(final long timeout, final TimeUnit timeUnit)
        throws InterruptedException, ExecutionException,
        ConnectionPoolTimeoutException {
      HttpClientConnection clientConnection = new AbfsApacheHttpConnection(connectionRequest.get(timeout, timeUnit));
      return clientConnection;
    }

    @Override
    public boolean cancel() {
      return connectionRequest.cancel();
    }
  }

  private static class AbfsConnMgr extends PoolingHttpClientConnectionManager {

    @Override
    public ConnectionRequest requestConnection(final HttpRoute route,
        final Object state) {
      return new AbfsConnRequest(super.requestConnection(route, state));
    }

    public AbfsConnMgr(ConnectionSocketFactory connectionSocketFactory) {
      super(createSocketFactoryRegistry(connectionSocketFactory));
    }
    @Override
    public void connect(final HttpClientConnection managedConn,
        final HttpRoute route,
        final int connectTimeout,
        final HttpContext context) throws IOException {
      long start = System.currentTimeMillis();
      super.connect(managedConn, route, connectTimeout, context);
      long timeElapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).connectTime = timeElapsed;
      }
    }
  }

  private final ConnectionReuseStrategy connectionReuseStrategy = new ConnectionReuseStrategy() {
    @Override
    public boolean keepAlive(final HttpResponse response,
        final HttpContext context) {
      if(context instanceof AbfsHttpClientContext) {
        return ((AbfsHttpClientContext) context).readTime <= 100;
      }
      return true;
    }
  };

  private static class AbfsHttpRequestExecutor extends HttpRequestExecutor {

    @Override
    protected HttpResponse doSendRequest(final HttpRequest request,
        final HttpClientConnection conn,
        final HttpContext context) throws IOException, HttpException {
      long start = System.currentTimeMillis();
      final HttpResponse res = super.doSendRequest(request, conn, context);
      long elapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).readTime = elapsed;
      }
      return res;
    }

    @Override
    protected HttpResponse doReceiveResponse(final HttpRequest request,
        final HttpClientConnection conn,
        final HttpContext context) throws HttpException, IOException {
      long start = System.currentTimeMillis();
      final HttpResponse res = super.doReceiveResponse(request, conn, context);
      long elapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).readTime = elapsed;
      }
      return res;
    }
  }

  final HttpClient httpClient;

  public AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory) {
    final AbfsConnMgr connMgr = new AbfsConnMgr(new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null));
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsHttpRequestExecutor())
        .setConnectionReuseStrategy(connectionReuseStrategy)
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        .setUserAgent(""); // SDK will set the user agent header in the pipeline. Don't let Apache waste time
    httpClient = builder.build();
  }

  public void execute(final URL url, final String method, final List<AbfsHttpHeader> requestHeaders) throws Exception {
    HttpRequestBase httpRequest = new HttpGet(url.toURI());
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectionRequestTimeout(20)
        .setConnectTimeout(30_000)
        .setSocketTimeout(30_000);
    httpRequest.setConfig(requestConfigBuilder.build());
  }


  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(ConnectionSocketFactory sslSocketFactory) {
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https", sslSocketFactory)
        .build();
  }
}
