package org.apache.hadoop.fs.azurebfs.services;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
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
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultClientConnectionReuseStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;


public class AbfsApacheHttpClient {

  public static class AbfsHttpClientContext extends HttpClientContext {
    public Long connectTime;
    public Long readTime;
    public Boolean expect100;
  }

  public static class AbfsConnFactory extends ManagedHttpClientConnectionFactory {

    @Override
    public ManagedHttpClientConnection create(final HttpRoute route,
        final ConnectionConfig config) {
      return new AbfsApacheHttpConnection(super.create(route, config));
    }
  }

  public static class AbfsApacheHttpConnection implements ManagedHttpClientConnection {

    private ManagedHttpClientConnection httpClientConnection;

    public AbfsApacheHttpConnection(ManagedHttpClientConnection clientConnection) {
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
      Long start = System.currentTimeMillis();
      httpClientConnection.sendRequestHeader(request);
      System.out.println("BLA BLA" + (System.currentTimeMillis() - start));
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

    @Override
    public String getId() {
      return httpClientConnection.getId();
    }

    @Override
    public void bind(final Socket socket) throws IOException {
      httpClientConnection.bind(socket);
    }

    @Override
    public Socket getSocket() {
      return httpClientConnection.getSocket();
    }

    @Override
    public SSLSession getSSLSession() {
      return httpClientConnection.getSSLSession();
    }

    @Override
    public InetAddress getLocalAddress() {
      return httpClientConnection.getLocalAddress();
    }

    @Override
    public int getLocalPort() {
      return httpClientConnection.getLocalPort();
    }

    @Override
    public InetAddress getRemoteAddress() {
      return httpClientConnection.getRemoteAddress();
    }

    @Override
    public int getRemotePort() {
      return httpClientConnection.getRemotePort();
    }
  }

  private static AbfsConnFactory abfsConnFactory = new AbfsConnFactory();

//  public static class AbfsConnRequest implements ConnectionRequest {
//
//    private ConnectionRequest connectionRequest;
//
//    public AbfsConnRequest(ConnectionRequest connectionRequest) {
//      this.connectionRequest = connectionRequest;
//    }
//
//    @Override
//    public HttpClientConnection get(final long timeout, final TimeUnit timeUnit)
//        throws InterruptedException, ExecutionException,
//        ConnectionPoolTimeoutException {
//      HttpClientConnection clientConnection = new AbfsApacheHttpConnection(connectionRequest.get(timeout, timeUnit));
//      return clientConnection;
//    }
//
//    @Override
//    public boolean cancel() {
//      return connectionRequest.cancel();
//    }
//  }

  private static class AbfsConnMgr extends PoolingHttpClientConnectionManager {

    public AbfsConnMgr(ConnectionSocketFactory connectionSocketFactory) {
      super(createSocketFactoryRegistry(connectionSocketFactory), abfsConnFactory);
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

  public static class AhcConnReuseStrategy extends
      DefaultClientConnectionReuseStrategy {

    @Override
    public boolean keepAlive(final HttpResponse response,
        final HttpContext context) {
//      response.
      return super.keepAlive(response, context);
    }
  }

  private final ConnectionReuseStrategy connectionReuseStrategy  = new AhcConnReuseStrategy();

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
      if(request != null && request.containsHeader(EXPECT) && res != null && res.getStatusLine().getStatusCode() != 200) {
        throw new AbfsApacheHttpExpect100Exception("Server rejected operation", res);
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
    final AbfsConnMgr connMgr;
    if(delegatingSSLSocketFactory == null) {
      connMgr = new AbfsConnMgr(null);
    } else {
      connMgr = new AbfsConnMgr(
          new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null));
    }
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

  public HttpResponse execute(HttpRequestBase httpRequest) throws IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
//        .setConnectionRequestTimeout(20_000)
        .setConnectTimeout(30_000)
        .setSocketTimeout(30_000);
    httpRequest.setConfig(requestConfigBuilder.build());
    return httpClient.execute(httpRequest, new AbfsHttpClientContext());
  }


  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(ConnectionSocketFactory sslSocketFactory) {
    if(sslSocketFactory == null) {
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("http", PlainConnectionSocketFactory.getSocketFactory())
          .build();
    }
    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register("http", PlainConnectionSocketFactory.getSocketFactory())
        .register("https", sslSocketFactory)
        .build();
  }
}
