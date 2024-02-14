package org.apache.hadoop.fs.azurebfs.services;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
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
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.DefaultClientConnectionReuseStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;


public class AbfsApacheHttpClient {

  public static class AbfsHttpClientContext extends HttpClientContext {
    Long connectTime;
    Long readTime;
    Long sendTime;
    HttpClientConnection httpClientConnection;
    Long expect100HeaderSendTime = 0L;
    Long expect100ResponseTime;

    long keepAliveTime;

    Boolean isBeingRead = false;
    final Boolean isReadable;
    final AbfsRestOperationType abfsRestOperationType;

    public AbfsHttpClientContext(Boolean isReadable, AbfsRestOperationType operationType) {
      this.isReadable = isReadable;
      this.abfsRestOperationType = operationType;
    }

    public boolean shouldKillConn() {
//      if(sendTime != null && sendTime > MetricPercentile.getSendPercentileVal(abfsRestOperationType, 99.9)) {
//        return true;
//      }
//      if(readTime != null && readTime > MetricPercentile.getRcvPercentileVal(abfsRestOperationType, 99.9)) {
//        return true;
//      }
      if (httpClientConnection instanceof ManagedHttpClientConnection) {
        AbfsApacheHttpConnection abfsApacheHttpConnection
            = abfsApacheHttpConnectionMap.get(
            ((ManagedHttpClientConnection) httpClientConnection).getId());
        if (abfsApacheHttpConnection != null && abfsApacheHttpConnection.getCount() >= 5) {
          return true;
        }
      }
      return false;
    }
  }

  public static class AbfsKeepAliveStrategy implements ConnectionKeepAliveStrategy {
    private final long keepIdleTime;

    public AbfsKeepAliveStrategy(AbfsConfiguration abfsConfiguration) {
      keepIdleTime = abfsConfiguration.getHttpClientConnMaxIdleTime();
    }

    @Override
    public long getKeepAliveDuration(final HttpResponse response,
        final HttpContext context) {
      // If there's a Keep-Alive timeout directive in the response and it's
      // shorter than our configured max, honor that. Otherwise go with the
      // configured maximum.

      long duration = DefaultConnectionKeepAliveStrategy.INSTANCE
          .getKeepAliveDuration(response, context);

      if (0 < duration && duration < keepIdleTime) {
        if(context instanceof AbfsHttpClientContext) {
          ((AbfsHttpClientContext) context).keepAliveTime = duration;
        }
        return duration;
      }
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).keepAliveTime = keepIdleTime;
      }
      return keepIdleTime;
    }
  }

  public static class AbfsConnFactory extends ManagedHttpClientConnectionFactory {

    @Override
    public ManagedHttpClientConnection create(final HttpRoute route,
        final ConnectionConfig config) {
      return new AbfsApacheHttpConnection(super.create(route, config));
    }
  }

  public static final Map<String, AbfsApacheHttpConnection> abfsApacheHttpConnectionMap
      = new HashMap<String, AbfsApacheHttpConnection>();

  public static class AbfsApacheHttpConnection implements ManagedHttpClientConnection {

    private ManagedHttpClientConnection httpClientConnection;

    private AbfsHttpClientContext abfsHttpClientContext;

    int count = 0;
    final String uuid = UUID.randomUUID().toString();

    public int getCount() {
      return count;
    }

    public AbfsApacheHttpConnection(ManagedHttpClientConnection clientConnection) {
      this.httpClientConnection = clientConnection;
      abfsApacheHttpConnectionMap.put(getId(), this);
    }

    public void setAbfsHttpClientContext(AbfsHttpClientContext abfsHttpClientContext) {
      this.abfsHttpClientContext = abfsHttpClientContext;
    }

    public void removeAbfsHttpClientContext() {
      this.abfsHttpClientContext = null;
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
      Long start = System.currentTimeMillis();
      boolean val = httpClientConnection.isResponseAvailable(timeout);
      if(abfsHttpClientContext != null) {
        abfsHttpClientContext.expect100ResponseTime += (System.currentTimeMillis() - start);
      }
      return val;
    }

    @Override
    public void sendRequestHeader(final HttpRequest request)
        throws HttpException, IOException {
      count++;
      long start = System.currentTimeMillis();
      httpClientConnection.sendRequestHeader(request);
      long elapsed = System.currentTimeMillis() - start;
      if(request instanceof HttpEntityEnclosingRequest && ((HttpEntityEnclosingRequest) request).expectContinue()) {
        if(abfsHttpClientContext != null && abfsHttpClientContext.expect100HeaderSendTime == 0L) {
          abfsHttpClientContext.expect100HeaderSendTime = elapsed;
        }
      }
    }

    @Override
    public void sendRequestEntity(final HttpEntityEnclosingRequest request)
        throws HttpException, IOException {
      httpClientConnection.sendRequestEntity(request);
    }

    @Override
    public HttpResponse receiveResponseHeader()
        throws HttpException, IOException {
      long start = System.currentTimeMillis();
      HttpResponse response = httpClientConnection.receiveResponseHeader();
      if(abfsHttpClientContext != null) {
        abfsHttpClientContext.expect100ResponseTime += System.currentTimeMillis() - start;
      }
      return response;
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

    private final AtomicInteger connCount = new AtomicInteger(0);
    private final AtomicInteger inTransits = new AtomicInteger(0);

    private int maxConn;

    public synchronized void checkAvailablity() {
      if(maxConn <= inTransits.get()) {
        maxConn *= 2;
        setDefaultMaxPerRoute(maxConn);
        setMaxTotal(maxConn);
      }
    }

    public AbfsConnMgr(ConnectionSocketFactory connectionSocketFactory, AbfsConfiguration abfsConfiguration) {
      super(createSocketFactoryRegistry(connectionSocketFactory), abfsConnFactory);
//      maxConn = abfsConfiguration.getHttpClientMaxConn();
      maxConn = 1;
      setDefaultMaxPerRoute(maxConn);
      setMaxTotal(maxConn);
    }
    @Override
    public void connect(final HttpClientConnection managedConn,
        final HttpRoute route,
        final int connectTimeout,
        final HttpContext context) throws IOException {
      long start = System.currentTimeMillis();
      super.connect(managedConn, route, connectTimeout, context);
      connCount.incrementAndGet();
      long timeElapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).connectTime = timeElapsed;
      }
    }

    @Override
    protected HttpClientConnection leaseConnection(final Future future,
        final long timeout,
        final TimeUnit timeUnit)
        throws InterruptedException, ExecutionException,
        ConnectionPoolTimeoutException {
      inTransits.incrementAndGet();
      return super.leaseConnection(future, timeout, timeUnit);
    }

    @Override
    public void releaseConnection(final HttpClientConnection managedConn,
        final Object state,
        final long keepalive,
        final TimeUnit timeUnit) {
      if(AbfsAHCHttpOperation.connThatCantBeClosed.contains(managedConn)) {
        return;
      }
      if(keepalive == 0 && managedConn instanceof ManagedHttpClientConnection) {
        abfsApacheHttpConnectionMap.remove(((ManagedHttpClientConnection)managedConn).getId());
      }
      super.releaseConnection(managedConn, state, keepalive, timeUnit);
      inTransits.decrementAndGet();
      if(keepalive == 0) {
        connCount.decrementAndGet();
      }
    }
  }

  public static class AhcConnReuseStrategy extends
      DefaultClientConnectionReuseStrategy {

    @Override
    public boolean keepAlive(final HttpResponse response,
        final HttpContext context) {
//      response.
      if(context instanceof AbfsHttpClientContext) {
        if(!((AbfsHttpClientContext) context).isReadable && ((AbfsHttpClientContext) context).shouldKillConn()) {
          return false;
        }
        return super.keepAlive(response, context);
      }
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
        ((AbfsHttpClientContext) context).httpClientConnection = conn;
        ((AbfsHttpClientContext) context).sendTime = elapsed;
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

  private final AbfsConnMgr connMgr;

  public AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final AbfsConfiguration abfsConfiguration) {
    if(delegatingSSLSocketFactory == null) {
      connMgr = new AbfsConnMgr(null, abfsConfiguration);
    } else {
      connMgr = new AbfsConnMgr(
          new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null), abfsConfiguration);
    }
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsHttpRequestExecutor())
        .setConnectionReuseStrategy(connectionReuseStrategy)
        .setKeepAliveStrategy(new AbfsKeepAliveStrategy(abfsConfiguration))
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        .setUserAgent(""); // SDK will set the user agent header in the pipeline. Don't let Apache waste time
    httpClient = builder.build();
  }

  public void releaseConn(HttpClientConnection clientConnection, AbfsHttpClientContext context) {
    if(clientConnection != null) {
      connMgr.releaseConnection(clientConnection, context.getUserToken(),
          context.keepAliveTime, TimeUnit.MILLISECONDS);
    }
  }

  public void destroyConn(HttpClientConnection httpClientConnection)
      throws IOException {
    if(httpClientConnection != null) {
      httpClientConnection.close();
      connMgr.releaseConnection(
          httpClientConnection, null, 0, TimeUnit.MILLISECONDS);
    }
  }

  public HttpResponse execute(HttpRequestBase httpRequest, final AbfsHttpClientContext abfsHttpClientContext) throws IOException {
    connMgr.checkAvailablity();
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(30_000)
        .setSocketTimeout(30_000);
    httpRequest.setConfig(requestConfigBuilder.build());
    return httpClient.execute(httpRequest, abfsHttpClientContext);
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
