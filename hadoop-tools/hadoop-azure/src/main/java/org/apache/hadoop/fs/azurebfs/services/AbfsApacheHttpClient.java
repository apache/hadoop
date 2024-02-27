package org.apache.hadoop.fs.azurebfs.services;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsApacheHttpExpect100Exception;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
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
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultClientConnectionReuseStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestExecutor;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.EXPECT;


public class AbfsApacheHttpClient {
  public static final Stack<Integer> connectionReuseCount = new Stack<>();
  public static final Stack<Integer> kacSizeStack = new Stack<>();

  public void close() throws IOException {
    if(httpClient != null) {
      httpClient.close();
    }
//    if(connMgr != null) {
//      connMgr.close();
//    }
  }


  public static class AbfsHttpClientContext extends HttpClientContext {
    Long connectTime;
    long readTime;
    long sendTime;
    HttpClientConnection httpClientConnection;
    long expect100HeaderSendTime = 0L;
    long expect100ResponseTime;

    long keepAliveTime;

    Boolean isBeingRead = false;
    final Boolean isReadable;
    final AbfsRestOperationType abfsRestOperationType;

    public AbfsHttpClientContext(Boolean isReadable, AbfsRestOperationType operationType) {
      this.isReadable = isReadable;
      this.abfsRestOperationType = operationType;
    }

    public boolean shouldKillConn(final AbfsConnMgr connMgr) {
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
        if(abfsApacheHttpConnection != null) {
          connectionReuseCount.push(abfsApacheHttpConnection.count);
        }
        if (abfsApacheHttpConnection != null && connMgr.abfsConfiguration.isReuseLimitApplied() &&
            abfsApacheHttpConnection.getCount() >= 5) {
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

    AbfsConnMgr connMgr;

    public void setConnMgr(AbfsConnMgr connMgr) {
      this.connMgr = connMgr;
    }


    @Override
    public ManagedHttpClientConnection create(final HttpRoute route,
        final ConnectionConfig config) {
      return new AbfsApacheHttpConnection(super.create(route, config), connMgr);
    }
  }

  public static final Map<String, AbfsApacheHttpConnection> abfsApacheHttpConnectionMap
      = new HashMap<String, AbfsApacheHttpConnection>();

  public static class AbfsApacheHttpConnection implements ManagedHttpClientConnection {

    private ManagedHttpClientConnection httpClientConnection;

    private final AbfsConnMgr connMgr;

    private AbfsHttpClientContext abfsHttpClientContext;

    int count = 0;
    public boolean cached = false;
    final String uuid = UUID.randomUUID().toString();

    private boolean isClosed = false;

    public int getCount() {
      return count;
    }

    public AbfsApacheHttpConnection(ManagedHttpClientConnection clientConnection,
        final AbfsConnMgr connMgr) {
      this.httpClientConnection = clientConnection;
      abfsApacheHttpConnectionMap.put(getId(), this);
      this.connMgr = connMgr;
    }

    public void setAbfsHttpClientContext(AbfsHttpClientContext abfsHttpClientContext) {
      this.abfsHttpClientContext = abfsHttpClientContext;
    }

    public void removeAbfsHttpClientContext() {
      this.abfsHttpClientContext = null;
    }

    @Override
    public void close() throws IOException {
      boolean wasClosed = isClosed;
      httpClientConnection.close();
      isClosed = true;
      if(wasClosed) {
        return;
      }
//      connMgr.connCount.decrementAndGet();
      if(cached) {
        synchronized (connMgr.kacCount) {
          connMgr.kacCount.decrementAndGet();
        }
        abfsApacheHttpConnectionMap.remove(getId());
      }
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
//      Long start = System.currentTimeMillis();
      boolean val = httpClientConnection.isResponseAvailable(timeout);
//      if(abfsHttpClientContext != null) {
//        abfsHttpClientContext.expect100ResponseTime += (System.currentTimeMillis() - start);
//      }
      return val;
    }

    @Override
    public void sendRequestHeader(final HttpRequest request)
        throws HttpException, IOException {
      count++;
//      long start = System.currentTimeMillis();
      httpClientConnection.sendRequestHeader(request);
//      long elapsed = System.currentTimeMillis() - start;
      if(request instanceof HttpEntityEnclosingRequest && ((HttpEntityEnclosingRequest) request).expectContinue()) {
        if(abfsHttpClientContext != null && abfsHttpClientContext.expect100HeaderSendTime == 0L) {
//          abfsHttpClientContext.expect100HeaderSendTime = elapsed;
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
//      long start = System.currentTimeMillis();
      HttpResponse response = httpClientConnection.receiveResponseHeader();
//      if(abfsHttpClientContext != null) {
//        abfsHttpClientContext.expect100ResponseTime += System.currentTimeMillis() - start;
//      }
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

  public static class IntegerWrapper {
    int val;
    public IntegerWrapper(int val) {
      this.val = val;
    }

    public synchronized int incrementAndGet() {
      val++;
      return val;
    }

    public synchronized int decrementAndGet() {
      val--;
      return val;
    }

    public synchronized int get() {
      return val;
    }
  }

  public static class AbfsConnMgr extends PoolingHttpClientConnectionManager {

    /**
     * Gives count of connections that have been cached. Increment when adding in the KAC.
     * Decrement when connection is taken from KAC, or connection from KAC is getting closed.
     */
    private final IntegerWrapper kacCount = new IntegerWrapper(0);

    /**
     * Gives the number of connections at a moment. Increased when a new connection
     * is opened. Decreased when a connection is closed.
     */
//    private final AtomicInteger connCount = new AtomicInteger(0);
//    private final AtomicInteger inTransits = new AtomicInteger(0);

    private int maxConn;

    public AbfsConfiguration abfsConfiguration;

    public void checkAvailablity() {
      PoolStats poolStats = getTotalStats();

      if(poolStats.getMax() <= (poolStats.getLeased() + poolStats.getPending() + 1)) {
        maxConn *= 2;
        setDefaultMaxPerRoute(maxConn);
        setMaxTotal(maxConn);
      }
    }

    public AbfsConnMgr(ConnectionSocketFactory connectionSocketFactory, AbfsConfiguration abfsConfiguration) {
      super(createSocketFactoryRegistry(connectionSocketFactory), abfsConnFactory);
      abfsConnFactory.setConnMgr(this);
      maxConn = abfsConfiguration.getHttpClientMaxConn();
//      maxConn = 1;
      setDefaultMaxPerRoute(maxConn);
      setMaxTotal(maxConn);
      this.abfsConfiguration = abfsConfiguration;
    }

//    @Override
//    public ConnectionRequest requestConnection(final HttpRoute route,
//        final Object state) {
//      checkAvailablity();
//      return super.requestConnection(route, state);
//    }

    public void closeAllConn() {
      enumAvailable((entry)-> {
        entry.close();
      });
    }

    @Override
    public void connect(final HttpClientConnection managedConn,
        final HttpRoute route,
        final int connectTimeout,
        final HttpContext context) throws IOException {
      long start = System.currentTimeMillis();
      super.connect(managedConn, route, connectTimeout, context);
//      connCount.incrementAndGet();
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
//      inTransits.incrementAndGet();
      try {
        HttpClientConnection connection = super.leaseConnection(future, timeout, timeUnit);
        if(connection instanceof ManagedHttpClientConnection) {
          AbfsApacheHttpConnection abfsApacheHttpConnection = abfsApacheHttpConnectionMap.get(((ManagedHttpClientConnection) connection).getId());
          if(abfsApacheHttpConnection != null && abfsApacheHttpConnection.cached) {
            synchronized (kacCount) {
              kacCount.decrementAndGet();
            }
            abfsApacheHttpConnection.cached = false;
          }
        }
        return connection;
      } catch (InterruptedException | ExecutionException | ConnectionPoolTimeoutException e) {
//        inTransits.decrementAndGet();
        throw e;
      }
    }

    @Override
    public void releaseConnection(final HttpClientConnection managedConn,
        final Object state,
        final long keepalive,
        final TimeUnit timeUnit) {
      if(AbfsAHCHttpOperation.connThatCantBeClosed.contains(managedConn)) {
        return;
      }
      final AbfsApacheHttpConnection abfsApacheHttpConnection;
      if(managedConn instanceof ManagedHttpClientConnection) {
        abfsApacheHttpConnection = abfsApacheHttpConnectionMap.get(((ManagedHttpClientConnection)managedConn).getId());
      } else {
        abfsApacheHttpConnection = null;
      }
//      if(keepalive == 0 && managedConn instanceof ManagedHttpClientConnection) {
//        abfsApacheHttpConnectionMap.remove(((ManagedHttpClientConnection)managedConn).getId());
//      }
//      inTransits.decrementAndGet();
      if(keepalive == 0) {
        super.releaseConnection(managedConn, state, keepalive, timeUnit);
        return;
      }
      boolean toBeCached = true;
      synchronized (kacCount) {
        int kacSize = kacCount.incrementAndGet();
        if(kacSize >5) {
          kacCount.decrementAndGet();
          toBeCached = false;
        }
      }
      if(toBeCached) {
        if(abfsApacheHttpConnection != null) {
          abfsApacheHttpConnection.cached = true;
        }
        super.releaseConnection(managedConn, state, keepalive, timeUnit);
      } else {
        synchronized (managedConn) {
          /*
          * Taken from ConnectionHolder.releaseConnection();
          */
          try {
            managedConn.close();
          } catch (IOException ignored) {

          }
          super.releaseConnection(managedConn, null, 0, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  public static class AhcConnReuseStrategy extends
      DefaultClientConnectionReuseStrategy {

    AbfsConnMgr connMgr;

    public AhcConnReuseStrategy(final AbfsConnMgr connMgr) {
      this.connMgr = connMgr;
    }

    @Override
    public boolean keepAlive(final HttpResponse response,
        final HttpContext context) {
//      response.
      if (context instanceof AbfsHttpClientContext) {
//        if (((AbfsHttpClientContext) context).shouldKillConn(connMgr)) {
//          return false;
//        }
        return super.keepAlive(response, context);
      }
      return super.keepAlive(response, context);
    }
  }

  private static class AbfsHttpRequestExecutor extends HttpRequestExecutor {

    @Override
    protected HttpResponse doSendRequest(final HttpRequest request,
        final HttpClientConnection conn,
        final HttpContext context) throws IOException, HttpException {
//      long start = System.currentTimeMillis();
      final HttpResponse res = super.doSendRequest(request, conn, context);
//      long elapsed = System.currentTimeMillis() - start;
      if(context instanceof AbfsHttpClientContext) {
        ((AbfsHttpClientContext) context).httpClientConnection = conn;
//        ((AbfsHttpClientContext) context).sendTime = elapsed;
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

  final CloseableHttpClient httpClient;

  private final AbfsConnectionManager connMgr;

  public AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final AbfsConfiguration abfsConfiguration) {
//    if(delegatingSSLSocketFactory == null) {
//      connMgr = new AbfsConnMgr(null, abfsConfiguration);
//    } else {
//      connMgr = new AbfsConnMgr(
//          new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null), abfsConfiguration);
//    }
    connMgr = new AbfsConnectionManager(createSocketFactoryRegistry(new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null)), new org.apache.hadoop.fs.azurebfs.services.AbfsConnFactory());
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsHttpRequestExecutor())
//        .setConnectionReuseStrategy(new AhcConnReuseStrategy(connMgr))
//        .setKeepAliveStrategy(new AbfsKeepAliveStrategy(abfsConfiguration))
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
//        .evictIdleConnections(5000L, TimeUnit.MILLISECONDS)
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
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(30_000)
        .setSocketTimeout(30_000);
    httpRequest.setConfig(requestConfigBuilder.build());
    return httpClient.execute(httpRequest, abfsHttpClientContext);
  }

  public int getParallelConnAtMoment() {
    return 0;
//    return connMgr.connCount.get();
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

  public void closeAllConn() {
    connMgr.closeAllConn();
  }
}
