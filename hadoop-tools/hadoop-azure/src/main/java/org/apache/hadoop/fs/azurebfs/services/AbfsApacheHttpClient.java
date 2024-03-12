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

  private final AbfsConfiguration abfsConfiguration;

  public AbfsApacheHttpClient(DelegatingSSLSocketFactory delegatingSSLSocketFactory,
      final AbfsConfiguration abfsConfiguration) {
    this.abfsConfiguration = abfsConfiguration;
    connMgr = new AbfsConnectionManager(createSocketFactoryRegistry(
        new SSLConnectionSocketFactory(delegatingSSLSocketFactory, null)),
        new org.apache.hadoop.fs.azurebfs.services.AbfsConnFactory());
    final HttpClientBuilder builder = HttpClients.custom();
    builder.setConnectionManager(connMgr)
        .setRequestExecutor(new AbfsHttpRequestExecutor())
        .disableContentCompression()
        .disableRedirectHandling()
        .disableAutomaticRetries()
        .setUserAgent(
            ""); // SDK will set the user agent header in the pipeline. Don't let Apache waste time
    httpClient = builder.build();
  }

  public HttpResponse execute(HttpRequestBase httpRequest, final AbfsHttpClientContext abfsHttpClientContext) throws IOException {
    RequestConfig.Builder requestConfigBuilder = RequestConfig
        .custom()
        .setConnectTimeout(abfsConfiguration.getHttpConnectionTimeout())
        .setSocketTimeout(abfsConfiguration.getHttpReadTimeout());
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
