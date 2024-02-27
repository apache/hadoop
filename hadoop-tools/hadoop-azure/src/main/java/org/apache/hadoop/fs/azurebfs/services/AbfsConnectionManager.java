package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azurebfs.services.kac.KeepAliveCache;
import org.apache.http.HttpClientConnection;
import org.apache.http.config.Registry;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.HttpClientConnectionOperator;
import org.apache.http.conn.HttpConnectionFactory;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.DefaultHttpClientConnectionOperator;
import org.apache.http.protocol.HttpContext;

public class AbfsConnectionManager implements HttpClientConnectionManager {

  KeepAliveCache kac = KeepAliveCache.INSTANCE;

  private final Registry<ConnectionSocketFactory> socketFactoryRegistry;
  private final HttpConnectionFactory<HttpRoute, HttpClientConnection> httpConnectionFactory;

  private final HttpClientConnectionOperator connectionOperator;


  private final Map<HttpClientConnection, HttpRoute> map = new HashMap<>();

  public AbfsConnectionManager(Registry<ConnectionSocketFactory> socketFactoryRegistry,
      HttpConnectionFactory connectionFactory) {
    this.socketFactoryRegistry = socketFactoryRegistry;
    this.httpConnectionFactory = connectionFactory;
    connectionOperator = new DefaultHttpClientConnectionOperator(socketFactoryRegistry, null, null);
  }

  @Override
  public ConnectionRequest requestConnection(final HttpRoute route,
      final Object state) {
    return new ConnectionRequest() {
      @Override
      public HttpClientConnection get(final long timeout,
          final TimeUnit timeUnit)
          throws InterruptedException, ExecutionException,
          ConnectionPoolTimeoutException {
        try {
          HttpClientConnection client = kac.get(route);
          if(client != null && client.isOpen()) {
             return  client;
          }
          return httpConnectionFactory.create(route, null);
        } catch (IOException ex) {
          throw new ExecutionException(ex);
        }
      }

      @Override
      public boolean cancel() {
        return false;
      }
    };
  }

  @Override
  public void releaseConnection(final HttpClientConnection conn,
      final Object newState,
      final long validDuration,
      final TimeUnit timeUnit) {
    if(conn.isOpen() && conn instanceof AbfsConnFactory.AbfsApacheHttpConnection) {
      HttpRoute route = ((AbfsConnFactory.AbfsApacheHttpConnection) conn).httpRoute;
      if(route != null) {
        kac.put(route, conn);
      }
    }
  }

  @Override
  public void connect(final HttpClientConnection conn,
      final HttpRoute route,
      final int connectTimeout,
      final HttpContext context) throws IOException {
    long start = System.currentTimeMillis();
    connectionOperator.connect((ManagedHttpClientConnection) conn, route.getTargetHost(), route.getLocalSocketAddress(),
        connectTimeout, SocketConfig.DEFAULT, context);
    if(context instanceof AbfsApacheHttpClient.AbfsHttpClientContext) {
      ((AbfsApacheHttpClient.AbfsHttpClientContext)context).connectTime = (System.currentTimeMillis() - start);
    }
  }

  @Override
  public void upgrade(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {

  }

  @Override
  public void routeComplete(final HttpClientConnection conn,
      final HttpRoute route,
      final HttpContext context) throws IOException {

  }

  @Override
  public void closeIdleConnections(final long idletime,
      final TimeUnit timeUnit) {

  }

  @Override
  public void closeExpiredConnections() {

  }

  @Override
  public void shutdown() {

  }

  public void closeAllConn() {
    KeepAliveCache.restart();

  }
}
