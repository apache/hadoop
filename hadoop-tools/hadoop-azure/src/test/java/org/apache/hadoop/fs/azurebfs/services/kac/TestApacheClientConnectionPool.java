package org.apache.hadoop.fs.azurebfs.services.kac;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.services.kac.KeepAliveCache;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.DEFAULT_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.KAC_CONN_TTL;

public class TestApacheClientConnectionPool extends
    AbstractAbfsTestWithTimeout {

  public TestApacheClientConnectionPool() throws Exception {
    super();
  }

  @Test
  public void testBasicPool() throws IOException {
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    validatePoolSize(DEFAULT_MAX_CONN_SYS_PROP);
  }

  @Test
  public void testSysPropAppliedPool() throws IOException {
    final String customPoolSize = "10";
    System.setProperty(HTTP_MAX_CONN_SYS_PROP, customPoolSize);
    validatePoolSize(Integer.parseInt(customPoolSize));
  }

  private void validatePoolSize(int size) throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    final HttpClientConnection[] connections = new HttpClientConnection[size * 2];

    for (int i = 0; i < size * 2; i++) {
      connections[i] = Mockito.mock(HttpClientConnection.class);
    }

    for (int i = 0; i < size * 2; i++) {
      keepAliveCache.put(routes, connections[i]);
    }

    for (int i = size; i < size * 2; i++) {
      Mockito.verify(connections[i], Mockito.times(1)).close();
    }

    for (int i = 0; i < size * 2; i++) {
      if (i < size) {
        Assert.assertNotNull(keepAliveCache.get(routes));
      } else {
        Assert.assertNull(keepAliveCache.get(routes));
      }
    }
    System.clearProperty(HTTP_MAX_CONN_SYS_PROP);
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCache() throws IOException {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);

    keepAliveCache.put(routes, connection);

    Assert.assertNotNull(keepAliveCache.get(routes));
    keepAliveCache.put(routes, connection);

    final HttpRoute routes1 = new HttpRoute(new HttpHost("localhost1"));
    Assert.assertNull(keepAliveCache.get(routes1));
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCacheCleanup() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * KAC_CONN_TTL);
    Mockito.verify(connection, Mockito.times(1)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
    keepAliveCache.close();
  }

  @Test
  public void testKeepAliveCacheCleanupWithConnections() throws Exception {
    KeepAliveCache keepAliveCache = KeepAliveCache.INSTANCE;
    keepAliveCache.pauseThread();
    final HttpRoute routes = new HttpRoute(new HttpHost("localhost"));
    HttpClientConnection connection = Mockito.mock(HttpClientConnection.class);
    keepAliveCache.put(routes, connection);

    Thread.sleep(2 * KAC_CONN_TTL);
    Mockito.verify(connection, Mockito.times(0)).close();
    Assert.assertNull(keepAliveCache.get(routes));
    Mockito.verify(connection, Mockito.times(1)).close();
    keepAliveCache.close();
  }



  private AzureBlobFileSystem getAbfsFs(final Configuration configuration)
      throws IOException {
    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.newInstance(configuration);
    return fs;
  }
}

