package org.apache.hadoop.fs.azurebfs.services;

import java.io.Closeable;
import java.io.IOException;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.http.HttpClientConnection;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_MAX_CONN_SYS_PROP;

/**
 * Connection-pooling heuristics used by {@link AbfsConnectionManager}. Each
 * instance of FileSystem has its own KeepAliveCache.
 * <p>
 * Why this implementation is required in comparison to {@link org.apache.http.impl.conn.PoolingHttpClientConnectionManager}
 * connection-pooling:
 * <ol>
 * <li>PoolingHttpClientConnectionManager heuristic caches all the reusable connections it has created.
 * JDK's implementation only caches limited number of connections. The limit is given by JVM system
 * property "http.maxConnections". If there is no system-property, it defaults to 5.</li>
 * <li>In PoolingHttpClientConnectionManager, it expects the application to provide `setMaxPerRoute` and `setMaxTotal`,
 * which the implementation uses as the total number of connections it can create. For application using ABFS, it is not
 * feasible to provide a value in the initialisation of the connectionManager. JDK's implementation has no cap on the
 * number of connections it can create.</li>
 * </ol>
 */
public class KeepAliveCache extends Stack<KeepAliveCache.KeepAliveEntry>
    implements
    Closeable {

  /**
   * Scheduled timer that evicts idle connections.
   */
  private final Timer timer;

  /**
   * Task provided to the timer that owns eviction logic.
   */
  private final TimerTask timerTask;

  /**
   * Flag to indicate if the cache is closed.
   */
  private boolean isClosed;

  /**
   * Counter to keep track of the number of KeepAliveCache instances created.
   */
  private static final AtomicInteger KAC_COUNTER = new AtomicInteger(0);

  /**
   * Maximum number of connections that can be cached.
   */
  private final int maxConn;

  /**
   * Time-to-live for an idle connection.
   */
  private final long connectionIdleTTL;

  /**
   * Flag to indicate if the eviction thread is paused.
   */
  private boolean isPaused = false;

  @VisibleForTesting
  synchronized void pauseThread() {
    isPaused = true;
  }

  @VisibleForTesting
  synchronized void resumeThread() {
    isPaused = false;
  }

  /**
   * @return connectionIdleTTL
   */
  @VisibleForTesting
  public long getConnectionIdleTTL() {
    return connectionIdleTTL;
  }

  public KeepAliveCache(AbfsConfiguration abfsConfiguration) {
    this.timer = new Timer(
        String.format("abfs-kac-" + KAC_COUNTER.getAndIncrement()), true);
    String sysPropMaxConn = System.getProperty(HTTP_MAX_CONN_SYS_PROP);
    if (sysPropMaxConn == null) {
      this.maxConn = abfsConfiguration.getMaxApacheHttpClientCacheConnections();
    } else {
      maxConn = Integer.parseInt(sysPropMaxConn);
    }

    this.connectionIdleTTL
        = abfsConfiguration.getMaxApacheHttpClientConnectionIdleTime();
    this.timerTask = new TimerTask() {
      @Override
      public void run() {
          if (isPaused) {
            return;
          }
          evictIdleConnection();
      }
    };
    timer.schedule(timerTask, 0, connectionIdleTTL);
  }

  /**
   * Iterate over the cache and evict the idle connections. An idle connection is
   * one that has been in the cache for more than connectionIdleTTL milliseconds.
   */
  synchronized void evictIdleConnection() {
    long currentTime = System.currentTimeMillis();
    int i;
    for (i = 0; i < size(); i++) {
      KeepAliveEntry e = elementAt(i);
      if ((currentTime - e.idleStartTime) > connectionIdleTTL
          || e.httpClientConnection.isStale()) {
        HttpClientConnection hc = e.httpClientConnection;
        closeHtpClientConnection(hc);
      } else {
        break;
      }
    }
    subList(0, i).clear();
  }

  /**
   * Safe close of the HttpClientConnection.
   *
   * @param hc HttpClientConnection to be closed
   */
  private void closeHtpClientConnection(final HttpClientConnection hc) {
    try {
      hc.close();
    } catch (IOException ignored) {

    }
  }

  /**
   * Close all connections in cache and cancel the eviction timer.
   */
  @Override
  public synchronized void close() {
    isClosed = true;
    timerTask.cancel();
    timer.purge();
    while (!empty()) {
      KeepAliveEntry e = pop();
      closeHtpClientConnection(e.httpClientConnection);
    }
  }

  /**
   * Gets the latest added HttpClientConnection from the cache. The returned connection
   * is non-stale and has been in the cache for less than connectionIdleTTL milliseconds.
   *
   * The cache is checked from the top of the stack. If the connection is stale or has been
   * in the cache for more than connectionIdleTTL milliseconds, it is closed and the next
   * connection is checked. Once a valid connection is found, it is returned.
   *
   * @return HttpClientConnection: if a valid connection is found, else null.
   */
  public synchronized HttpClientConnection get()
      throws IOException {
    if (isClosed) {
      throw new IOException("KeepAliveCache is closed");
    }
    if (empty()) {
      return null;
    }
    HttpClientConnection hc = null;
    long currentTime = System.currentTimeMillis();
    do {
      KeepAliveEntry e = pop();
      if ((currentTime - e.idleStartTime) > connectionIdleTTL
          || e.httpClientConnection.isStale()) {
        closeHtpClientConnection(e.httpClientConnection);
      } else {
        hc = e.httpClientConnection;
      }
    } while ((hc == null) && (!empty()));
    return hc;
  }

  /**
   * Puts the HttpClientConnection in the cache. If the size of cache is equal to
   * maxConn, the give HttpClientConnection is closed and not added in cache.
   *
   * @param httpClientConnection HttpClientConnection to be cached
   */
  public synchronized void put(HttpClientConnection httpClientConnection) {
    if (isClosed) {
      return;
    }
    if (size() >= maxConn) {
      closeHtpClientConnection(httpClientConnection);
      return;
    }
    KeepAliveEntry entry = new KeepAliveEntry(httpClientConnection,
        System.currentTimeMillis());
    push(entry);
  }

  /**
   * Entry data-structure in the cache.
   */
  static class KeepAliveEntry {

    /**HttpClientConnection in the cache entry.*/
    private final HttpClientConnection httpClientConnection;

    /**Time at which the HttpClientConnection was added to the cache.*/
    private final long idleStartTime;

    KeepAliveEntry(HttpClientConnection hc, long idleStartTime) {
      this.httpClientConnection = hc;
      this.idleStartTime = idleStartTime;
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof KeepAliveEntry) {
        return httpClientConnection.equals(
            ((KeepAliveEntry) o).httpClientConnection);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return httpClientConnection.hashCode();
    }
  }
}
