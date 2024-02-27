package org.apache.hadoop.fs.azurebfs.services;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Stack;

import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.conn.ManagedHttpClientConnectionFactory;

public class AbfsConnFactory extends ManagedHttpClientConnectionFactory {

  @Override
  public ManagedHttpClientConnection create(final HttpRoute route,
      final ConnectionConfig config) {
    return new AbfsApacheHttpConnection(super.create(route, config), route);
  }


  public static class AbfsApacheHttpConnection implements ManagedHttpClientConnection {
    private final ManagedHttpClientConnection httpClientConnection;
    public final HttpRoute httpRoute;

    private final static Stack<ManagedHttpClientConnection> stack = new Stack<>();
    private final static Thread thread;

    static {
      thread = new Thread(() -> {
        while(true) {
          if(!stack.empty()) {
            try {
              stack.pop().close();
            } catch (IOException ex) {}
          }
        }
      });
      thread.start();
    }

    public AbfsApacheHttpConnection(ManagedHttpClientConnection conn,
        final HttpRoute route) {
      this.httpClientConnection = conn;
      this.httpRoute = route;
    }

    @Override
    public void close() throws IOException {
      stack.push(httpClientConnection);
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
      return val;
    }

    @Override
    public void sendRequestHeader(final HttpRequest request)
        throws HttpException, IOException {
      long start = System.currentTimeMillis();
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
      long start = System.currentTimeMillis();
      HttpResponse response = httpClientConnection.receiveResponseHeader();
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
}
