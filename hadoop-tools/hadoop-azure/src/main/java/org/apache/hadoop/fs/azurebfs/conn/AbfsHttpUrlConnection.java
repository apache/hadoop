package org.apache.hadoop.fs.azurebfs.conn;

import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import sun.net.www.http.HttpClient;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

import static org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID;

public class AbfsHttpUrlConnection extends HttpURLConnection {
  private Boolean getOutputStreamFailed = false;

  private static Set<HttpClient> httpClientSet = new HashSet<>();

  public Long timeTaken;
  public Boolean isFromCache = true;

  public AbfsHttpUrlConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  @Override
  protected void plainConnect0() throws IOException {
    Long start = System.currentTimeMillis();
    super.plainConnect0();
    timeTaken = System.currentTimeMillis() - start;
    if(!httpClientSet.contains(http)) {
      isFromCache = false;
    }
    httpClientSet.add(http);
  }

  private void publish(final Long timeTaken) {
    setRequestProperty(X_MS_CLIENT_REQUEST_ID, getRequestProperty(X_MS_CLIENT_REQUEST_ID) + "_" + timeTaken);
  }

  public void registerGetOutputStreamFailure() {
    getOutputStreamFailed = true;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    if(!getOutputStreamFailed) {
      return super.getInputStream();
    }
    return null;
  }
}
