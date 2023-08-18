package org.apache.hadoop.fs.azurebfs.conn;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

public class AbfsHttpUrlConnection extends HttpURLConnection {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpUrlConnection.class);

  public AbfsHttpUrlConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  @Override
  protected void plainConnect0() throws IOException {
    LOG.info("Going for connecting");
    long startTime = System.currentTimeMillis();
    super.plainConnect0();
    LOG.info("Connected in time: " + (System.currentTimeMillis() - startTime));
  }
}
