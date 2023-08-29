package org.apache.hadoop.fs.azurebfs.conn;

import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.http.HttpClient;
import sun.net.www.protocol.http.Handler;
import sun.net.www.protocol.http.HttpURLConnection;

public class AbfsHttpUrlConnection extends HttpURLConnection {

  private static final Logger LOG = LoggerFactory.getLogger(AbfsHttpUrlConnection.class);

  private Boolean failed100cont = false;

  public void failed100cont() {
    failed100cont = true;
  }

  private HttpClient httpClientObj;

  private Boolean insideInputStream = false;
  private Boolean insideResponsCode = false;

  @Override
  public synchronized InputStream getInputStream() throws IOException {
    if(!failed100cont) {
      insideInputStream = true;
      InputStream in = super.getInputStream();
      if(!insideResponsCode) {
        insideInputStream = false;
      }
      return in;
    }
    return null;
  }

  public AbfsHttpUrlConnection(final URL url,
      final Proxy proxy,
      final Handler handler) throws IOException {
    super(url, proxy, handler);
  }

  @Override
  public int getResponseCode() throws IOException {
    insideResponsCode = true;
    int code = super.getResponseCode();
    if (insideInputStream) {
      long cl = 0l;


      String contentLen = getHeaderField("content-length");
      if(contentLen != null) {
        cl = Long.parseLong(contentLen);
      }

      if (method.equals("HEAD") || cl == 0 ||
          code == HTTP_NOT_MODIFIED ||
          code == HTTP_NO_CONTENT) {
        httpClientObj.closeServer();
      }
    }
    insideResponsCode = false;
    return code;
  }

  @Override
  protected void plainConnect0() throws IOException {
    LOG.info("Going for connecting");
    long startTime = System.currentTimeMillis();
    super.plainConnect0();
    httpClientObj = this.http;
    LOG.info("Connected in time: " + (System.currentTimeMillis() - startTime));


  }
  public void closeServer() {
    if(httpClientObj != null) {
      httpClientObj.closeServer();
    }
  }
}
