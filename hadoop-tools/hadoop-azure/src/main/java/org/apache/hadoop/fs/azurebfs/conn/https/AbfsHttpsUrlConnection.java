package org.apache.hadoop.fs.azurebfs.conn.https;

import java.io.IOException;
import java.net.Proxy;
import java.net.URL;

import sun.net.www.protocol.https.Handler;
import sun.net.www.protocol.https.HttpsURLConnectionImpl;

public class AbfsHttpsUrlConnection extends HttpsURLConnectionImpl {

  protected AbfsHttpsUrlConnection(final URL url, Handler handler) throws IOException {
    super(url);
  }
}
