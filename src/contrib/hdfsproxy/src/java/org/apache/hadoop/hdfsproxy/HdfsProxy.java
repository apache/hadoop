/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfsproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.StringUtils;

/**
 * A HTTPS/SSL proxy to HDFS, implementing certificate based access control.
 */
public class HdfsProxy {
  public static final Log LOG = LogFactory.getLog(HdfsProxy.class);

  private ProxyHttpServer server;
  private InetSocketAddress sslAddr;
  
  /** Construct a proxy from the given configuration */
  public HdfsProxy(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      this.stop();
      throw e;
    }
  }
  
  private void initialize(Configuration conf) throws IOException {
    sslAddr = getSslAddr(conf);
    String nn = conf.get("hdfsproxy.dfs.namenode.address");
    if (nn == null)
      throw new IOException("HDFS NameNode address is not specified");
    InetSocketAddress nnAddr = NetUtils.createSocketAddr(nn);
    LOG.info("HDFS NameNode is at: " + nnAddr.getHostName() + ":" + nnAddr.getPort());

    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get("hdfsproxy.https.server.keystore.resource",
        "ssl-server.xml"));
    // unit testing
    sslConf.set("proxy.http.test.listener.addr",
                conf.get("proxy.http.test.listener.addr"));

    this.server = new ProxyHttpServer(sslAddr, sslConf);
    this.server.setAttribute("proxy.https.port", server.getPort());
    this.server.setAttribute("name.node.address", nnAddr);
    this.server.setAttribute("name.conf", new Configuration());
    this.server.addGlobalFilter("ProxyFilter", ProxyFilter.class.getName(), null);
    this.server.addServlet("listPaths", "/listPaths/*", ProxyListPathsServlet.class);
    this.server.addServlet("data", "/data/*", ProxyFileDataServlet.class);
    this.server.addServlet("streamFile", "/streamFile/*", ProxyStreamFile.class);
  }

  /** return the http port if any, only for testing purposes */
  int getPort() throws IOException {
    return server.getPort();
  }
  
  /**
   * Start the server.
   */
  public void start() throws IOException {
    this.server.start();
    LOG.info("HdfsProxy server up at: " + sslAddr.getHostName() + ":"
        + sslAddr.getPort());
  }
  
  /**
   * Stop all server threads and wait for all to finish.
   */
  public void stop() {
    try {
      if (server != null) {
        server.stop();
        server.join();
      }
    } catch (Exception e) {
      LOG.warn("Got exception shutting down proxy", e);
    }
  }
  
  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      this.server.join();
    } catch (InterruptedException ie) {
    }
  }
  
  private static enum StartupOption {
    RELOAD("-reloadPermFiles"), CLEAR("-clearUgiCache"), REGULAR("-regular");

    private String name = null;

    private StartupOption(String arg) {
      this.name = arg;
    }

    public String getName() {
      return name;
    }
  }

  private static void printUsage() {
    System.err.println("Usage: hdfsproxy ["
        + StartupOption.RELOAD.getName() + "] | ["
        + StartupOption.CLEAR.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for (int i = 0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.RELOAD.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.RELOAD;
      } else if (StartupOption.CLEAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.CLEAR;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else
        return null;
    }
    return startOpt;
  }

  /**
   * Dummy hostname verifier that is used to bypass hostname checking
   */
  private static class DummyHostnameVerifier implements HostnameVerifier {
    public boolean verify(String hostname, SSLSession session) {
      return true;
    }
  }

  private static HttpsURLConnection openConnection(String hostname, int port,
      String path) throws IOException {
    try {
      final URL url = new URI("https", null, hostname, port, path, null, null)
          .toURL();
      HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
      // bypass hostname verification
      conn.setHostnameVerifier(new DummyHostnameVerifier());
      conn.setRequestMethod("GET");
      return conn;
    } catch (URISyntaxException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  private static void setupSslProps(Configuration conf) {
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get("hdfsproxy.https.server.keystore.resource",
        "ssl-server.xml"));
    System.setProperty("javax.net.ssl.trustStore", sslConf
        .get("ssl.server.truststore.location"));
    System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
        "ssl.server.truststore.password", ""));
    System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
        "ssl.server.truststore.type", "jks"));
    System.setProperty("javax.net.ssl.keyStore", sslConf
        .get("ssl.server.keystore.location"));
    System.setProperty("javax.net.ssl.keyStorePassword", sslConf.get(
        "ssl.server.keystore.password", ""));
    System.setProperty("javax.net.ssl.keyPassword", sslConf.get(
        "ssl.server.keystore.keypassword", ""));
    System.setProperty("javax.net.ssl.keyStoreType", sslConf.get(
        "ssl.server.keystore.type", "jks"));
  }

  static InetSocketAddress getSslAddr(Configuration conf) throws IOException {
    String addr = conf.get("hdfsproxy.https.address");
    if (addr == null)
      throw new IOException("HdfsProxy address is not specified");
    return NetUtils.createSocketAddr(addr);
  }

  private static boolean sendCommand(Configuration conf, String path)
      throws IOException {
    setupSslProps(conf);
    int sslPort = getSslAddr(conf).getPort();
    int err = 0;
    StringBuilder b = new StringBuilder();
    HostsFileReader hostsReader = new HostsFileReader(conf.get("hdfsproxy.hosts",
        "hdfsproxy-hosts"), "");
    Set<String> hostsList = hostsReader.getHosts();
    for (String hostname : hostsList) {
      HttpsURLConnection connection = null;
      try {
        connection = openConnection(hostname, sslPort, path);
        connection.connect();
        if (connection.getResponseCode() != HttpServletResponse.SC_OK) {
          b.append("\n\t" + hostname + ": " + connection.getResponseCode()
              + " " + connection.getResponseMessage());
          err++;
        }
      } catch (IOException e) {
        b.append("\n\t" + hostname + ": " + e.getLocalizedMessage());
        err++;
      } finally {
        if (connection != null)
          connection.disconnect();
      }
    }
    if (err > 0) {
      System.err.print("Command failed on the following "
          + err + " host" + (err==1?":":"s:") + b.toString() + "\n");
      return true;
    }
    return false;
  }

  public static HdfsProxy createHdfsProxy(String argv[], Configuration conf)
      throws IOException {
    if (conf == null) {
      conf = new Configuration(false);
      conf.addResource("hdfsproxy-default.xml");
    }
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }

    switch (startOpt) {
    case RELOAD:
      boolean error = sendCommand(conf, "/reloadPermFiles");
      System.exit(error ? 1 : 0);
    case CLEAR:
      error = sendCommand(conf, "/clearUgiCache");
      System.exit(error ? 1 : 0);
    default:
    }

    StringUtils.startupShutdownMessage(HdfsProxy.class, argv, LOG);
    HdfsProxy proxy = new HdfsProxy(conf);
    //proxy.addSslListener(conf);
    proxy.start();
    return proxy;
  }

  public static void main(String[] argv) throws Exception {
    try {
      HdfsProxy proxy = createHdfsProxy(argv, null);
      if (proxy != null)
        proxy.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
