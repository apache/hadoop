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
import java.util.Map;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.mortbay.http.SocketListener;
import org.mortbay.http.SslListener;
import org.mortbay.jetty.servlet.Dispatcher;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.WebApplicationContext;
import org.mortbay.jetty.servlet.WebApplicationHandler;

/**
 * Create a Jetty embedded server to answer http/https requests.
 */
public class ProxyHttpServer {
  public static final Log LOG = LogFactory.getLog(ProxyHttpServer.class);

  protected final org.mortbay.jetty.Server webServer;
  protected final WebApplicationContext webAppContext;
  protected SslListener sslListener;
  protected SocketListener listener;
  protected boolean findPort;

  /**
   * Create a status server on the given port.
   * 
   * @param name
   *            The name of the server
   * @param port
   *            The port to use on the server
   * @param conf
   *            Configuration
   */
  public ProxyHttpServer() throws IOException {
    webServer = new org.mortbay.jetty.Server();
    webAppContext = webServer.addWebApplication("/", "/");
  }

  /**
   * Add a servlet to the server.
   * 
   * @param name
   *            The name of the servlet (can be passed as null)
   * @param pathSpec
   *            The path spec for the servlet
   * @param clazz
   *            The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    try {
      if (name == null) {
        webAppContext.addServlet(pathSpec, clazz.getName());
      } else {
        webAppContext.addServlet(name, pathSpec, clazz.getName());
      }
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Problem instantiating class", cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException("Problem instantiating class", ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Problem instantiating class", iae);
    }
  }

  /** add a global filter */
  public void addGlobalFilter(String name, String classname,
      Map<String, String> parameters) {
    final String[] ALL_URLS = { "/*" };
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    LOG.info("Added global filter" + name + " (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  protected void defineFilter(WebApplicationContext ctx, String name,
      String classname, Map<String, String> parameters, String[] urls) {
    WebApplicationHandler handler = ctx.getWebApplicationHandler();
    FilterHolder holder = handler.defineFilter(name, classname);
    if (parameters != null) {
      for (Map.Entry<String, String> e : parameters.entrySet()) {
        holder.setInitParameter(e.getKey(), e.getValue());
      }
    }
    for (String url : urls) {
      handler.addFilterPathMapping(url, name, Dispatcher.__ALL);
    }
  }

  /**
   * Set a value in the webapp context.
   * 
   * @param name
   *            The name of the attribute
   * @param value
   *            The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Get the value in the webapp context.
   * 
   * @param name
   *            The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /** return the http port that the server is on */
  public int getPort() throws IOException {
    if (listener == null)
      throw new IOException("No http listerner found");
    return listener.getPort();
  }

  public void setThreads(int min, int max) {
    sslListener.setMinThreads(min);
    sslListener.setMaxThreads(max);
  }

  /**
   * Configure an http listener on the server
   * 
   * @param addr
   *            address to listen on
   * @param findPort
   *            whether the listener should bind the given port and increment by
   *            1 until it finds a free port
   */
  public void addListener(InetSocketAddress addr, boolean findPort)
      throws IOException {
    if (listener != null || webServer.isStarted()) {
      throw new IOException("Failed to add listener");
    }
    this.findPort = findPort;
    listener = new SocketListener();
    listener.setHost(addr.getHostName());
    listener.setPort(addr.getPort());
    webServer.addListener(listener);
  }

  /**
   * Configure an ssl listener on the server.
   * 
   * @param addr
   *            address to listen on
   * @param sslConf
   *            conf to retrieve SSL properties from
   */
  public void addSslListener(InetSocketAddress addr, Configuration sslConf)
      throws IOException {
    if (sslListener != null || webServer.isStarted()) {
      throw new IOException("Failed to add ssl listener");
    }
    sslListener = new SslListener();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
    sslListener.setPassword(sslConf.get("ssl.server.keystore.password", ""));
    sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword",
        ""));
    sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
    sslListener.setNeedClientAuth(true);
    webServer.addListener(sslListener);
    System.setProperty("javax.net.ssl.trustStore", sslConf
        .get("ssl.server.truststore.location"));
    System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
        "ssl.server.truststore.password", ""));
    System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
        "ssl.server.truststore.type", "jks"));
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      while (true) {
        try {
          webServer.start();
          break;
        } catch (org.mortbay.util.MultiException ex) {
          // if the multi exception contains ONLY a bind exception,
          // then try the next port number.
          boolean needNewPort = false;
          if (ex.size() == 1) {
            Exception sub = ex.getException(0);
            if (sub instanceof java.net.BindException) {
              if (!findPort || listener == null)
                throw sub; // java.net.BindException
              needNewPort = true;
            }
          }
          if (!needNewPort)
            throw ex;
          listener.setPort(listener.getPort() + 1);
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      IOException ie = new IOException("Problem starting http server");
      ie.initCause(e);
      throw ie;
    }
  }

  /**
   * stop the server
   */
  public void stop() throws InterruptedException {
    webServer.stop();
  }

  /**
   * wait for the server
   */
  public void join() throws InterruptedException {
    webServer.join();
  }
}
