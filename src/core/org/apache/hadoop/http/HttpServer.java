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
package org.apache.hadoop.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.net.InetSocketAddress;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.util.ReflectionUtils;

import org.mortbay.http.HttpContext;
import org.mortbay.http.SocketListener;
import org.mortbay.http.SslListener;
import org.mortbay.http.handler.ResourceHandler;
import org.mortbay.jetty.servlet.WebApplicationContext;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -> points to the log directory
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 */
public class HttpServer {
  public static final Log LOG = LogFactory.getLog(HttpServer.class);

  protected final org.mortbay.jetty.Server webServer;
  protected final WebApplicationContext webAppContext;
  protected final boolean findPort;
  protected final SocketListener listener;
  private SslListener sslListener;

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   */
  public HttpServer(String name, String bindAddress, int port, boolean findPort
      ) throws IOException {
    webServer = new org.mortbay.jetty.Server();
    this.findPort = findPort;
    listener = new SocketListener();
    listener.setPort(port);
    listener.setHost(bindAddress);
    webServer.addListener(listener);

    final String appDir = getWebAppsPath();
    webAppContext = webServer.addWebApplication("/", appDir + "/" + name);
    addWebapps(appDir);
  }

  /**
   * Add webapps and servlets.
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addWebapps(final String appDir) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
    String logDir = System.getProperty("hadoop.log.dir");
    if (logDir != null) {
      HttpContext logContext = new HttpContext();
      logContext.setContextPath("/logs/*");
      logContext.setResourceBase(logDir);
      logContext.addHandler(new ResourceHandler());
      webServer.addContext(logContext);
    }

    // set up the context for "/static/*"
    HttpContext staticContext = new HttpContext();
    staticContext.setContextPath("/static/*");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addHandler(new ResourceHandler());
    webServer.addContext(staticContext);

    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param servletClass The servlet class
   */
  public <T extends HttpServlet> void addServlet(String name, String pathSpec,
      Class<T> servletClass) {
    try {
      if (name == null) {
        webAppContext.addServlet(pathSpec, servletClass.getName());
      } else {
        webAppContext.addServlet(name, pathSpec, servletClass.getName());
      } 
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Problem instantiating class", cnfe);
    } catch (InstantiationException ie) {
      throw new RuntimeException("Problem instantiating class", ie);
    } catch (IllegalAccessException iae) {
      throw new RuntimeException("Problem instantiating class", iae);
    }
  }

  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the webapps files.
   * @return the pathname as a URL
   * @throws IOException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected String getWebAppsPath() throws IOException {
    URL url = getClass().getClassLoader().getResource("webapps");
    if (url == null) 
      throw new IOException("webapps not found in CLASSPATH"); 
    return url.toString();
  }

  /**
   * Get the port that the server is on
   * @return the port
   */
  public int getPort() {
    return listener.getPort();
  }

  public void setThreads(int min, int max) {
    listener.setMinThreads(min);
    listener.setMaxThreads(max);
  }

  /**
   * Configure an ssl listener on the server.
   * @param addr address to listen on
   * @param keystore location of the keystore
   * @param storPass password for the keystore
   * @param keyPass password for the key
   */
  public void addSslListener(InetSocketAddress addr, String keystore,
      String storPass, String keyPass) throws IOException {
    if (sslListener != null || webServer.isStarted()) {
      throw new IOException("Failed to add ssl listener");
    }
    sslListener = new SslListener();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(keystore);
    sslListener.setPassword(storPass);
    sslListener.setKeyPassword(keyPass);
    webServer.addListener(sslListener);
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
          if(ex.size() == 1) {
            Exception sub = ex.getException(0);
            if (sub instanceof java.net.BindException) {
              if(!findPort)
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
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
      
      PrintWriter out = new PrintWriter(response.getOutputStream());
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);      
    }
  }
}