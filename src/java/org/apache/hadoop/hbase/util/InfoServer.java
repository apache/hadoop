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
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.mortbay.http.HttpContext;
import org.mortbay.http.SocketListener;
import org.mortbay.http.handler.ResourceHandler;
import org.mortbay.jetty.servlet.WebApplicationContext;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/stacks/" -> points to stack trace
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 */
public class InfoServer {
  // Bulk of this class is copied from
  // {@link org.apache.hadoop.mapred.StatusHttpServer}.  StatusHttpServer
  // is not amenable to subclassing.  It keeps webAppContext inaccessible
  // and will find webapps only in the jar the class StatusHttpServer was
  // loaded from.
  private static final Log LOG = LogFactory.getLog(InfoServer.class.getName());
  private org.mortbay.jetty.Server webServer;
  private SocketListener listener;
  private boolean findPort;
  private WebApplicationContext webAppContext;
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<code>name<code>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   * increment by 1 until it finds a free port.
   */
  public InfoServer(String name, String bindAddress, int port, boolean findPort)
  throws IOException {
    this.webServer = new org.mortbay.jetty.Server();
    this.findPort = findPort;
    this.listener = new SocketListener();
    this.listener.setPort(port);
    this.listener.setHost(bindAddress);
    this.webServer.addListener(listener);

    // Set up the context for "/static/*"
    String appDir = getWebAppsPath();
    
    // Set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
    String logDir = System.getProperty("hbase.log.dir");
    if (logDir != null) {
      HttpContext logContext = new HttpContext();
      logContext.setContextPath("/logs/*");
      logContext.setResourceBase(logDir);
      logContext.addHandler(new ResourceHandler());
      webServer.addContext(logContext);
    }
    
    HttpContext staticContext = new HttpContext();
    staticContext.setContextPath("/static/*");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addHandler(new ResourceHandler());
    this.webServer.addContext(staticContext);

    // set up the context for "/" jsp files
    String webappDir = getWebAppDir(name);
    this.webAppContext =
      this.webServer.addWebApplication("/", webappDir);
    if (name.equals("master")) {
      // Put up the rest webapp.
      this.webServer.addWebApplication("/api", getWebAppDir("rest"));
    }
    addServlet("stacks", "/stacks", StatusHttpServer.StackServlet.class);
    addServlet("logLevel", "/logLevel", org.apache.hadoop.log.LogLevel.Servlet.class);
  }
  
  public static String getWebAppDir(final String webappName) throws IOException {
    String webappDir = null;
    try {
      webappDir = getWebAppsPath("webapps" + File.separator + webappName);
    } catch (FileNotFoundException e) {
      // Retry.  Resource may be inside jar on a windows machine.
      webappDir = getWebAppsPath("webapps/" + webappName);
    }
    return webappDir;
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    this.webAppContext.setAttribute(name, value);
  }

  /**
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param servletClass The servlet class
   */
  public <T extends HttpServlet> void addServlet(String name, String pathSpec, 
      Class<T> servletClass) {
    WebApplicationContext context = webAppContext;
    try {
      if (name == null) {
        context.addServlet(pathSpec, servletClass.getName());
      } else {
        context.addServlet(name, pathSpec, servletClass.getName());
      } 
    } catch (ClassNotFoundException ex) {
      throw makeRuntimeException("Problem instantiating class", ex);
    } catch (InstantiationException ex) {
      throw makeRuntimeException("Problem instantiating class", ex);
    } catch (IllegalAccessException ex) {
      throw makeRuntimeException("Problem instantiating class", ex);
    }
  }
  
  private static RuntimeException makeRuntimeException(String msg, Throwable cause) {
    RuntimeException result = new RuntimeException(msg);
    if (cause != null) {
      result.initCause(cause);
    }
    return result;
  }
  
  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return this.webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the <code>webapps</code> files.
   * @return the pathname as a URL
   */
  private static String getWebAppsPath() throws IOException {
    return getWebAppsPath("webapps");
  }
  
  /**
   * Get the pathname to the <code>patch</code> files.
   * @param path Path to find.
   * @return the pathname as a URL
   */
  private static String getWebAppsPath(final String path) throws IOException {
    URL url = InfoServer.class.getClassLoader().getResource(path);
    if (url == null) 
      throw new IOException("webapps not found in CLASSPATH: " + path); 
    return url.toString();
  }
  
  /**
   * Get the port that the server is on
   * @return the port
   */
  public int getPort() {
    return this.listener.getPort();
  }

  public void setThreads(int min, int max) {
    this.listener.setMinThreads(min);
    this.listener.setMaxThreads(max);
  }
  
  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      while (true) {
        try {
          this.webServer.start();
          break;
        } catch (org.mortbay.util.MultiException ex) {
          // look for the multi exception containing a bind exception,
          // in that case try the next port number.
          boolean needNewPort = false;
          for(int i=0; i < ex.size(); ++i) {
            Exception sub = ex.getException(i);
            if (sub instanceof java.net.BindException) {
              needNewPort = true;
              break;
            }
          }
          if (!findPort || !needNewPort) {
            throw ex;
          }
          this.listener.setPort(listener.getPort() + 1);
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
    this.webServer.stop();
  }
}
