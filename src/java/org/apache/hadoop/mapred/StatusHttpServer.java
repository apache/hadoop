/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

import org.mortbay.http.HttpContext;
import org.mortbay.http.handler.ResourceHandler;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.WebApplicationContext;
import org.mortbay.jetty.servlet.ServletHttpContext;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -> points to the log directory
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 * @author Owen O'Malley
 */
public class StatusHttpServer {
  private static final boolean isWindows = 
    System.getProperty("os.name").startsWith("Windows");
  private org.mortbay.jetty.Server webServer;
  private SocketListener listener;
  private boolean findPort;
  private WebApplicationContext webAppContext;
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   */
  public StatusHttpServer(String name, int port, 
                          boolean findPort) throws IOException {
    webServer = new org.mortbay.jetty.Server();
    this.findPort = findPort;
    listener = new SocketListener();
    listener.setPort(port);
    webServer.addListener(listener);

    // set up the context for "/logs/"
    HttpContext logContext = new HttpContext();
    logContext.setContextPath("/logs/*");
    String logDir = System.getProperty("hadoop.log.dir");
    logContext.setResourceBase(logDir);
    logContext.addHandler(new ResourceHandler());
    webServer.addContext(logContext);

    // set up the context for "/static/*"
    String appDir = getWebAppsPath();
    HttpContext staticContext = new HttpContext();
    staticContext.setContextPath("/static/*");
    staticContext.setResourceBase(appDir + File.separator + "static");
    staticContext.addHandler(new ResourceHandler());
    webServer.addContext(staticContext);

    // set up the context for "/" jsp files
    webAppContext = 
      webServer.addWebApplication("/", appDir + File.separator + name);      
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name,value);
  }

  /**
   * Add a servlet in the server
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param classname The class name for the servlet
   * @param contextPath The context path (can be null, defaults to "/")
   */
  public void addServlet(String name, String pathSpec, String classname,
     String contextPath) 
 throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String tmpContextPath = contextPath;
    if (tmpContextPath == null) tmpContextPath = "/";
    ServletHttpContext context = 
                    (ServletHttpContext)webServer.getContext(tmpContextPath);
    if (name == null)
      context.addServlet(pathSpec, classname);
    else
      context.addServlet(name, pathSpec, classname);
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
   * @return the pathname
   */
  private static String getWebAppsPath() throws IOException {
    URL url = StatusHttpServer.class.getClassLoader().getResource("webapps");
    String path = url.getPath();
    if (isWindows && path.startsWith("/")) {
      path = path.substring(1);
      try {
        path = URLDecoder.decode(path, "UTF-8");
      } catch (UnsupportedEncodingException e) {
      }
    }
    return new File(path).getCanonicalPath();
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
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      while (true) {
        try {
          webServer.start();
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
          } else {
            listener.setPort(listener.getPort() + 1);
          }
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
}
