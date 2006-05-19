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
  private static org.mortbay.jetty.Server webServer = null;
  private static final boolean isWindows = 
    System.getProperty("os.name").startsWith("Windows");
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   */
  public StatusHttpServer(String name, int port) throws IOException {
    webServer = new org.mortbay.jetty.Server();
    SocketListener listener = new SocketListener();
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
    webServer.addWebApplication("/", appDir + File.separator + name);      
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
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      webServer.start();
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
