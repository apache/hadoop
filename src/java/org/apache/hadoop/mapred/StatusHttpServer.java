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
package org.apache.hadoop.mapred;

import java.io.*;
import java.net.BindException;
import java.net.URL;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.BoundedThreadPool;
import org.mortbay.util.MultiException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.*;


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
  private WebAppContext webAppContext ;  
  private boolean findPort;
  private static final Log LOG =
    LogFactory.getLog(StatusHttpServer.class.getName());
  
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   */
  public StatusHttpServer(String name, String bindAddress, int port, 
                          boolean findPort) throws IOException {
    webServer = new org.mortbay.jetty.Server(port);
    this.findPort = findPort;

    ContextHandlerCollection contexts = new ContextHandlerCollection();

    // set up the context for "/logs/"
    Context logContext = new Context(contexts,"/logs");
    
    String logDir = System.getProperty("hadoop.log.dir");
    logContext.setResourceBase(logDir);
    logContext.addServlet("org.mortbay.jetty.servlet.DefaultServlet","/");
    
    // set up the context for "/static/*"
    String appDir = getWebAppsPath();
    Context staticContext = new Context(contexts,"/static");
    staticContext.setResourceBase(appDir + File.separator + "static");
    staticContext.addServlet("org.mortbay.jetty.servlet.DefaultServlet","/");

    // set up the context for "/" jsp files
    webAppContext = new WebAppContext() ;
    webAppContext.setContextPath("/");
    webAppContext.setWar(appDir + File.separator + name); 
    contexts.addHandler(webAppContext); 

    Context stackContext = new Context(contexts,"/stacks");
    stackContext.addServlet(StackServlet.class, "/"); 
    // used as default but still set, in case it changes in future versions
    webServer.setThreadPool(new BoundedThreadPool()); 
    webServer.setHandler(contexts);
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
   * Add a servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param servletClass The servlet class
   */
  public <T extends HttpServlet> 
  void addServlet(String name, String pathSpec, 
                  Class<T> servletClass) {
    WebAppContext context = webAppContext;
    try {
      if (name == null) {
        context.addServlet(servletClass, pathSpec);
      } else {
        ServletHolder holder = new ServletHolder(servletClass); 
        holder.setName(name); 
        context.addServlet(holder, pathSpec);
      } 
    } catch (Throwable ex) {
      throw makeRuntimeException("Problem instantiating class", ex);
    } 
  }
  
  private static RuntimeException makeRuntimeException(String msg, 
                                                       Throwable cause) {
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
    return webServer.getConnectors()[0].getPort(); 
  }

  public void setThreads(int min, int max) {
    BoundedThreadPool pool = (BoundedThreadPool) webServer.getThreadPool() ;
    pool.setMinThreads(min); 
    pool.setMaxThreads(max); 
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
        } catch(BindException be){
          if( findPort ){
            webServer.getConnectors()[0].setPort(getPort() + 1);
          }else{
            throw be ; 
          }
        }catch (MultiException ex) {
          // look for the multi exception containing a bind exception,
          // in that case try the next port number.
          boolean needNewPort = false;
          for(int i=0; i < ex.size(); ++i) {
            Throwable sub = ex.getThrowable(i);
            
            if (sub instanceof java.net.BindException) {
              needNewPort = true;
            }
          }
          if (!findPort || !needNewPort) {
            throw ex;
          } else {
            // Not using multiple connectors
           webServer.getConnectors()[0].setPort(getPort() + 1);
          }
        }
      }
    }catch (IOException ie) {
      throw ie;
    }catch (Exception e) {
      IOException ie = new IOException("Problem starting http server");
      ie.initCause(e);
      throw ie;
    }
  }
  
  /**
   * stop the server
   */
  public void stop() throws InterruptedException {
    try{
      webServer.stop();
    }catch(InterruptedException ex){
      throw ex ; 
    }catch(Exception e){
      e.printStackTrace(); 
    }
  }
  
  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   * @author Owen O'Malley
   */
  public static class StackServlet extends HttpServlet {
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                     ) throws ServletException, IOException {
      OutputStream outStream = response.getOutputStream();
      ReflectionUtils.printThreadInfo(new PrintWriter(outStream), "");
      outStream.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);      
    }
  }

}
