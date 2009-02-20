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
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.http.SocketListener;
import org.mortbay.http.SslListener;
import org.mortbay.jetty.servlet.Dispatcher;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.WebApplicationContext;
import org.mortbay.jetty.servlet.WebApplicationHandler;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -> points to the log directory
 *   "/static/" -> points to common static files (src/webapps/static)
 *   "/" -> the jsp server code from (src/webapps/<name>)
 */
public class HttpServer implements FilterContainer {
  public static final Log LOG = LogFactory.getLog(HttpServer.class);

  static final String FILTER_INITIALIZER_PROPERTY
      = "hadoop.http.filter.initializers";

  protected final org.mortbay.jetty.Server webServer;
  protected final WebApplicationContext webAppContext;
  protected final Map<WebApplicationContext, Boolean> defaultContexts = 
    new HashMap<WebApplicationContext, Boolean>();
  protected final boolean findPort;
  protected final SocketListener listener;
  private SslListener sslListener;
  protected final List<String> filterNames = new ArrayList<String>();

  /** Same as this(name, bindAddress, port, findPort, null); */
  public HttpServer(String name, String bindAddress, int port, boolean findPort
      ) throws IOException {
    this(name, bindAddress, port, findPort, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   * @param conf Configuration 
   */
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf) throws IOException {
    webServer = new org.mortbay.jetty.Server();
    this.findPort = findPort;
    listener = new SocketListener();
    listener.setPort(port);
    listener.setHost(bindAddress);
    webServer.addListener(listener);

    final String appDir = getWebAppsPath();
    webAppContext = webServer.addWebApplication("/", appDir + "/" + name);
    addDefaultApps(appDir);

    final FilterInitializer[] initializers = getFilterInitializers(conf); 
    if (initializers != null) {
      for(FilterInitializer c : initializers) {
        c.initFilter(this);
      }
    }
    addDefaultServlets();
  }

  /** Get an array of FilterConfiguration specified in the conf */
  private static FilterInitializer[] getFilterInitializers(Configuration conf) {
    if (conf == null) {
      return null;
    }

    Class<?>[] classes = conf.getClasses(FILTER_INITIALIZER_PROPERTY);
    if (classes == null) {
      return null;
    }

    FilterInitializer[] initializers = new FilterInitializer[classes.length];
    for(int i = 0; i < classes.length; i++) {
      initializers[i] = (FilterInitializer)ReflectionUtils.newInstance(
          classes[i], conf);
    }
    return initializers;
  }

  /**
   * Add default apps.
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(final String appDir) throws IOException {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
    String logDir = System.getProperty("hadoop.log.dir");
    if (logDir != null) {
      addContext("/logs/*", logDir, true);
    }

    // set up the context for "/static/*"
    addContext("/static/*", appDir + "/static", true);
  }
  
  /**
   * Add default servlets.
   */
  protected void addDefaultServlets() {
    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
  }

  /**
   * Add a context 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping 
   * @throws IOException 
   */
  protected void addContext(String pathSpec, String dir, boolean isFiltered) throws IOException {
    WebApplicationContext webAppCtx = webServer.addWebApplication(pathSpec, dir);
    defaultContexts.put(webAppCtx, isFiltered);
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
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz);
    addFilterPathMapping(pathSpec);
  }

  /**
   * Add an internal servlet in the server.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @deprecated this is a temporary method
   */
  @Deprecated
  public void addInternalServlet(String name, String pathSpec,
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

  /** {@inheritDoc} */
  public void addFilter(String name, String classname,
      Map<String, String> parameters) {

    final String[] USER_FACING_URLS = {"*.html", "*.jsp"};
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);

    final String[] ALL_URLS = { "/*" };
    for (Map.Entry<WebApplicationContext, Boolean> e : defaultContexts
        .entrySet()) {
      if (e.getValue()) {
        WebApplicationContext ctx = e.getKey();
        defineFilter(ctx, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + ctx.getName());
      }
    }
    filterNames.add(name);
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  protected void defineFilter(WebApplicationContext ctx, String name,
      String classname, Map<String, String> parameters, String[] urls) {

    WebApplicationHandler handler = ctx.getWebApplicationHandler();
    FilterHolder holder = handler.defineFilter(name, classname);
    if (parameters != null) {
      for(Map.Entry<String, String> e : parameters.entrySet()) {
        holder.setInitParameter(e.getKey(), e.getValue());
      }
    }

    for (String url : urls) {
      handler.addFilterPathMapping(url, name, Dispatcher.__ALL);
    }
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   */
  protected void addFilterPathMapping(String pathSpec) {
    WebApplicationHandler handler = webAppContext.getWebApplicationHandler();
    for(String name : filterNames) {
      handler.addFilterPathMapping(pathSpec, name, Dispatcher.__ALL);
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