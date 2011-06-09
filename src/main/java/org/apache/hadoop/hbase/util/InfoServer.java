/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.http.HttpServer;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/stacks/" -> points to stack trace
 *   "/static/" -> points to common static files (src/hbase-webapps/static)
 *   "/" -> the jsp server code from (src/hbase-webapps/<name>)
 */
public class InfoServer extends HttpServer {
  private final Configuration config;

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/hbase-webapps/<code>name<code>.
   * @param name The name of the server
   * @param bindAddress address to bind to
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   * increment by 1 until it finds a free port.
   * @throws IOException e
   */
  public InfoServer(String name, String bindAddress, int port, boolean findPort,
      final Configuration c)
  throws IOException {
    super(name, bindAddress, port, findPort, HBaseConfiguration.create());
    this.config = c;
    fixupLogsServletLocation();
  }

  /**
   * Fixup where the logs app points, make it point at hbase logs rather than
   * hadoop logs.
   */
  private void fixupLogsServletLocation() {
    // Must be same as up in hadoop.
    final String logsContextPath = "/logs";
    // Now, put my logs in place of hadoops... disable old one first.
    Context oldLogsContext = null;
    for (Map.Entry<Context, Boolean> e : defaultContexts.entrySet()) {
      if (e.getKey().getContextPath().equals(logsContextPath)) {
        oldLogsContext = e.getKey();
        break;
      }
    }
    if (oldLogsContext != null) {
      this.defaultContexts.put(oldLogsContext, Boolean.FALSE);
    }
    // Now do my logs.
    // Set up the context for "/logs/" if "hbase.log.dir" property is defined.
    String logDir = System.getProperty("hbase.log.dir");
    if (logDir != null) {
      // This is a little presumptious but seems to work.
      Context logContext =
        new Context((ContextHandlerCollection)this.webServer.getHandler(),
          logsContextPath);
      logContext.setResourceBase(logDir);
      logContext.addServlet(DefaultServlet.class, "/");
      defaultContexts.put(logContext, true);
    }
  }

  /**
   * Get the pathname to the webapps files.
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected String getWebAppsPath(String appName) throws FileNotFoundException {
    // Copied from the super-class.
    URL url = getClass().getClassLoader().getResource("hbase-webapps/" + appName);
    if (url == null)
      throw new FileNotFoundException("webapps/" + appName
          + " not found in CLASSPATH");
    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the pathname to the <code>path</code> files.
   * @return the pathname as a URL
   */
  protected String getWebAppsPath() throws IOException {
    // Hack: webapps is not a unique enough element to find in CLASSPATH
    // We'll more than likely find the hadoop webapps dir.  So, instead
    // look for the 'master' webapp in the webapps subdir.  That should
    // get us the hbase context.  Presumption is that place where the
    // master webapp resides is where we want this InfoServer picking up
    // web applications.
    final String master = "master";
    String p = getWebAppsPath(master);
    int index = p.lastIndexOf(master);
    // Now strip master off the end if it is present
    return index == -1? p: p.substring(0, index);
  }
}