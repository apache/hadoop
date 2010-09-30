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
package org.apache.hadoop.hbase.rest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

public class HBaseRESTClusterTestBase extends HBaseClusterTestCase 
    implements Constants {

  static final Log LOG =
    LogFactory.getLog(HBaseRESTClusterTestBase.class);

  protected int testServletPort;
  Server server;

  protected void setUp() throws Exception {
    super.setUp();
    startServletContainer();
  }

  protected void tearDown() throws Exception {
    stopServletContainer();
    super.tearDown();
  }

  private void startServletContainer() throws Exception {
    if (server != null) {
      LOG.error("ServletContainer already running");
      return;
    }

    // Inject the conf from the test cluster by being first to make singleton
    RESTServlet.getInstance(super.conf);

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    LOG.info("configured " + ServletContainer.class.getName());
    
    // set up Jetty and run the embedded server
    server = new Server(0);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");
      // start the server
    server.start();
      // get the port
    testServletPort = server.getConnectors()[0].getLocalPort();
    
    LOG.info("started " + server.getClass().getName() + " on port " + 
      testServletPort);
  }

  private void stopServletContainer() {
    if (server != null) try {
      server.stop();
      server = null;
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }
}
