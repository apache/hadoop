/*
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.stargate;

import java.net.InetAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Main class for launching Stargate as a servlet hosted by an embedded Jetty
 * servlet container.
 * <p> 
 * The following options are supported:
 * <ul>
 * <li>-p: service port</li>
 * </ul>
 */
public class Main implements Constants {

  public static void main(String[] args) throws Exception {
    // process command line

    Options options = new Options();
    options.addOption("p", "port", true, "service port");
    options.addOption("m", "multiuser", false, "enable multiuser mode");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    int port = 8080;
    if (cmd.hasOption("p")) {
      port = Integer.valueOf(cmd.getOptionValue("p"));
    }

    // set up the Jersey servlet container for Jetty

    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    // configure the Stargate singleton

    RESTServlet servlet = RESTServlet.getInstance();
    port = servlet.getConfiguration().getInt("stargate.port", port);
    if (!servlet.isMultiUser()) {
      servlet.setMultiUser(cmd.hasOption("m"));
    }
    servlet.addConnectorAddress(
      servlet.getConfiguration().get("stargate.hostname",
        InetAddress.getLocalHost().getCanonicalHostName()),
      port);

    // set up Jetty and run the embedded server

    Server server = new Server(port);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");

    server.start();
    server.join();
  }
}
