/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.List;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Main class for launching REST gateway as a servlet hosted by Jetty.
 * <p> 
 * The following options are supported:
 * <ul>
 * <li>-p: service port</li>
 * </ul>
 */
public class Main implements Constants {
  private static final String DEFAULT_LISTEN_PORT = "8080";

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("REST", null, options,
      "To start the REST server run 'bin/hbase-daemon.sh start rest'\n" +
      "To shutdown the REST server run 'bin/hbase-daemon.sh stop rest' or" +
      " send a kill signal to the rest server pid",
      true);
    System.exit(exitCode);
  }

  public static void main(String[] args) throws Exception {
    Log LOG = LogFactory.getLog("RESTServer");
    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default:" +
      DEFAULT_LISTEN_PORT + "]");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    /**
     * This is so complicated to please both bin/hbase and bin/hbase-daemon.
     * hbase-daemon provides "start" and "stop" arguments
     * hbase should print the help if no argument is provided
     */
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    if (cmd.hasOption("help") || !start || stop) {
      printUsageAndExit(options, 1);
    }
    // Get port to bind to
    int port = 0;
    try {
      port = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // set up the Jersey servlet container for Jetty

    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    // set up Jetty and run the embedded server

    RESTServlet servlet = RESTServlet.getInstance();
    port = servlet.getConfiguration().getInt("hbase.rest.port", port);

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
