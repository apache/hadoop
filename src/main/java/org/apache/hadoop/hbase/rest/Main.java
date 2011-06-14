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
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.rest.filter.GzipFilter;
import org.apache.hadoop.hbase.util.VersionInfo;

import java.util.List;
import java.util.ArrayList;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Main class for launching REST gateway as a servlet hosted by Jetty.
 * <p>
 * The following options are supported:
 * <ul>
 * <li>-p --port : service port</li>
 * <li>-ro --readonly : server mode</li>
 * </ul>
 */
public class Main implements Constants {

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("bin/hbase rest start", "", options,
      "\nTo run the REST server as a daemon, execute " +
      "bin/hbase-daemon.sh start|stop rest [-p <port>] [-ro]\n", true);
    System.exit(exitCode);
  }

  /**
   * The main method for the HBase rest server.
   * @param args command-line arguments
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    Log LOG = LogFactory.getLog("RESTServer");

	VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    RESTServlet servlet = RESTServlet.getInstance(conf);

    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default: 8080]");
    options.addOption("ro", "readonly", false, "Respond only to GET HTTP " +
      "method requests [default: false]");

    CommandLine commandLine = null;
    try {
      commandLine = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit(options, -1);
    }

    // check for user-defined port setting, if so override the conf
    if (commandLine != null && commandLine.hasOption("port")) {
      String val = commandLine.getOptionValue("port");
      servlet.getConfiguration()
          .setInt("hbase.rest.port", Integer.valueOf(val));
      LOG.debug("port set to " + val);
    }

    // check if server should only process GET requests, if so override the conf
    if (commandLine != null && commandLine.hasOption("readonly")) {
      servlet.getConfiguration().setBoolean("hbase.rest.readonly", true);
      LOG.debug("readonly set to true");
    }

    @SuppressWarnings("unchecked")
    List<String> remainingArgs = commandLine != null ?
        commandLine.getArgList() : new ArrayList<String>();
    if (remainingArgs.size() != 1) {
      printUsageAndExit(options, 1);
    }

    String command = remainingArgs.get(0);
    if ("start".equals(command)) {
      // continue and start container
    } else if ("stop".equals(command)) {
      System.exit(1);
    } else {
      printUsageAndExit(options, 1);
    }

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");

    // set up Jetty and run the embedded server

    int port = servlet.getConfiguration().getInt("hbase.rest.port", 8080);

    Server server = new Server(port);
    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");
    context.addFilter(GzipFilter.class, "/*", 0);

    server.start();
    server.join();
  }
}
