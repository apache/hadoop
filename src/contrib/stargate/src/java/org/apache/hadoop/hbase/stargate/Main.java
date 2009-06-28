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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.NCSARequestLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.deployer.WebAppDeployer;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.thread.QueuedThreadPool;

public class Main {

  public static void main(String[] args) throws Exception {
    // process command line
    Options options = new Options();
    options.addOption("p", "port", true, "service port");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    int port = 8080;
    if (cmd.hasOption("p")) {
      port = Integer.valueOf(cmd.getOptionValue("p"));
    }

    HBaseConfiguration conf = new HBaseConfiguration();
    if (cmd.hasOption("m")) {
      conf.set("hbase.master", cmd.getOptionValue("m"));
    }

    /*
     * RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean(); if (runtime
     * != null) { LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
     * runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
     * LOG.info("vmInputArguments=" + runtime.getInputArguments()); }
     */
    /*
     * poached from:
     * http://jetty.mortbay.org/xref/org/mortbay/jetty/example/LikeJettyXml.html
     */
    String jetty_home = ".";
    Server server = new Server();

    QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMaxThreads(100);
    server.setThreadPool(threadPool);

    Connector connector = new SelectChannelConnector();
    connector.setPort(port);
    connector.setMaxIdleTime(30000);
    server.setConnectors(new Connector[] { connector });

    HandlerCollection handlers = new HandlerCollection();
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    RequestLogHandler requestLogHandler = new RequestLogHandler();
    handlers.setHandlers(new Handler[] { contexts, new DefaultHandler(), 
    										requestLogHandler });
    server.setHandler(handlers);

    WebAppDeployer deployer1 = new WebAppDeployer();
    deployer1.setContexts(contexts);
    deployer1.setWebAppDir(jetty_home + "/webapps");
    deployer1.setParentLoaderPriority(false);
    deployer1.setExtract(true);
    deployer1.setAllowDuplicates(false);
    // deployer1.setDefaultsDescriptor(jetty_home + "/etc/webdefault.xml");
    server.addLifeCycle(deployer1);

    NCSARequestLog requestLog = new NCSARequestLog(jetty_home 
    		+ "/logs/jetty-yyyy_mm_dd.log");
    requestLog.setExtended(false);
    requestLogHandler.setRequestLog(requestLog);
    
    server.setStopAtShutdown(true);
    server.setSendServerVersion(true);
    server.start();
    server.join();
  }
}
