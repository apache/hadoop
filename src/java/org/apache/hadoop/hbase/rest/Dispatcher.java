/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.mapred.StatusHttpServer;
import org.mortbay.http.NCSARequestLog;
import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.WebApplicationContext;

/**
 * Servlet implementation class for hbase REST interface.
 * Presumes container ensures single thread through here at any one time
 * (Usually the default configuration).  In other words, code is not
 * written thread-safe.
 * <p>This servlet has explicit dependency on Jetty server; it uses the
 * jetty implementation of MultipartResponse.
 * 
 * <p>TODO:
 * <ul>
 * <li>multipart/related response is not correct; the servlet setContentType
 * is broken.  I am unable to add parameters such as boundary or start to
 * multipart/related.  They get stripped.</li>
 * <li>Currently creating a scanner, need to specify a column.  Need to make
 * it so the HTable instance has current table's metadata to-hand so easy to
 * find the list of all column families so can make up list of columns if none
 * specified.</li>
 * <li>Minor items are we are decoding URLs in places where probably already
 * done and how to timeout scanners that are in the scanner list.</li>
 * </ul>
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseRest">Hbase REST Specification</a>
 */
@SuppressWarnings("serial")
public class Dispatcher extends javax.servlet.http.HttpServlet
implements javax.servlet.Servlet {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(Dispatcher.class.getName());
  private MetaHandler metaHandler;
  private TableHandler tableHandler;
  private RowHandler rowHandler;
  private ScannerHandler scannerHandler;

  private static final String SCANNER = "scanner";
  private static final String ROW = "row";
      
  /**
   * Default constructor
   */
  public Dispatcher() {
    super();
  }

  @Override
  public void init() throws ServletException {
    super.init();
    
    HBaseConfiguration conf = new HBaseConfiguration();
    HBaseAdmin admin = null;
    
    try{
      admin = new HBaseAdmin(conf);
      metaHandler = new MetaHandler(conf, admin);
      tableHandler = new TableHandler(conf, admin);
      rowHandler = new RowHandler(conf, admin);
      scannerHandler = new ScannerHandler(conf, admin);
    } catch(Exception e){
      throw new ServletException(e);
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 0 || pathSegments[0].length() <= 0) {
      // if it was a root request, then get some metadata about 
      // the entire instance.
      metaHandler.doGet(request, response, pathSegments);
    } else {
      if (pathSegments.length >= 2 && pathSegments[0].length() > 0 && pathSegments[1].toLowerCase().equals(ROW)) {
        // if it has table name and row path segments
        rowHandler.doGet(request, response, pathSegments);
      } else {
        // otherwise, it must be a GET request suitable for the
        // table handler.
        tableHandler.doGet(request, response, pathSegments);
      }
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 0 || pathSegments[0].length() <= 0) {
      // if it was a root request, it must be a create table request
      tableHandler.doPost(request, response, pathSegments);
      return;
    } else {
      // there should be at least two path segments (table name and row or
      // scanner or disable/enable operation)
      if (pathSegments.length >= 2 && pathSegments[0].length() > 0) {
        if (pathSegments[1].toLowerCase().equals(SCANNER)
            && pathSegments.length >= 2) {
          scannerHandler.doPost(request, response, pathSegments);
          return;
        } else if (pathSegments[1].toLowerCase().equals(ROW)
            && pathSegments.length >= 3) {
          rowHandler.doPost(request, response, pathSegments);
          return;
        } else if ((pathSegments[1].toLowerCase().equals(TableHandler.DISABLE) || pathSegments[1].toLowerCase().equals(TableHandler.ENABLE))
            && pathSegments.length == 2) {
          tableHandler.doPost(request, response, pathSegments);
          return;
        }
      }
    }

    // if we get to this point, then no handler was matched this request.
    GenericHandler.doNotFound(response, "No handler for " + request.getPathInfo());
  }
  

  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 1 && pathSegments[0].length() > 0) {
      // if it has only table name
      tableHandler.doPut(request, response, pathSegments);
    } else {
      // Equate PUT with a POST.
      doPost(request, response);
    }
  }

  @Override
  protected void doDelete(HttpServletRequest request,
      HttpServletResponse response)
  throws IOException, ServletException {
    String [] pathSegments = getPathSegments(request);
    
    if (pathSegments.length == 1 && pathSegments[0].length() > 0) {
      // if it only has only table name
      tableHandler.doDelete(request, response, pathSegments);
      return;
    } else if (pathSegments.length >= 3 && pathSegments[0].length() > 0) {
      // must be at least two path segments (table name and row or scanner)
      if (pathSegments[1].toLowerCase().equals(SCANNER) &&
          pathSegments.length == 3 && pathSegments[2].length() > 0) {
        // DELETE to a scanner requires at least three path segments
        scannerHandler.doDelete(request, response, pathSegments);
        return;
      } else if (pathSegments[1].toLowerCase().equals(ROW) &&
          pathSegments.length >= 3) {
        rowHandler.doDelete(request, response, pathSegments);
        return;
      }
    }
    
    // if we reach this point, then no handler exists for this request.
    GenericHandler.doNotFound(response, "No handler");
  }
  
  /*
   * @param request
   * @return request pathinfo split on the '/' ignoring the first '/' so first
   * element in pathSegment is not the empty string.
   */
  private String [] getPathSegments(final HttpServletRequest request) {
    int context_len = request.getContextPath().length() + 1;
    return request.getRequestURI().substring(context_len).split("/");
  }

  //
  // Main program and support routines
  //
  
  private static void printUsageAndExit() {
    printUsageAndExit(null);
  }
  
  private static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.out.println("Usage: java org.apache.hadoop.hbase.rest.Dispatcher " +
      "--help | [--port=PORT] [--bind=ADDR] start");
    System.out.println("Arguments:");
    System.out.println(" start Start REST server");
    System.out.println(" stop  Stop REST server");
    System.out.println("Options:");
    System.out.println(" port  Port to listen on. Default: 60050.");
    System.out.println(" bind  Address to bind on. Default: 0.0.0.0.");
    System.out.println(" max-num-threads  The maximum number of threads for Jetty to run. Defaults to 256.");
    System.out.println(" help  Print this message and exit.");

    System.exit(0);
  }

  /*
   * Start up the REST servlet in standalone mode.
   * @param args
   */
  protected static void doMain(final String [] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }

    int port = 60050;
    String bindAddress = "0.0.0.0";
    int numThreads = 256;

    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).
    final String addressArgKey = "--bind=";
    final String portArgKey = "--port=";
    final String numThreadsKey = "--max-num-threads=";
    for (String cmd: args) {
      if (cmd.startsWith(addressArgKey)) {
        bindAddress = cmd.substring(addressArgKey.length());
        continue;
      } else if (cmd.startsWith(portArgKey)) {
        port = Integer.parseInt(cmd.substring(portArgKey.length()));
        continue;
      } else if (cmd.equals("--help") || cmd.equals("-h")) {
        printUsageAndExit();
      } else if (cmd.equals("start")) {
        continue;
      } else if (cmd.equals("stop")) {
        printUsageAndExit("To shutdown the REST server run " +
          "bin/hbase-daemon.sh stop rest or send a kill signal to " +
          "the REST server pid");
      } else if (cmd.startsWith(numThreadsKey)) { 
        numThreads = Integer.parseInt(cmd.substring(numThreadsKey.length()));
        continue;
      }
      
      // Print out usage if we get to here.
      printUsageAndExit();
    }
    org.mortbay.jetty.Server webServer = new org.mortbay.jetty.Server();
    SocketListener listener = new SocketListener();
    listener.setPort(port);
    listener.setHost(bindAddress);
    listener.setMaxThreads(numThreads);
    webServer.addListener(listener);
    NCSARequestLog ncsa = new NCSARequestLog();
    ncsa.setLogLatency(true);
    webServer.setRequestLog(ncsa);
    WebApplicationContext context =
      webServer.addWebApplication("/api", InfoServer.getWebAppDir("rest"));
    context.addServlet("stacks", "/stacks",
      StatusHttpServer.StackServlet.class.getName());
    context.addServlet("logLevel", "/logLevel",
      org.apache.hadoop.log.LogLevel.Servlet.class.getName());
    webServer.start();
  }
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String [] args) throws Exception {
    doMain(args);
  }
}
