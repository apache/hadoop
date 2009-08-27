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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;
import org.apache.hadoop.hbase.rest.parser.HBaseRestParserFactory;
import org.apache.hadoop.hbase.rest.parser.IHBaseRestParser;
import org.apache.hadoop.hbase.rest.serializer.RestSerializerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.InfoServer;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.NCSARequestLog;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Servlet implementation class for hbase REST interface. Presumes container
 * ensures single thread through here at any one time (Usually the default
 * configuration). In other words, code is not written thread-safe.
 * <p>
 * This servlet has explicit dependency on Jetty server; it uses the jetty
 * implementation of MultipartResponse.
 * 
 * <p>
 * TODO:
 * <ul>
 * <li>multipart/related response is not correct; the servlet setContentType is
 * broken. I am unable to add parameters such as boundary or start to
 * multipart/related. They get stripped.</li>
 * <li>Currently creating a scanner, need to specify a column. Need to make it
 * so the HTable instance has current table's metadata to-hand so easy to find
 * the list of all column families so can make up list of columns if none
 * specified.</li>
 * <li>Minor items are we are decoding URLs in places where probably already
 * done and how to timeout scanners that are in the scanner list.</li>
 * </ul>
 * 
 * @see <a href="http://wiki.apache.org/lucene-hadoop/Hbase/HbaseRest">Hbase
 *      REST Specification</a>
 * @deprecated Use the {@link org.apache.hadoop.hbase.stargate}  hbase contrib instead.
 */
public class Dispatcher extends javax.servlet.http.HttpServlet {

  /**
   * 
   */
  private static final long serialVersionUID = -8075335435797071569L;
  private static final Log LOG = LogFactory.getLog(Dispatcher.class);
  protected DatabaseController dbController;
  protected TableController tableController;
  protected RowController rowController;
  protected ScannerController scannercontroller;
  protected TimestampController tsController;
  private HBaseConfiguration conf = null;

  public enum ContentType {
    XML("text/xml"), JSON("application/json"), PLAIN("text/plain"), MIME(
        "multipart/related"), NOT_ACCEPTABLE("");

    private final String type;

    private ContentType(final String t) {
      this.type = t;
    }

    @Override
    public String toString() {
      return this.type;
    }

    /**
     * Utility method used looking at Accept header content.
     * 
     * @param t
     *          The content type to examine.
     * @return The enum that matches the prefix of <code>t</code> or the default
     *         enum if <code>t</code> is empty. If unsupported type, we return
     *         NOT_ACCEPTABLE.
     */
    public static ContentType getContentType(final String t) {
      // Default to text/plain. Curl sends */*.
      if (t == null || t.equals("*/*")) {
        return ContentType.XML;
      }
      String lowerCased = t.toLowerCase();
      ContentType[] values = ContentType.values();
      ContentType result = null;
      for (int i = 0; i < values.length; i++) {
        if (lowerCased.startsWith(values[i].type)) {
          result = values[i];
          break;
        }
      }
      return result == null ? NOT_ACCEPTABLE : result;
    }
  }

  /**
   * Default constructor
   */
  public Dispatcher() {
    super();
  }

  @Override
  public void init() throws ServletException {
    super.init();

    this.conf = new HBaseConfiguration();
    HBaseAdmin admin = null;

    try {
      admin = new HBaseAdmin(conf);
      createControllers();

      dbController.initialize(conf, admin);
      tableController.initialize(conf, admin);
      rowController.initialize(conf, admin);
      tsController.initialize(conf, admin);
      scannercontroller.initialize(conf, admin);

      LOG.debug("no errors in init.");
    } catch (Exception e) {
      System.out.println(e.toString());
      throw new ServletException(e);
    }
  }

  protected void createControllers() {
    dbController = new DatabaseController();
    tableController = new TableController();
    rowController = new RowController();
    tsController = new TimestampController();
    scannercontroller = new ScannerController();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    try {
      Status s = this.createStatus(request, response);
      byte[][] pathSegments = getPathSegments(request);
      Map<String, String[]> queryMap = request.getParameterMap();

      if (pathSegments.length == 0 || pathSegments[0].length <= 0) {
        // if it was a root request, then get some metadata about
        // the entire instance.
        dbController.get(s, pathSegments, queryMap);
      } else {
        if (pathSegments.length >= 2
            && pathSegments.length <= 3
            && pathSegments[0].length > 0
            && Bytes.toString(pathSegments[1]).toLowerCase().equals(
                RESTConstants.ROW)) {
          // if it has table name and row path segments
          rowController.get(s, pathSegments, queryMap);
        } else if (pathSegments.length == 4
            && Bytes.toString(pathSegments[1]).toLowerCase().equals(
                RESTConstants.ROW)) {
          tsController.get(s, pathSegments, queryMap);
        } else {
          // otherwise, it must be a GET request suitable for the
          // table handler.
          tableController.get(s, pathSegments, queryMap);
        }
      }
      LOG.debug("GET - No Error");
    } catch (HBaseRestException e) {
      LOG.debug("GET - Error: " + e.toString());
      try {
        Status sError = createStatus(request, response);
        sError.setInternalError(e);
        sError.respond();
      } catch (HBaseRestException f) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    try {

      Status s = createStatus(request, response);
      byte[][] pathSegments = getPathSegments(request);
      Map<String, String[]> queryMap = request.getParameterMap();
      byte[] input = readInputBuffer(request);
      IHBaseRestParser parser = this.getParser(request);

      if ((pathSegments.length >= 0 && pathSegments.length <= 1)
          || Bytes.toString(pathSegments[1]).toLowerCase().equals(
              RESTConstants.ENABLE)
          || Bytes.toString(pathSegments[1]).toLowerCase().equals(
              RESTConstants.DISABLE)) {
        // this is a table request
        tableController.post(s, pathSegments, queryMap, input, parser);
      } else {
        // there should be at least two path segments (table name and row or
        // scanner)
        if (pathSegments.length >= 2 && pathSegments[0].length > 0) {
          if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
              RESTConstants.SCANNER)) {
            scannercontroller.post(s, pathSegments, queryMap, input, parser);
            return;
          } else if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
              RESTConstants.ROW)
              && pathSegments.length >= 3) {
            rowController.post(s, pathSegments, queryMap, input, parser);
            return;
          }
        }
      }
    } catch (HBaseRestException e) {
      LOG.debug("POST - Error: " + e.toString());
      try {
        Status s_error = createStatus(request, response);
        s_error.setInternalError(e);
        s_error.respond();
      } catch (HBaseRestException f) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      byte[][] pathSegments = getPathSegments(request);
      if(pathSegments.length == 0) {
        throw new HBaseRestException("method not supported");
      } else if (pathSegments.length == 1 && pathSegments[0].length > 0) {
        // if it has only table name
        Status s = createStatus(request, response);
        Map<String, String[]> queryMap = request.getParameterMap();
        IHBaseRestParser parser = this.getParser(request);
        byte[] input = readInputBuffer(request);
        tableController.put(s, pathSegments, queryMap, input, parser);
      } else {
        // Equate PUT with a POST.
        doPost(request, response);
      }
    } catch (HBaseRestException e) {
      try {
        Status s_error = createStatus(request, response);
        s_error.setInternalError(e);
        s_error.respond();
      } catch (HBaseRestException f) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void doDelete(HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    try {
      Status s = createStatus(request, response);
      byte[][] pathSegments = getPathSegments(request);
      Map<String, String[]> queryMap = request.getParameterMap();
      
      if(pathSegments.length == 0) {
        throw new HBaseRestException("method not supported");
      } else if (pathSegments.length == 1 && pathSegments[0].length > 0) {
        // if it only has only table name
        tableController.delete(s, pathSegments, queryMap);
        return;
      } else if (pathSegments.length >= 3 && pathSegments[0].length > 0) {
        // must be at least two path segments (table name and row or scanner)
        if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
            RESTConstants.SCANNER)
            && pathSegments.length == 3 && pathSegments[2].length > 0) {
          // DELETE to a scanner requires at least three path segments
          scannercontroller.delete(s, pathSegments, queryMap);
          return;
        } else if (Bytes.toString(pathSegments[1]).toLowerCase().equals(
            RESTConstants.ROW)
            && pathSegments.length >= 3) {
          rowController.delete(s, pathSegments, queryMap);
          return;
        } else if (pathSegments.length == 4) {
          tsController.delete(s, pathSegments, queryMap);
        }
      }
    } catch (HBaseRestException e) {
      LOG.debug("POST - Error: " + e.toString());
      try {
        Status s_error = createStatus(request, response);
        s_error.setInternalError(e);
        s_error.respond();
      } catch (HBaseRestException f) {
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      }
    }
  }

  /**
   * This method will get the path segments from the HttpServletRequest.  Please
   * note that if the first segment of the path is /api this is removed from the 
   * returning byte array.
   * 
   * @param request
   * 
   * @return request pathinfo split on the '/' ignoring the first '/' so first
   * element in pathSegment is not the empty string.
   */
  protected byte[][] getPathSegments(final HttpServletRequest request) {
    int context_len = request.getContextPath().length() + 1;
    
    byte[][] pathSegments = Bytes.toByteArrays(request.getRequestURI().substring(context_len)
        .split("/"));
    byte[] apiAsBytes = "api".getBytes();
    if (Arrays.equals(apiAsBytes, pathSegments[0])) {
      byte[][] newPathSegments = new byte[pathSegments.length - 1][];
      for(int i = 0; i < newPathSegments.length; i++) {
        newPathSegments[i] = pathSegments[i + 1];
      }
      pathSegments = newPathSegments;
    }
    return pathSegments;
  }

  protected byte[] readInputBuffer(HttpServletRequest request)
      throws HBaseRestException {
    try {
      String resultant = "";
      BufferedReader r = request.getReader();

      int defaultmaxlength = 10 * 1024 * 1024;
      int maxLength = this.conf == null?
        defaultmaxlength: this.conf.getInt("hbase.rest.input.limit", defaultmaxlength);
      int bufferLength = 640;

      // TODO make s maxLength and c size values in configuration
      if (!r.ready()) {
        Thread.sleep(1000); // If r is not ready wait 1 second
        if (!r.ready()) { // If r still is not ready something is wrong, return
          // blank.
          return new byte[0];
        }
      }
      char [] c;// 40 characters * sizeof(UTF16)
      while (true) {
    	c = new char[bufferLength]; 
        int n = r.read(c, 0, bufferLength);
        if (n == -1) break;
        resultant += new String(c, 0, n);
        if (resultant.length() > maxLength) {
          resultant = resultant.substring(0, maxLength);
          break;
        }
      }
      return Bytes.toBytes(resultant.trim());
    } catch (Exception e) {
      throw new HBaseRestException(e);
    }
  }

  protected IHBaseRestParser getParser(HttpServletRequest request) {
    return HBaseRestParserFactory.getParser(ContentType.getContentType(request
        .getHeader("content-type")));
  }

  protected Status createStatus(HttpServletRequest request,
      HttpServletResponse response) throws HBaseRestException {
    return new Status(response, RestSerializerFactory.getSerializer(request,
        response), this.getPathSegments(request));
  }

  //
  // Main program and support routines
  //
  protected static void printUsageAndExit() {
    printUsageAndExit(null);
  }

  protected static void printUsageAndExit(final String message) {
    if (message != null) {
      System.err.println(message);
    }
    System.out.println("Usage: java org.apache.hadoop.hbase.rest.Dispatcher "
        + "--help | [--port=PORT] [--bind=ADDR] start");
    System.out.println("Arguments:");
    System.out.println(" start Start REST server");
    System.out.println(" stop  Stop REST server");
    System.out.println("Options:");
    System.out.println(" port  Port to listen on. Default: 60050.");
    System.out.println(" bind  Address to bind on. Default: 0.0.0.0.");
    System.out
        .println(" max-num-threads  The maximum number of threads for Jetty to run. Defaults to 256.");
    System.out.println(" help  Print this message and exit.");

    System.exit(0);
  }

  /*
   * Start up the REST servlet in standalone mode.
   * 
   * @param args
   */
  protected static void doMain(final String[] args) throws Exception {
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
    for (String cmd : args) {
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
        printUsageAndExit("To shutdown the REST server run "
            + "bin/hbase-daemon.sh stop rest or send a kill signal to "
            + "the REST server pid");
      } else if (cmd.startsWith(numThreadsKey)) {
        numThreads = Integer.parseInt(cmd.substring(numThreadsKey.length()));
        continue;
      }

      // Print out usage if we get to here.
      printUsageAndExit();
    }
    org.mortbay.jetty.Server webServer = new org.mortbay.jetty.Server();

    Connector connector = new SocketConnector();
    connector.setPort(port);
    connector.setHost(bindAddress);

    QueuedThreadPool pool = new QueuedThreadPool();
    pool.setMaxThreads(numThreads);

    webServer.addConnector(connector);
    webServer.setThreadPool(pool);

    WebAppContext wac = new WebAppContext();
    wac.setContextPath("/");
    wac.setWar(InfoServer.getWebAppDir("rest"));

    NCSARequestLog ncsa = new NCSARequestLog();
    ncsa.setLogLatency(true);

    RequestLogHandler rlh = new RequestLogHandler();
    rlh.setRequestLog(ncsa);
    rlh.setHandler(wac);

    webServer.addHandler(rlh);

    webServer.start();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.out.println("Starting restServer");
    doMain(args);
  }
}
