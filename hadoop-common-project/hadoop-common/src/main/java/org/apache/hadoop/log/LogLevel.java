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
package org.apache.hadoop.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Jdk14Logger;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ServletUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Change log level in runtime.
 */
@InterfaceStability.Evolving
public class LogLevel {
  public static final String USAGES = "\nUsage: Command options are:\n"
      + "\t[-getlevel <host:port> <classname> [-protocol (http|https)]\n"
      + "\t[-setlevel <host:port> <classname> <level> "
      + "[-protocol (http|https)]\n";

  public static final String PROTOCOL_HTTP = "http";
  public static final String PROTOCOL_HTTPS = "https";
  /**
   * A command line implementation
   */
  public static void main(String[] args) throws Exception {
    CLI cli = new CLI(new Configuration());
    System.exit(ToolRunner.run(cli, args));
  }

  /**
   * Valid command line options.
   */
  private enum Operations {
    GETLEVEL,
    SETLEVEL,
    UNKNOWN
  }

  private static void printUsage() {
    System.err.println(USAGES);
    GenericOptionsParser.printGenericCommandUsage(System.err);
  }

  public static boolean isValidProtocol(String protocol) {
    return ((protocol.equals(PROTOCOL_HTTP) ||
      protocol.equals(PROTOCOL_HTTPS)));
  }

  @VisibleForTesting
  static class CLI extends Configured implements Tool {
    private Operations operation = Operations.UNKNOWN;
    private String protocol;
    private String hostName;
    private String className;
    private String level;

    CLI(Configuration conf) {
      setConf(conf);
    }

    @Override
    public int run(String[] args) throws Exception {
      try {
        parseArguments(args);
        sendLogLevelRequest();
      } catch (HadoopIllegalArgumentException e) {
        printUsage();
        return -1;
      }
      return 0;
    }

    /**
     * Send HTTP/HTTPS request to the daemon.
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception if unable to connect
     */
    private void sendLogLevelRequest()
        throws HadoopIllegalArgumentException, Exception {
      switch (operation) {
      case GETLEVEL:
        doGetLevel();
        break;
      case SETLEVEL:
        doSetLevel();
        break;
      default:
        throw new HadoopIllegalArgumentException(
          "Expect either -getlevel or -setlevel");
      }
    }

    public void parseArguments(String[] args) throws
        HadoopIllegalArgumentException {
      if (args.length == 0) {
        throw new HadoopIllegalArgumentException("No arguments specified");
      }
      int nextArgIndex = 0;
      while (nextArgIndex < args.length) {
        if (args[nextArgIndex].equals("-getlevel")) {
          nextArgIndex = parseGetLevelArgs(args, nextArgIndex);
        } else if (args[nextArgIndex].equals("-setlevel")) {
          nextArgIndex = parseSetLevelArgs(args, nextArgIndex);
        } else if (args[nextArgIndex].equals("-protocol")) {
          nextArgIndex = parseProtocolArgs(args, nextArgIndex);
        } else {
          throw new HadoopIllegalArgumentException(
              "Unexpected argument " + args[nextArgIndex]);
        }
      }

      // if operation is never specified in the arguments
      if (operation == Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException(
            "Must specify either -getlevel or -setlevel");
      }

      // if protocol is unspecified, set it as http.
      if (protocol == null) {
        protocol = PROTOCOL_HTTP;
      }
    }

    private int parseGetLevelArgs(String[] args, int index) throws
        HadoopIllegalArgumentException {
      // fail if multiple operations are specified in the arguments
      if (operation != Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException(
            "Redundant -getlevel command");
      }
      // check number of arguments is sufficient
      if (index+2 >= args.length) {
        throw new HadoopIllegalArgumentException(
            "-getlevel needs two parameters");
      }
      operation = Operations.GETLEVEL;
      hostName = args[index+1];
      className = args[index+2];
      return index+3;
    }

    private int parseSetLevelArgs(String[] args, int index) throws
        HadoopIllegalArgumentException {
      // fail if multiple operations are specified in the arguments
      if (operation != Operations.UNKNOWN) {
        throw new HadoopIllegalArgumentException(
            "Redundant -setlevel command");
      }
      // check number of arguments is sufficient
      if (index+3 >= args.length) {
        throw new HadoopIllegalArgumentException(
            "-setlevel needs three parameters");
      }
      operation = Operations.SETLEVEL;
      hostName = args[index+1];
      className = args[index+2];
      level = args[index+3];
      return index+4;
    }

    private int parseProtocolArgs(String[] args, int index) throws
        HadoopIllegalArgumentException {
      // make sure only -protocol is specified
      if (protocol != null) {
        throw new HadoopIllegalArgumentException(
            "Redundant -protocol command");
      }
      // check number of arguments is sufficient
      if (index+1 >= args.length) {
        throw new HadoopIllegalArgumentException(
            "-protocol needs one parameter");
      }
      // check protocol is valid
      protocol = args[index+1];
      if (!isValidProtocol(protocol)) {
        throw new HadoopIllegalArgumentException(
            "Invalid protocol: " + protocol);
      }
      return index+2;
    }

    /**
     * Send HTTP/HTTPS request to get log level.
     *
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception if unable to connect
     */
    private void doGetLevel() throws Exception {
      process(protocol + "://" + hostName + "/logLevel?log=" + className);
    }

    /**
     * Send HTTP/HTTPS request to set log level.
     *
     * @throws HadoopIllegalArgumentException if arguments are invalid.
     * @throws Exception if unable to connect
     */
    private void doSetLevel() throws Exception {
      process(protocol + "://" + hostName + "/logLevel?log=" + className
          + "&level=" + level);
    }

    /**
     * Connect to the URL. Supports HTTP/HTTPS and supports SPNEGO
     * authentication. It falls back to simple authentication if it fails to
     * initiate SPNEGO.
     *
     * @param url the URL address of the daemon servlet
     * @return a connected connection
     * @throws Exception if it can not establish a connection.
     */
    private URLConnection connect(URL url) throws Exception {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      AuthenticatedURL aUrl;
      SSLFactory clientSslFactory;
      URLConnection connection;
      // If https is chosen, configures SSL client.
      if (PROTOCOL_HTTPS.equals(url.getProtocol())) {
        clientSslFactory = new SSLFactory(
            SSLFactory.Mode.CLIENT, this.getConf());
        clientSslFactory.init();
        SSLSocketFactory sslSocketF = clientSslFactory.createSSLSocketFactory();

        aUrl = new AuthenticatedURL(
            new KerberosAuthenticator(), clientSslFactory);
        connection = aUrl.openConnection(url, token);
        HttpsURLConnection httpsConn = (HttpsURLConnection) connection;
        httpsConn.setSSLSocketFactory(sslSocketF);
      } else {
        aUrl = new AuthenticatedURL(new KerberosAuthenticator());
        connection = aUrl.openConnection(url, token);
      }

      connection.connect();
      return connection;
    }

    /**
     * Configures the client to send HTTP/HTTPS request to the URL.
     * Supports SPENGO for authentication.
     * @param urlString URL and query string to the daemon's web UI
     * @throws Exception if unable to connect
     */
    private void process(String urlString) throws Exception {
      URL url = new URL(urlString);
      System.out.println("Connecting to " + url);

      URLConnection connection = connect(url);

      // read from the servlet
      BufferedReader in = new BufferedReader(
          new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
      for (String line;;) {
        line = in.readLine();
        if (line == null) {
          break;
        }
        if (line.startsWith(MARKER)) {
          System.out.println(TAG.matcher(line).replaceAll(""));
        }
      }
      in.close();
    }
  }

  static final String MARKER = "<!-- OUTPUT -->";
  static final Pattern TAG = Pattern.compile("<[^>]*>");

  /**
   * A servlet implementation
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Unstable
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response
        ) throws ServletException, IOException {

      // If user is a static user and auth Type is null, that means
      // there is a non-security environment and no need authorization,
      // otherwise, do the authorization.
      final ServletContext servletContext = getServletContext();
      if (!HttpServer2.isStaticUserAndNoneAuthType(servletContext, request) &&
          !HttpServer2.hasAdministratorAccess(servletContext,
              request, response)) {
        return;
      }

      PrintWriter out = ServletUtil.initHTML(response, "Log Level");
      String logName = ServletUtil.getParameter(request, "log");
      String level = ServletUtil.getParameter(request, "level");

      if (logName != null) {
        out.println("<br /><hr /><h3>Results</h3>");
        out.println(MARKER
            + "Submitted Class Name: <b>" + logName + "</b><br />");

        Log log = LogFactory.getLog(logName);
        out.println(MARKER
            + "Log Class: <b>" + log.getClass().getName() +"</b><br />");
        if (level != null) {
          out.println(MARKER + "Submitted Level: <b>" + level + "</b><br />");
        }

        if (log instanceof Log4JLogger) {
          process(((Log4JLogger)log).getLogger(), level, out);
        }
        else if (log instanceof Jdk14Logger) {
          process(((Jdk14Logger)log).getLogger(), level, out);
        }
        else {
          out.println("Sorry, " + log.getClass() + " not supported.<br />");
        }
      }

      out.println(FORMS);
      out.println(ServletUtil.HTML_TAIL);
    }

    static final String FORMS = "\n<br /><hr /><h3>Get / Set</h3>"
        + "\n<form>Class Name: <input type='text' size='50' name='log' /> "
        + "<input type='submit' value='Get Log Level' />"
        + "</form>"
        + "\n<form>Class Name: <input type='text' size='50' name='log' /> "
        + "Level: <input type='text' name='level' /> "
        + "<input type='submit' value='Set Log Level' />"
        + "</form>";

    private static void process(org.apache.log4j.Logger log, String level,
        PrintWriter out) throws IOException {
      if (level != null) {
        if (!level.equalsIgnoreCase(org.apache.log4j.Level.toLevel(level)
            .toString())) {
          out.println(MARKER + "Bad Level : <b>" + level + "</b><br />");
        } else {
          log.setLevel(org.apache.log4j.Level.toLevel(level));
          out.println(MARKER + "Setting Level to " + level + " ...<br />");
        }
      }
      out.println(MARKER
          + "Effective Level: <b>" + log.getEffectiveLevel() + "</b><br />");
    }

    private static void process(java.util.logging.Logger log, String level,
        PrintWriter out) throws IOException {
      if (level != null) {
        String levelToUpperCase = level.toUpperCase();
        try {
          log.setLevel(java.util.logging.Level.parse(levelToUpperCase));
        } catch (IllegalArgumentException e) {
          out.println(MARKER + "Bad Level : <b>" + level + "</b><br />");
        }
        out.println(MARKER + "Setting Level to " + level + " ...<br />");
      }

      java.util.logging.Level lev;
      for(; (lev = log.getLevel()) == null; log = log.getParent());
      out.println(MARKER + "Effective Level: <b>" + lev + "</b><br />");
    }
  }
}
