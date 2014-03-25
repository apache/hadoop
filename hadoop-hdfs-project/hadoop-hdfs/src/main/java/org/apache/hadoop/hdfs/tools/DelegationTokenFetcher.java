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
package org.apache.hadoop.hdfs.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.hdfs.web.HftpFileSystem;
import org.apache.hadoop.hdfs.web.HsftpFileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Charsets;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
@InterfaceAudience.Private
public class DelegationTokenFetcher {
  private static final Log LOG = 
    LogFactory.getLog(DelegationTokenFetcher.class);
  private static final String WEBSERVICE = "webservice";
  private static final String RENEWER = "renewer";
  private static final String CANCEL = "cancel";
  private static final String RENEW = "renew";
  private static final String PRINT = "print";
  private static final String HELP = "help";
  private static final String HELP_SHORT = "h";

  private static void printUsage(PrintStream err) {
    err.println("fetchdt retrieves delegation tokens from the NameNode");
    err.println();
    err.println("fetchdt <opts> <token file>");
    err.println("Options:");
    err.println("  --webservice <url>  Url to contact NN on");
    err.println("  --renewer <name>    Name of the delegation token renewer");
    err.println("  --cancel            Cancel the delegation token");
    err.println("  --renew             Renew the delegation token.  Delegation " 
    		+ "token must have been fetched using the --renewer <name> option.");
    err.println("  --print             Print the delegation token");
    err.println();
    GenericOptionsParser.printGenericCommandUsage(err);
    ExitUtil.terminate(1);    
  }

  private static Collection<Token<?>> readTokens(Path file, Configuration conf)
      throws IOException {
    Credentials creds = Credentials.readTokenStorageFile(file, conf);
    return creds.getAllTokens();
  }
    
  /**
   * Command-line interface
   */
  public static void main(final String[] args) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    Options fetcherOptions = new Options();
    fetcherOptions.addOption(WEBSERVICE, true,
        "HTTP url to reach the NameNode at");
    fetcherOptions.addOption(RENEWER, true,
        "Name of the delegation token renewer");
    fetcherOptions.addOption(CANCEL, false, "cancel the token");
    fetcherOptions.addOption(RENEW, false, "renew the token");
    fetcherOptions.addOption(PRINT, false, "print the token");
    fetcherOptions.addOption(HELP_SHORT, HELP, false, "print out help information");
    GenericOptionsParser parser = new GenericOptionsParser(conf,
        fetcherOptions, args);
    CommandLine cmd = parser.getCommandLine();
    
    // get options
    final String webUrl = cmd.hasOption(WEBSERVICE) ? cmd
        .getOptionValue(WEBSERVICE) : null;
    final String renewer = cmd.hasOption(RENEWER) ? 
        cmd.getOptionValue(RENEWER) : null;
    final boolean cancel = cmd.hasOption(CANCEL);
    final boolean renew = cmd.hasOption(RENEW);
    final boolean print = cmd.hasOption(PRINT);
    final boolean help = cmd.hasOption(HELP);
    String[] remaining = parser.getRemainingArgs();

    // check option validity
    if (help) {
      printUsage(System.out);
      System.exit(0);
    }
    if (cancel && renew || cancel && print || renew && print || cancel && renew
        && print) {
      System.err.println("ERROR: Only specify cancel, renew or print.");
      printUsage(System.err);
    }
    if (remaining.length != 1 || remaining[0].charAt(0) == '-') {
      System.err.println("ERROR: Must specify exacltly one token file");
      printUsage(System.err);
    }
    // default to using the local file system
    FileSystem local = FileSystem.getLocal(conf);
    final Path tokenFile = new Path(local.getWorkingDirectory(), remaining[0]);
    final URLConnectionFactory connectionFactory = URLConnectionFactory.DEFAULT_SYSTEM_CONNECTION_FACTORY;

    // Login the current user
    UserGroupInformation.getCurrentUser().doAs(
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            if (print) {
              DelegationTokenIdentifier id = new DelegationTokenSecretManager(
                  0, 0, 0, 0, null).createIdentifier();
              for (Token<?> token : readTokens(tokenFile, conf)) {
                DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(token.getIdentifier()));
                id.readFields(in);
                System.out.println("Token (" + id + ") for " + token.getService());
              }
              return null;
            }
            
            if (renew) {
              for (Token<?> token : readTokens(tokenFile, conf)) {
                if (token.isManaged()) {
                  long result = token.renew(conf);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Renewed token for " + token.getService()
                        + " until: " + new Date(result));
                  }
                }
              }
            } else if (cancel) {
              for(Token<?> token: readTokens(tokenFile, conf)) {
                if (token.isManaged()) {
                  token.cancel(conf);
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Cancelled token for " + token.getService());
                  }
                }
              }
            } else {
              // otherwise we are fetching
              if (webUrl != null) {
                Credentials creds = getDTfromRemote(connectionFactory, new URI(
                    webUrl), renewer, null);
                creds.writeTokenStorageFile(tokenFile, conf);
                for (Token<?> token : creds.getAllTokens()) {
                  System.out.println("Fetched token via " + webUrl + " for "
                      + token.getService() + " into " + tokenFile);
                }
              } else {
                FileSystem fs = FileSystem.get(conf);
                Credentials cred = new Credentials();
                Token<?> tokens[] = fs.addDelegationTokens(renewer, cred);
                cred.writeTokenStorageFile(tokenFile, conf);
                for (Token<?> token : tokens) {
                  System.out.println("Fetched token for " + token.getService()
                      + " into " + tokenFile);
                }
              }
            }
            return null;
          }
        });
  }
  
  static public Credentials getDTfromRemote(URLConnectionFactory factory,
      URI nnUri, String renewer, String proxyUser) throws IOException {
    StringBuilder buf = new StringBuilder(nnUri.toString())
        .append(GetDelegationTokenServlet.PATH_SPEC);
    String separator = "?";
    if (renewer != null) {
      buf.append("?").append(GetDelegationTokenServlet.RENEWER).append("=")
          .append(renewer);
      separator = "&";
    }
    if (proxyUser != null) {
      buf.append(separator).append("doas=").append(proxyUser);
    }

    boolean isHttps = nnUri.getScheme().equals("https");

    HttpURLConnection conn = null;
    DataInputStream dis = null;
    InetSocketAddress serviceAddr = NetUtils.createSocketAddr(nnUri
        .getAuthority());

    try {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Retrieving token from: " + buf);
      }

      conn = run(factory, new URL(buf.toString()));
      InputStream in = conn.getInputStream();
      Credentials ts = new Credentials();
      dis = new DataInputStream(in);
      ts.readFields(dis);
      for (Token<?> token : ts.getAllTokens()) {
        token.setKind(isHttps ? HsftpFileSystem.TOKEN_KIND : HftpFileSystem.TOKEN_KIND);
        SecurityUtil.setTokenService(token, serviceAddr);
      }
      return ts;
    } catch (Exception e) {
      throw new IOException("Unable to obtain remote token", e);
    } finally {
      IOUtils.cleanup(LOG, dis);
      if (conn != null) {
        conn.disconnect();
      }
    }
  }

  /**
   * Cancel a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to cancel
   * @throws IOException
   * @throws AuthenticationException
   */
  static public void cancelDelegationToken(URLConnectionFactory factory,
      URI nnAddr, Token<DelegationTokenIdentifier> tok) throws IOException,
      AuthenticationException {
    StringBuilder buf = new StringBuilder(nnAddr.toString())
        .append(CancelDelegationTokenServlet.PATH_SPEC).append("?")
        .append(CancelDelegationTokenServlet.TOKEN).append("=")
        .append(tok.encodeToUrlString());
    HttpURLConnection conn = run(factory, new URL(buf.toString()));
    conn.disconnect();
  }

  /**
   * Renew a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to renew
   * @return the Date that the token will expire next.
   * @throws IOException
   * @throws AuthenticationException
   */
  static public long renewDelegationToken(URLConnectionFactory factory,
      URI nnAddr, Token<DelegationTokenIdentifier> tok) throws IOException,
      AuthenticationException {
    StringBuilder buf = new StringBuilder(nnAddr.toString())
        .append(RenewDelegationTokenServlet.PATH_SPEC).append("?")
        .append(RenewDelegationTokenServlet.TOKEN).append("=")
        .append(tok.encodeToUrlString());

    HttpURLConnection connection = null;
    BufferedReader in = null;
    try {
      connection = run(factory, new URL(buf.toString()));
      in = new BufferedReader(new InputStreamReader(
          connection.getInputStream(), Charsets.UTF_8));
      long result = Long.parseLong(in.readLine());
      return result;
    } catch (IOException ie) {
      LOG.info("error in renew over HTTP", ie);
      IOException e = getExceptionFromResponse(connection);

      if (e != null) {
        LOG.info("rethrowing exception from HTTP request: "
            + e.getLocalizedMessage());
        throw e;
      }
      throw ie;
    } finally {
      IOUtils.cleanup(LOG, in);
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  // parse the message and extract the name of the exception and the message
  static private IOException getExceptionFromResponse(HttpURLConnection con) {
    IOException e = null;
    String resp;
    if(con == null) 
      return null;    
    
    try {
      resp = con.getResponseMessage();
    } catch (IOException ie) { return null; }
    if(resp == null || resp.isEmpty())
      return null;

    String exceptionClass = "", exceptionMsg = "";
    String[] rs = resp.split(";");
    if(rs.length < 2)
      return null;
    exceptionClass = rs[0];
    exceptionMsg = rs[1];
    LOG.info("Error response from HTTP request=" + resp + 
        ";ec=" + exceptionClass + ";em="+exceptionMsg);
    
    if(exceptionClass == null || exceptionClass.isEmpty())
      return null;
    
    // recreate exception objects
    try {
      Class<? extends Exception> ec = 
         Class.forName(exceptionClass).asSubclass(Exception.class);
      // we are interested in constructor with String arguments
      java.lang.reflect.Constructor<? extends Exception> constructor =
          ec.getConstructor (new Class[] {String.class});

      // create an instance
      e =  (IOException) constructor.newInstance (exceptionMsg);

    } catch (Exception ee)  {
      LOG.warn("failed to create object of this class", ee);
    }
    if(e == null)
      return null;
    
    e.setStackTrace(new StackTraceElement[0]); // local stack is not relevant
    LOG.info("Exception from HTTP response=" + e.getLocalizedMessage());
    return e;
  }

  private static HttpURLConnection run(URLConnectionFactory factory, URL url)
      throws IOException, AuthenticationException {
    HttpURLConnection conn = null;

    try {
      conn = (HttpURLConnection) factory.openConnection(url, true);
      if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        String msg = conn.getResponseMessage();

        throw new IOException("Error when dealing remote token: " + msg);
      }
    } catch (IOException ie) {
      LOG.info("Error when dealing remote token:", ie);
      IOException e = getExceptionFromResponse(conn);

      if (e != null) {
        LOG.info("rethrowing exception from HTTP request: "
            + e.getLocalizedMessage());
        throw e;
      }
      throw ie;
    }
    return conn;
  }
}
