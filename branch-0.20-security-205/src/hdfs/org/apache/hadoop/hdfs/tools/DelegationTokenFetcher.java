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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
public class DelegationTokenFetcher {
  
  private static final Log LOG = 
    LogFactory.getLog(DelegationTokenFetcher.class);
  private static final String WEBSERVICE = "webservice";
  private static final String CANCEL = "cancel";
  private static final String RENEW = "renew";
  
  static {
    // Enable Kerberos sockets
    System.setProperty("https.cipherSuites", "TLS_KRB5_WITH_3DES_EDE_CBC_SHA");
  }

  private static void printUsage(PrintStream err) throws IOException {
    err.println("fetchdt retrieves delegation tokens from the NameNode");
    err.println();
    err.println("fetchdt <opts> <token file>");
    err.println("Options:");
    err.println("  --webservice <url>  Url to contact NN on");
    err.println("  --cancel            Cancel the delegation token");
    err.println("  --renew             Renew the delegation token");
    err.println();
    GenericOptionsParser.printGenericCommandUsage(err);
    System.exit(1);
  }

  private static Collection<Token<?>>
    readTokens(Path file, Configuration conf) throws IOException{
    Credentials creds = Credentials.readTokenStorageFile(file, conf);
    return creds.getAllTokens();
  }

  /**
   * Command-line interface
   */
  public static void main(final String [] args) throws Exception {
    final Configuration conf = new Configuration();
    Options fetcherOptions = new Options();
    fetcherOptions.addOption(WEBSERVICE, true, 
                             "HTTPS url to reach the NameNode at");
    fetcherOptions.addOption(CANCEL, false, "cancel the token");
    fetcherOptions.addOption(RENEW, false, "renew the token");
    GenericOptionsParser parser =
      new GenericOptionsParser(conf, fetcherOptions, args);
    CommandLine cmd = parser.getCommandLine();
    
    // get options
    final String webUrl =
      cmd.hasOption(WEBSERVICE) ? cmd.getOptionValue(WEBSERVICE) : null;
    final boolean cancel = cmd.hasOption(CANCEL);
    final boolean renew = cmd.hasOption(RENEW);
    String[] remaining = parser.getRemainingArgs();
    
    // check option validity
    if (cancel && renew) {
      System.err.println("ERROR: Only specify cancel or renew.");
      printUsage(System.err);
    }
    if (remaining.length != 1 || remaining[0].charAt(0) == '-') {
      System.err.println("ERROR: Must specify exactly one token file");
      printUsage(System.err);
    }
    // default to using the local file system
    FileSystem local = FileSystem.getLocal(conf);
    final Path tokenFile = new Path(local.getWorkingDirectory(), remaining[0]);

    // Login the current user
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        
        if (cancel) {
          for(Token<?> token: readTokens(tokenFile, conf)) {
            if (token.isManaged()) {
              token.cancel(conf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Cancelled token for " + token.getService());
              }
            }
          }          
        } else if (renew) {
          for(Token<?> token: readTokens(tokenFile, conf)) {
            if (token.isManaged()) {
              token.renew(conf);
              if(LOG.isDebugEnabled()) {
                LOG.debug("Renewed token for " + token.getService());
              }
            }
          }          
        } else {
          if (webUrl != null) {
            getDTfromRemote(webUrl, null).
              writeTokenStorageFile(tokenFile, conf);
            if(LOG.isDebugEnabled()) {
              LOG.debug("Fetched token via http for " + webUrl);
            }
          } else {
            FileSystem fs = FileSystem.get(conf);
            Token<?> token = fs.getDelegationToken(ugi.getShortUserName());
            Credentials cred = new Credentials();
            cred.addToken(token.getService(), token);
            cred.writeTokenStorageFile(tokenFile, conf);
            if(LOG.isDebugEnabled()) {
              LOG.debug("Fetched token for " + fs.getUri() + " into " +
                               tokenFile);
            }
          }        
        }
        return null;
      }
    });

  }
  
  /**
   * Utility method to obtain a delegation token over http
   * @param nnHttpAddr Namenode http addr, such as http://namenode:50070
   */
  static public Credentials getDTfromRemote(String nnAddr, 
                                            String renewer) throws IOException {
    DataInputStream dis = null;
    InetSocketAddress serviceAddr = NetUtils.createSocketAddr(nnAddr);

    try {
      StringBuffer url = new StringBuffer();
      if (renewer != null) {
        url.append(nnAddr).append(GetDelegationTokenServlet.PATH_SPEC).append("?").
        append(GetDelegationTokenServlet.RENEWER).append("=").append(renewer);
      } else {
        url.append(nnAddr).append(GetDelegationTokenServlet.PATH_SPEC);
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Retrieving token from: " + url);
      }
      URL remoteURL = new URL(url.toString());
      SecurityUtil.fetchServiceTicket(remoteURL);
      URLConnection connection = remoteURL.openConnection();

      InputStream in = connection.getInputStream();
      Credentials ts = new Credentials();
      dis = new DataInputStream(in);
      ts.readFields(dis);
      for(Token<?> token: ts.getAllTokens()) {
        token.setKind(HftpFileSystem.TOKEN_KIND);
        SecurityUtil.setTokenService(token, serviceAddr);
      }
      return ts;
    } catch (Exception e) {
      throw new IOException("Unable to obtain remote token", e);
    } finally {
      if(dis != null) dis.close();
    }
  }
  
  /**
   * Renew a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to renew
   * @return the Date that the token will expire next.
   * @throws IOException
   */
  static public long renewDelegationToken(String nnAddr,
                                          Token<DelegationTokenIdentifier> tok
                                          ) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(nnAddr);
    buf.append(RenewDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(RenewDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    BufferedReader in = null;
    HttpURLConnection connection = null;
    try {
      URL url = new URL(buf.toString());
      SecurityUtil.fetchServiceTicket(url);
      connection = (HttpURLConnection)url.openConnection();
      in = new BufferedReader(new InputStreamReader
                              (connection.getInputStream()));
      long result = Long.parseLong(in.readLine());
      in.close();
      return result;
    } catch (IOException ie) {
      LOG.info("error in renew over HTTP", ie);
      IOException e = null;
      if(connection != null) {
        String resp = connection.getResponseMessage();
        e = getExceptionFromResponse(resp);
      }
      
      IOUtils.cleanup(LOG, in);
      if(e!=null) {
        LOG.info("rethrowing exception from HTTP request: " + e.getLocalizedMessage());
        throw e;
      }
      throw ie;
    }
  }

  static private IOException getExceptionFromResponse(String resp) {
    String exceptionClass = "", exceptionMsg = "";
    if(resp != null && !resp.isEmpty()) {
      String[] rs = resp.split(";");
      exceptionClass = rs[0];
      exceptionMsg = rs[1];
    }
    LOG.info("Error response from HTTP request=" + resp + 
        ";ec=" + exceptionClass + ";em="+exceptionMsg);
    IOException e = null;
    if(exceptionClass != null  && !exceptionClass.isEmpty()) {
      if(exceptionClass.contains("InvalidToken")) {
        e = new org.apache.hadoop.security.token.SecretManager.InvalidToken(exceptionMsg);
        e.setStackTrace(new StackTraceElement[0]); // stack is not relevant
      } else if(exceptionClass.contains("AccessControlException")) {
        e = new org.apache.hadoop.security.AccessControlException(exceptionMsg);
        e.setStackTrace(new StackTraceElement[0]); // stack is not relevant
      }
    }
    LOG.info("Exception from HTTP response=" + e.getLocalizedMessage());
    return e;
  }
  
  /**
   * Cancel a Delegation Token.
   * @param nnAddr the NameNode's address
   * @param tok the token to cancel
   * @throws IOException
   */
  static public void cancelDelegationToken(String nnAddr,
                                           Token<DelegationTokenIdentifier> tok
                                           ) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(nnAddr);
    buf.append(CancelDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(CancelDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    BufferedReader in = null;
    try {
      URL url = new URL(buf.toString());
      SecurityUtil.fetchServiceTicket(url);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException("Error cancelling token:" + 
                              connection.getResponseMessage());
      }
    } catch (IOException ie) {
      IOUtils.cleanup(LOG, in);
      throw ie;
    }
  }
}
