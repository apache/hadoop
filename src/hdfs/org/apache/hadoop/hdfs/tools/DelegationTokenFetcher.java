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
import java.net.URI;
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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HftpFileSystem;
import org.apache.hadoop.hdfs.HsftpFileSystem;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.CancelDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.GetDelegationTokenServlet;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.RenewDelegationTokenServlet;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.Krb5AndCertsSslSocketConnector;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
public class DelegationTokenFetcher {
  
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  private static final Log LOG = 
    LogFactory.getLog(DelegationTokenFetcher.class);
  private static final String WEBSERVICE = "webservice";
  private static final String CANCEL = "cancel";
  private static final String RENEW = "renew";

  static {
    // reference a field to make sure the static blocks run
    int x = Krb5AndCertsSslSocketConnector.KRB5_CIPHER_SUITES.size();
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
    setupSsl(conf);
    Options fetcherOptions = new Options();
    fetcherOptions.addOption(WEBSERVICE, true, 
                             "HTTP/S url to reach the NameNode at");
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

    if (cancel) {
      for(Token<?> token: readTokens(tokenFile, conf)) {
        if (token.isManaged()) {
          token.cancel(conf);
        }
      }          
    } else if (renew) {
      for(Token<?> token: readTokens(tokenFile, conf)) {
        if (token.isManaged()) {
          token.renew(conf);
        }
      }          
    } else {
      if (webUrl != null) {
        URI uri = new URI(webUrl);
        getDTfromRemote(uri.getScheme(), 
                        new InetSocketAddress(uri.getHost(), uri.getPort()),
                        null,
                        conf).
          writeTokenStorageFile(tokenFile, conf);
      } else {
        FileSystem fs = FileSystem.get(conf);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        Token<?> token = fs.getDelegationToken(ugi.getShortUserName());
        Credentials cred = new Credentials();
        cred.addToken(token.getService(), token);
        cred.writeTokenStorageFile(tokenFile, conf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Fetched token for " + fs.getUri() + " into " +
                    tokenFile);
        }
      }        
    }
  }
  
  /** Set up SSL resources */
  public static void setupSsl(Configuration conf) {
    Configuration sslConf = new Configuration(false);
    sslConf.addResource(conf.get("dfs.https.client.keystore.resource",
        "ssl-client.xml"));
    System.setProperty("javax.net.ssl.trustStore", sslConf.get(
        "ssl.client.truststore.location", ""));
    System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
        "ssl.client.truststore.password", ""));
    System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
        "ssl.client.truststore.type", "jks"));
    System.setProperty("javax.net.ssl.keyStore", sslConf.get(
        "ssl.client.keystore.location", ""));
    System.setProperty("javax.net.ssl.keyStorePassword", sslConf.get(
        "ssl.client.keystore.password", ""));
    System.setProperty("javax.net.ssl.keyPassword", sslConf.get(
        "ssl.client.keystore.keypassword", ""));
    System.setProperty("javax.net.ssl.keyStoreType", sslConf.get(
        "ssl.client.keystore.type", "jks"));
  }

  /**
   * Utility method to obtain a delegation token over http
   * @param protocol whether to use http or https
   * @param nnAddr the address for the NameNode
   * @param renewer User that is renewing the ticket in such a request
   * @param conf the configuration
   */
  static public Credentials getDTfromRemote(String protocol,
                                            final InetSocketAddress nnAddr,
                                            String renewer,
                                            Configuration conf
                                            ) throws IOException {
    final String renewAddress = getRenewAddress(protocol, nnAddr, conf);
    final boolean https = "https".equals(protocol);

    try {
      StringBuffer url = new StringBuffer(renewAddress);
      url.append(GetDelegationTokenServlet.PATH_SPEC);
      if (renewer != null) {
        url.append("?").
          append(GetDelegationTokenServlet.RENEWER).append("=").
          append(renewer);
      }
      if(LOG.isDebugEnabled()) {
        LOG.debug("Retrieving token from: " + url);
      }
      final URL remoteURL = new URL(url.toString());
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      return ugi.doAs(new PrivilegedExceptionAction<Credentials>(){
        public Credentials run() throws Exception {
          URLConnection connection = 
            SecurityUtil.openSecureHttpConnection(remoteURL);

          InputStream in = connection.getInputStream();
          Credentials ts = new Credentials();
          DataInputStream dis = new DataInputStream(in);
          try {
            ts.readFields(dis);
            for(Token<?> token: ts.getAllTokens()) {
              if (https) {
                token.setKind(HsftpFileSystem.TOKEN_KIND);
              } else {
                token.setKind(HftpFileSystem.TOKEN_KIND);
              }
              SecurityUtil.setTokenService(token, nnAddr);
            }
            dis.close();
          } catch (IOException ie) {
            IOUtils.cleanup(LOG, dis);
          }
          return ts;
        }
      });
    } catch (InterruptedException ie) {
      return null;
    }
  }
  
  /**
   * Get the URI that we use for getting, renewing, and cancelling the
   * delegation token. For KSSL with hftp that means we need to use
   * https and the NN's https port.
   */
  protected static String getRenewAddress(String protocol,
                                          InetSocketAddress addr,
                                          Configuration conf) {
    if (SecurityUtil.useKsslAuth() && "http".equals(protocol)) {
      protocol = "https";
      int port = 
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_KEY,
                    DFSConfigKeys.DFS_NAMENODE_HTTPS_PORT_DEFAULT);
      addr = new InetSocketAddress(addr.getAddress(), port);
    }
    return DFSUtil.createUri(protocol, addr).toString();
  }

  /**
   * Renew a Delegation Token.
   * @param protocol The protocol to renew over (http or https)
   * @param addr the address of the NameNode
   * @param tok the token to renew
   * @param conf the configuration
   * @return the Date that the token will expire next.
   * @throws IOException
   */
  static public long renewDelegationToken(String protocol,
                                          InetSocketAddress addr,
                                          Token<DelegationTokenIdentifier> tok,
                                          Configuration conf
                                          ) throws IOException {
    final String renewAddress = getRenewAddress(protocol, addr, conf);
    final StringBuilder buf = new StringBuilder(renewAddress);
    final String service = tok.getService().toString();
    buf.append(RenewDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(RenewDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    try {
      return ugi.doAs(new PrivilegedExceptionAction<Long>(){
        public Long run() throws Exception {
          BufferedReader in = null;
          HttpURLConnection connection = null;
          try {
            URL url = new URL(buf.toString());
            connection = 
              (HttpURLConnection) SecurityUtil.openSecureHttpConnection(url);
            in = new BufferedReader(new InputStreamReader
                                    (connection.getInputStream()));
            long result = Long.parseLong(in.readLine());
            in.close();
            if (LOG.isDebugEnabled()) {
              LOG.debug("Renewed token for " + service + " via " + 
                        renewAddress);
            }
            return result;
          } catch (IOException ie) {
            LOG.info("Error renewing token for " + renewAddress, ie);
            IOException e = null;
            if(connection != null) {
              String resp = connection.getResponseMessage();
              e = getExceptionFromResponse(resp);
            }
      
            IOUtils.cleanup(LOG, in);
            if (e!=null) {
              LOG.info("rethrowing exception from HTTP request: " + 
                       e.getLocalizedMessage());
              throw e;
            }
            throw ie;
          }
        }
        });
    } catch (InterruptedException ie) {
      return 0;
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
  static public void cancelDelegationToken(String protocol,
                                           InetSocketAddress addr,
                                           Token<DelegationTokenIdentifier> tok,
                                           Configuration conf
                                           ) throws IOException {
    final String renewAddress = getRenewAddress(protocol, addr, conf);
    StringBuilder buf = new StringBuilder(renewAddress);
    buf.append(CancelDelegationTokenServlet.PATH_SPEC);
    buf.append("?");
    buf.append(CancelDelegationTokenServlet.TOKEN);
    buf.append("=");
    buf.append(tok.encodeToUrlString());
    BufferedReader in = null;
    try {
      final URL url = new URL(buf.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("cancelling token at " + buf.toString());
      }
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      ugi.doAs(new PrivilegedExceptionAction<Void>(){
          public Void run() throws Exception {
            HttpURLConnection connection =
              (HttpURLConnection)SecurityUtil.openSecureHttpConnection(url);
            if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
              throw new IOException("Error cancelling token for " + 
                                    renewAddress + " response: " +
                                    connection.getResponseMessage());
            }
            return null;
          }
        });
      if (LOG.isDebugEnabled()) {
        LOG.debug("Cancelled token for " + tok.getService() + " via " + 
                  renewAddress);
      }
    } catch (IOException ie) {
      LOG.warn("Error cancelling token for " + renewAddress, ie);
      IOUtils.cleanup(LOG, in);
      throw ie;
    } catch (InterruptedException ie) {
      // PASS
    }
  }
}
