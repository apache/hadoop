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
package org.apache.hadoop.hdfs.security.token.delegation;

import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.util.IOUtilsClient;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;

@InterfaceAudience.Private
public class DelegationUtilsClient {
  public static final Logger LOG = LoggerFactory.getLogger(
      DelegationUtilsClient.class);

  public static final String STARTUP_PROGRESS_PATH_SPEC = "/startupProgress";
  public static final String GET_DELEGATION_TOKEN_PATH_SPEC = "/getDelegationToken";
  public static final String RENEW_DELEGATION_TOKEN_PATH_SPEC = "/renewDelegationToken";
  public static final String CANCEL_DELEGATION_TOKEN_PATH_SPEC = "/cancelDelegationToken";
  public static final String TOKEN = "token";
  public static final String RENEWER = "renewer";

  public static final String DELEGATION_PARAMETER_NAME = DelegationParam.NAME;
  private static final String SET_DELEGATION = "&" + DELEGATION_PARAMETER_NAME + "=";

  static public Credentials getDTfromRemote(URLConnectionFactory factory,
      URI nnUri, String renewer, String proxyUser) throws IOException {
    StringBuilder buf = new StringBuilder(nnUri.toString())
        .append(GET_DELEGATION_TOKEN_PATH_SPEC);
    String separator = "?";
    if (renewer != null) {
      buf.append("?").append(RENEWER).append("=")
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
      LOG.debug("Retrieving token from: {}", buf);

      conn = run(factory, new URL(buf.toString()));
      InputStream in = conn.getInputStream();
      Credentials ts = new Credentials();
      dis = new DataInputStream(in);
      ts.readFields(dis);
      for (Token<?> token : ts.getAllTokens()) {
        token.setKind(isHttps ? WebHdfsConstants.HSFTP_TOKEN_KIND :
            WebHdfsConstants.HFTP_TOKEN_KIND);
        SecurityUtil.setTokenService(token, serviceAddr);
      }
      return ts;
    } catch (Exception e) {
      throw new IOException("Unable to obtain remote token", e);
    } finally {
      IOUtilsClient.cleanup(LOG, dis);
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
        .append(CANCEL_DELEGATION_TOKEN_PATH_SPEC).append("?")
        .append(TOKEN).append("=")
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
        .append(RENEW_DELEGATION_TOKEN_PATH_SPEC).append("?")
        .append(TOKEN).append("=")
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
      IOUtilsClient.cleanup(LOG, in);
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

  /**
   * Returns the url parameter for the given token string.
   * @param tokenString
   * @return url parameter
   */
  public static String getDelegationTokenUrlParam(String tokenString) {
    if (tokenString == null ) {
      return "";
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      return SET_DELEGATION + tokenString;
    } else {
      return "";
    }
  }
}
