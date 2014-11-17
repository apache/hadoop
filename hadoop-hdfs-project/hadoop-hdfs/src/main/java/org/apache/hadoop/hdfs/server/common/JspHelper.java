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

package org.apache.hadoop.hdfs.server.common;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeHttpServer;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authorize.ProxyServers;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER;
import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER;

@InterfaceAudience.Private
public class JspHelper {
  public static final String CURRENT_CONF = "current.conf";
  public static final String DELEGATION_PARAMETER_NAME = DelegationParam.NAME;
  public static final String NAMENODE_ADDRESS = "nnaddr";
  static final String SET_DELEGATION = "&" + DELEGATION_PARAMETER_NAME +
                                              "=";
  private static final Log LOG = LogFactory.getLog(JspHelper.class);

  /** Private constructor for preventing creating JspHelper object. */
  private JspHelper() {}

  // data structure to count number of blocks on datanodes.
  private static class NodeRecord extends DatanodeInfo {
    int frequency;

    public NodeRecord(DatanodeInfo info, int count) {
      super(info);
      this.frequency = count;
    }

    @Override
    public boolean equals(Object obj) {
      // Sufficient to use super equality as datanodes are uniquely identified
      // by DatanodeID
      return (this == obj) || super.equals(obj);
    }
    @Override
    public int hashCode() {
      // Super implementation is sufficient
      return super.hashCode();
    }
  }

  // compare two records based on their frequency
  private static class NodeRecordComparator implements Comparator<NodeRecord> {

    @Override
    public int compare(NodeRecord o1, NodeRecord o2) {
      if (o1.frequency < o2.frequency) {
        return -1;
      } else if (o1.frequency > o2.frequency) {
        return 1;
      }
      return 0;
    }
  }

  public static DatanodeInfo bestNode(LocatedBlocks blks, Configuration conf)
      throws IOException {
    HashMap<DatanodeInfo, NodeRecord> map =
      new HashMap<DatanodeInfo, NodeRecord>();
    for (LocatedBlock block : blks.getLocatedBlocks()) {
      DatanodeInfo[] nodes = block.getLocations();
      for (DatanodeInfo node : nodes) {
        NodeRecord record = map.get(node);
        if (record == null) {
          map.put(node, new NodeRecord(node, 1));
        } else {
          record.frequency++;
        }
      }
    }
    NodeRecord[] nodes = map.values().toArray(new NodeRecord[map.size()]);
    Arrays.sort(nodes, new NodeRecordComparator());
    return bestNode(nodes, false);
  }

  private static DatanodeInfo bestNode(DatanodeInfo[] nodes, boolean doRandom)
      throws IOException {
    if (nodes == null || nodes.length == 0) {
      throw new IOException("No nodes contain this block");
    }
    int l = 0;
    while (l < nodes.length && !nodes[l].isDecommissioned()) {
      ++l;
    }

    if (l == 0) {
      throw new IOException("No active nodes contain this block");
    }

    int index = doRandom ? DFSUtil.getRandom().nextInt(l) : 0;
    return nodes[index];
  }

  /**
   * Validate filename. 
   * @return null if the filename is invalid.
   *         Otherwise, return the validated filename.
   */
  public static String validatePath(String p) {
    return p == null || p.length() == 0?
        null: new Path(p).toUri().getPath();
  }

  /**
   * Validate a long value. 
   * @return null if the value is invalid.
   *         Otherwise, return the validated Long object.
   */
  public static Long validateLong(String value) {
    return value == null? null: Long.parseLong(value);
  }

  public static String getDefaultWebUserName(Configuration conf) throws IOException {
    String user = conf.get(
        HADOOP_HTTP_STATIC_USER, DEFAULT_HADOOP_HTTP_STATIC_USER);
    if (user == null || user.length() == 0) {
      throw new IOException("Cannot determine UGI from request or conf");
    }
    return user;
  }

  private static InetSocketAddress getNNServiceAddress(ServletContext context,
      HttpServletRequest request) {
    String namenodeAddressInUrl = request.getParameter(NAMENODE_ADDRESS);
    InetSocketAddress namenodeAddress = null;
    if (namenodeAddressInUrl != null) {
      namenodeAddress = NetUtils.createSocketAddr(namenodeAddressInUrl);
    } else if (context != null) {
      namenodeAddress = NameNodeHttpServer.getNameNodeAddressFromContext(
          context); 
    }
    if (namenodeAddress != null) {
      return namenodeAddress;
    }
    return null;
  }

  /** Same as getUGI(null, request, conf). */
  public static UserGroupInformation getUGI(HttpServletRequest request,
      Configuration conf) throws IOException {
    return getUGI(null, request, conf);
  }
  
  /** Same as getUGI(context, request, conf, KERBEROS_SSL, true). */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf) throws IOException {
    return getUGI(context, request, conf, AuthenticationMethod.KERBEROS_SSL, true);
  }

  /**
   * Get {@link UserGroupInformation} and possibly the delegation token out of
   * the request.
   * @param context the ServletContext that is serving this request.
   * @param request the http request
   * @param conf configuration
   * @param secureAuthMethod the AuthenticationMethod used in secure mode.
   * @param tryUgiParameter Should it try the ugi parameter?
   * @return a new user from the request
   * @throws AccessControlException if the request has no token
   */
  public static UserGroupInformation getUGI(ServletContext context,
      HttpServletRequest request, Configuration conf,
      final AuthenticationMethod secureAuthMethod,
      final boolean tryUgiParameter) throws IOException {
    UserGroupInformation ugi = null;
    final String usernameFromQuery = getUsernameFromQuery(request, tryUgiParameter);
    final String doAsUserFromQuery = request.getParameter(DoAsParam.NAME);
    final String remoteUser;
   
    if (UserGroupInformation.isSecurityEnabled()) {
      remoteUser = request.getRemoteUser();
      final String tokenString = request.getParameter(DELEGATION_PARAMETER_NAME);
      if (tokenString != null) {
        // Token-based connections need only verify the effective user, and
        // disallow proxying to different user.  Proxy authorization checks
        // are not required since the checks apply to issuing a token.
        ugi = getTokenUGI(context, request, tokenString, conf);
        checkUsername(ugi.getShortUserName(), usernameFromQuery);
        checkUsername(ugi.getShortUserName(), doAsUserFromQuery);
      } else if (remoteUser == null) {
        throw new IOException(
            "Security enabled but user not authenticated by filter");
      }
    } else {
      // Security's not on, pull from url or use default web user
      remoteUser = (usernameFromQuery == null)
          ? getDefaultWebUserName(conf) // not specified in request
          : usernameFromQuery;
    }

    if (ugi == null) { // security is off, or there's no token
      ugi = UserGroupInformation.createRemoteUser(remoteUser);
      checkUsername(ugi.getShortUserName(), usernameFromQuery);
      if (UserGroupInformation.isSecurityEnabled()) {
        // This is not necessarily true, could have been auth'ed by user-facing
        // filter
        ugi.setAuthenticationMethod(secureAuthMethod);
      }
      if (doAsUserFromQuery != null) {
        // create and attempt to authorize a proxy user
        ugi = UserGroupInformation.createProxyUser(doAsUserFromQuery, ugi);
        ProxyUsers.authorize(ugi, getRemoteAddr(request));
      }
    }
    
    if(LOG.isDebugEnabled())
      LOG.debug("getUGI is returning: " + ugi.getShortUserName());
    return ugi;
  }

  private static UserGroupInformation getTokenUGI(ServletContext context,
                                                  HttpServletRequest request,
                                                  String tokenString,
                                                  Configuration conf)
                                                      throws IOException {
    final Token<DelegationTokenIdentifier> token =
        new Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(tokenString);
    InetSocketAddress serviceAddress = getNNServiceAddress(context, request);
    if (serviceAddress != null) {
      SecurityUtil.setTokenService(token, serviceAddress);
      token.setKind(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
    }

    ByteArrayInputStream buf =
        new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    id.readFields(in);
    if (context != null) {
      final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
      if (nn != null) {
        // Verify the token.
        nn.getNamesystem().verifyToken(id, token.getPassword());
      }
    }
    UserGroupInformation ugi = id.getUser();
    ugi.addToken(token);
    return ugi;
  }

  // honor the X-Forwarded-For header set by a configured set of trusted
  // proxy servers.  allows audit logging and proxy user checks to work
  // via an http proxy
  public static String getRemoteAddr(HttpServletRequest request) {
    String remoteAddr = request.getRemoteAddr();
    String proxyHeader = request.getHeader("X-Forwarded-For");
    if (proxyHeader != null && ProxyServers.isProxyServer(remoteAddr)) {
      final String clientAddr = proxyHeader.split(",")[0].trim();
      if (!clientAddr.isEmpty()) {
        remoteAddr = clientAddr;
      }
    }
    return remoteAddr;
  }


  /**
   * Expected user name should be a short name.
   */
  public static void checkUsername(final String expected, final String name
      ) throws IOException {
    if (expected == null && name != null) {
      throw new IOException("Usernames not matched: expecting null but name="
          + name);
    }
    if (name == null) { //name is optional, null is okay
      return;
    }
    KerberosName u = new KerberosName(name);
    String shortName = u.getShortName();
    if (!shortName.equals(expected)) {
      throw new IOException("Usernames not matched: name=" + shortName
          + " != expected=" + expected);
    }
  }

  private static String getUsernameFromQuery(final HttpServletRequest request,
      final boolean tryUgiParameter) {
    String username = request.getParameter(UserParam.NAME);
    if (username == null && tryUgiParameter) {
      //try ugi parameter
      final String ugiStr = request.getParameter("ugi");
      if (ugiStr != null) {
        username = ugiStr.split(",")[0];
      }
    }
    return username;
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

  /**
   * Returns the url parameter for the given string, prefixed with
   * paramSeparator.
   * 
   * @param name parameter name
   * @param val parameter value
   * @param paramSeparator URL parameter prefix, i.e. either '?' or '&'
   * @return url parameter
   */
  public static String getUrlParam(String name, String val, String paramSeparator) {
    return val == null ? "" : paramSeparator + name + "=" + val;
  }
  
  /**
   * Returns the url parameter for the given string, prefixed with '?' if
   * firstParam is true, prefixed with '&' if firstParam is false.
   * 
   * @param name parameter name
   * @param val parameter value
   * @param firstParam true if this is the first parameter in the list, false otherwise
   * @return url parameter
   */
  public static String getUrlParam(String name, String val, boolean firstParam) {
    return getUrlParam(name, val, firstParam ? "?" : "&");
  }
  
  /**
   * Returns the url parameter for the given string, prefixed with '&'.
   * 
   * @param name parameter name
   * @param val parameter value
   * @return url parameter
   */
  public static String getUrlParam(String name, String val) {
    return getUrlParam(name, val, false);
  }
}
