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
package org.apache.hadoop.hdfsproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.regex.Pattern;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UnixUserGroupInformation;

import org.apache.hadoop.hdfs.HdfsConfiguration;

public class LdapIpDirFilter implements Filter {
  public static final Log LOG = LogFactory.getLog(LdapIpDirFilter.class);

  private static String baseName;
  private static String hdfsIpSchemaStr;
  private static String hdfsIpSchemaStrPrefix;
  private static String hdfsUidSchemaStr;
  private static String hdfsGroupSchemaStr;
  private static String hdfsPathSchemaStr;

  private InitialLdapContext lctx;
  private String userId;
  private String groupName;
  private ArrayList<String> paths;

  /** Pattern for a filter to find out if a request is HFTP/HSFTP request */
  protected static final Pattern HFTP_PATTERN = Pattern
      .compile("^(/listPaths|/data|/streamFile|/file)$");
  /**
   * Pattern for a filter to find out if an HFTP/HSFTP request stores its file
   * path in the extra path information associated with the URL; if not, the
   * file path is stored in request parameter "filename"
   */
  protected static final Pattern FILEPATH_PATTERN = Pattern
      .compile("^(/listPaths|/data|/file)$");

  public void initialize(String bName, InitialLdapContext ctx) {
    // hook to cooperate unit test
    baseName = bName;
    hdfsIpSchemaStr = "uniqueMember";
    hdfsIpSchemaStrPrefix = "cn=";
    hdfsUidSchemaStr = "uid";
    hdfsGroupSchemaStr = "userClass";
    hdfsPathSchemaStr = "documentLocation";
    lctx = ctx;
    paths = new ArrayList<String>();
  }

  /** {@inheritDoc} */
  public void init(FilterConfig filterConfig) throws ServletException {
    ServletContext context = filterConfig.getServletContext();
    Configuration conf = new HdfsConfiguration(false);
    conf.addResource("hdfsproxy-default.xml");
    conf.addResource("hdfsproxy-site.xml");
    // extract namenode from source conf.
    String nn = conf.get("fs.default.name");
    if (nn == null) {
      throw new ServletException(
          "Proxy source cluster name node address not speficied");
    }
    InetSocketAddress nAddr = NetUtils.createSocketAddr(nn);
    context.setAttribute("name.node.address", nAddr);
    context.setAttribute("name.conf", conf);

    // for storing hostname <--> cluster mapping to decide which source cluster
    // to forward
    context.setAttribute("org.apache.hadoop.hdfsproxy.conf", conf);

    if (lctx == null) {
      Hashtable<String, String> env = new Hashtable<String, String>();
      env.put(InitialLdapContext.INITIAL_CONTEXT_FACTORY, conf.get(
          "hdfsproxy.ldap.initial.context.factory",
          "com.sun.jndi.ldap.LdapCtxFactory"));
      env.put(InitialLdapContext.PROVIDER_URL, conf
          .get("hdfsproxy.ldap.provider.url"));

      try {
        lctx = new InitialLdapContext(env, null);
      } catch (NamingException ne) {
        throw new ServletException("NamingException in initializing ldap"
            + ne.toString());
      }

      baseName = conf.get("hdfsproxy.ldap.role.base");
      hdfsIpSchemaStr = conf.get("hdfsproxy.ldap.ip.schema.string",
          "uniqueMember");
      hdfsIpSchemaStrPrefix = conf.get(
          "hdfsproxy.ldap.ip.schema.string.prefix", "cn=");
      hdfsUidSchemaStr = conf.get("hdfsproxy.ldap.uid.schema.string", "uid");
      hdfsGroupSchemaStr = conf.get("hdfsproxy.ldap.group.schema.string",
          "userClass");
      hdfsPathSchemaStr = conf.get("hdfsproxy.ldap.hdfs.path.schema.string",
          "documentLocation");
      paths = new ArrayList<String>();
    }
    LOG.info("LdapIpDirFilter initialization success: " + nn);
  }

  /** {@inheritDoc} */
  public void destroy() {
  }

  /** {@inheritDoc} */
  public void doFilter(ServletRequest request, ServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    HttpServletRequest rqst = (HttpServletRequest) request;
    HttpServletResponse rsp = (HttpServletResponse) response;

    if (LOG.isDebugEnabled()) {
      StringBuilder b = new StringBuilder("Request from ").append(
          rqst.getRemoteHost()).append("/").append(rqst.getRemoteAddr())
          .append(":").append(rqst.getRemotePort());
      b.append("\n The Scheme is " + rqst.getScheme());
      b.append("\n The Path Info is " + rqst.getPathInfo());
      b.append("\n The Translated Path Info is " + rqst.getPathTranslated());
      b.append("\n The Context Path is " + rqst.getContextPath());
      b.append("\n The Query String is " + rqst.getQueryString());
      b.append("\n The Request URI is " + rqst.getRequestURI());
      b.append("\n The Request URL is " + rqst.getRequestURL());
      b.append("\n The Servlet Path is " + rqst.getServletPath());
      LOG.debug(b.toString());
    }
    // check ip address
    String userIp = rqst.getRemoteAddr();
    boolean isAuthorized = false;
    try {
      isAuthorized = checkUserIp(userIp);
      if (!isAuthorized) {
        rsp.sendError(HttpServletResponse.SC_FORBIDDEN,
            "IP not authorized to access");
        return;
      }
    } catch (NamingException ne) {
      throw new IOException("NameingException in searching ldap"
          + ne.toString());
    }
    // check request path
    String servletPath = rqst.getServletPath();
    if (HFTP_PATTERN.matcher(servletPath).matches()) {
      // request is an HSFTP request
      if (FILEPATH_PATTERN.matcher(servletPath).matches()) {
        // file path as part of the URL
        isAuthorized = checkHdfsPath(rqst.getPathInfo() != null ? rqst
            .getPathInfo() : "/");
      } else {
        // file path is stored in "filename" parameter
        isAuthorized = checkHdfsPath(rqst.getParameter("filename"));
      }
    }
    if (!isAuthorized) {
      rsp.sendError(HttpServletResponse.SC_FORBIDDEN,
          "User not authorized to access path");
      return;
    }
    UnixUserGroupInformation ugi = new UnixUserGroupInformation(userId,
        groupName.split(","));
    rqst.setAttribute("authorized.ugi", ugi);
    // since we cannot pass ugi object cross context as they are from different
    // classloaders in different war file, we have to use String attribute.
    rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID", userId);
    rqst.setAttribute("org.apache.hadoop.hdfsproxy.authorized.role", groupName);
    LOG.info("User: " + userId + " (" + groupName + ") Request: "
        + rqst.getPathInfo() + " From: " + rqst.getRemoteAddr());
    chain.doFilter(request, response);
  }

  /** check that client's ip is listed in the Ldap Roles */
  @SuppressWarnings("unchecked")
  private boolean checkUserIp(String userIp) throws NamingException {
    String ipMember = hdfsIpSchemaStrPrefix + userIp;
    Attributes matchAttrs = new BasicAttributes(true);
    matchAttrs.put(new BasicAttribute(hdfsIpSchemaStr, ipMember));
    matchAttrs.put(new BasicAttribute(hdfsUidSchemaStr));
    matchAttrs.put(new BasicAttribute(hdfsPathSchemaStr));

    String[] attrIDs = { hdfsUidSchemaStr, hdfsGroupSchemaStr,
        hdfsPathSchemaStr };

    NamingEnumeration<SearchResult> results = lctx.search(baseName, matchAttrs,
        attrIDs);
    if (results.hasMore()) {
      SearchResult sr = results.next();
      Attributes attrs = sr.getAttributes();
      for (NamingEnumeration ne = attrs.getAll(); ne.hasMore();) {
        Attribute attr = (Attribute) ne.next();
        if (hdfsUidSchemaStr.equalsIgnoreCase(attr.getID())) {
          userId = (String) attr.get();
        } else if (hdfsGroupSchemaStr.equalsIgnoreCase(attr.getID())) {
          groupName = (String) attr.get();
        } else if (hdfsPathSchemaStr.equalsIgnoreCase(attr.getID())) {
          for (NamingEnumeration e = attr.getAll(); e.hasMore();) {
            paths.add((String) e.next());
          }
        }
      }
      return true;
    }
    LOG.info("Ip address " + userIp
        + " is not authorized to access the proxy server");
    return false;
  }

  /** check that the requested path is listed in the ldap entry */
  private boolean checkHdfsPath(String pathInfo) {
    if (pathInfo == null || pathInfo.length() == 0) {
      LOG.info("Can't get file path from the request");
      return false;
    }
    Path userPath = new Path(pathInfo);
    while (userPath != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("\n Checking file path " + userPath);
      }
      if (paths.contains(userPath.toString()))
        return true;
      userPath = userPath.getParent();
    }
    LOG.info("User " + userId + " is not authorized to access " + pathInfo);
    return false;
  }
}
