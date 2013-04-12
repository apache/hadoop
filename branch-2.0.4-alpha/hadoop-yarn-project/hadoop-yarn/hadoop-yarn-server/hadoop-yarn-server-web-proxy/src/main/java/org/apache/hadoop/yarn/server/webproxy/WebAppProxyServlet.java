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

package org.apache.hadoop.yarn.server.webproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class WebAppProxyServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Log LOG = LogFactory.getLog(WebAppProxyServlet.class);
  private static final HashSet<String> passThroughHeaders = 
    new HashSet<String>(Arrays.asList("User-Agent", "Accept", "Accept-Encoding",
        "Accept-Language", "Accept-Charset"));
  
  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

  private final List<TrackingUriPlugin> trackingUriPlugins;
  private final String rmAppPageUrlBase;

  private static class _ implements Hamlet._ {
    //Empty
  }
  
  private static class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }
  
    public HTML<WebAppProxyServlet._> html() {
      return new HTML<WebAppProxyServlet._>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }

  /**
   * Default constructor
   */
  public WebAppProxyServlet()
  {
    super();
    YarnConfiguration conf = new YarnConfiguration();
    this.trackingUriPlugins =
        conf.getInstances(YarnConfiguration.YARN_TRACKING_URL_GENERATOR,
            TrackingUriPlugin.class);
    this.rmAppPageUrlBase = StringHelper.pjoin(
        YarnConfiguration.getRMWebAppURL(conf), "cluster", "app");
  }

  /**
   * Output 404 with appropriate message.
   * @param resp the http response.
   * @param message the message to include on the page.
   * @throws IOException on any error.
   */
  private static void notFound(HttpServletResponse resp, String message) 
    throws IOException {
    resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    resp.setContentType(MimeType.HTML);
    Page p = new Page(resp.getWriter());
    p.html().
      h1(message).
    _();
  }
  
  /**
   * Warn the user that the link may not be safe!
   * @param resp the http response
   * @param link the link to point to
   * @param user the user that owns the link.
   * @throws IOException on any error.
   */
  private static void warnUserPage(HttpServletResponse resp, String link, 
      String user, ApplicationId id) throws IOException {
    //Set the cookie when we warn which overrides the query parameter
    //This is so that if a user passes in the approved query parameter without
    //having first visited this page then this page will still be displayed 
    resp.addCookie(makeCheckCookie(id, false));
    resp.setContentType(MimeType.HTML);
    Page p = new Page(resp.getWriter());
    p.html().
      h1("WARNING: The following page may not be safe!").h3().
      _("click ").a(link, "here").
      _(" to continue to an Application Master web interface owned by ", user).
      _().
    _();
  }
  
  /**
   * Download link and have it be the response.
   * @param req the http request
   * @param resp the http response
   * @param link the link to download
   * @param c the cookie to set if any
   * @throws IOException on any error.
   */
  private static void proxyLink(HttpServletRequest req, 
      HttpServletResponse resp, URI link, Cookie c, String proxyHost)
      throws IOException {
    org.apache.commons.httpclient.URI uri = 
      new org.apache.commons.httpclient.URI(link.toString(), false);
    HttpClientParams params = new HttpClientParams();
    params.setCookiePolicy(CookiePolicy.BROWSER_COMPATIBILITY);
    params.setBooleanParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS, true);
    HttpClient client = new HttpClient(params);
    // Make sure we send the request from the proxy address in the config
    // since that is what the AM filter checks against. IP aliasing or
    // similar could cause issues otherwise.
    HostConfiguration config = new HostConfiguration();
    InetAddress localAddress = InetAddress.getByName(proxyHost);
    if (LOG.isDebugEnabled()) {
      LOG.debug("local InetAddress for proxy host: " + localAddress.toString());
    }
    config.setLocalAddress(localAddress);
    HttpMethod method = new GetMethod(uri.getEscapedURI());

    @SuppressWarnings("unchecked")
    Enumeration<String> names = req.getHeaderNames();
    while(names.hasMoreElements()) {
      String name = names.nextElement();
      if(passThroughHeaders.contains(name)) {
        String value = req.getHeader(name);
        LOG.debug("REQ HEADER: "+name+" : "+value);
        method.setRequestHeader(name, value);
      }
    }

    String user = req.getRemoteUser();
    if(user != null && !user.isEmpty()) {
      method.setRequestHeader("Cookie",PROXY_USER_COOKIE_NAME+"="+
          URLEncoder.encode(user, "ASCII"));
    }
    OutputStream out = resp.getOutputStream();
    try {
      resp.setStatus(client.executeMethod(config, method));
      for(Header header : method.getResponseHeaders()) {
        resp.setHeader(header.getName(), header.getValue());
      }
      if(c != null) {
        resp.addCookie(c);
      }
      InputStream in = method.getResponseBodyAsStream();
      if(in != null) {
        IOUtils.copyBytes(in, out, 4096, true);
      }
    } finally {
      method.releaseConnection();
    }
  }
  
  private static String getCheckCookieName(ApplicationId id){
    return "checked_"+id;
  }
  
  private static Cookie makeCheckCookie(ApplicationId id, boolean isSet) {
    Cookie c = new Cookie(getCheckCookieName(id),String.valueOf(isSet));
    c.setPath(ProxyUriUtils.getPath(id));
    c.setMaxAge(60 * 60 * 2); //2 hours in seconds
    return c;
  }
  
  private boolean isSecurityEnabled() {
    Boolean b = (Boolean) getServletContext()
        .getAttribute(WebAppProxy.IS_SECURITY_ENABLED_ATTRIBUTE);
    if(b != null) return b;
    return false;
  }
  
  private ApplicationReport getApplicationReport(ApplicationId id) throws IOException {
    return ((AppReportFetcher) getServletContext()
        .getAttribute(WebAppProxy.FETCHER_ATTRIBUTE)).getApplicationReport(id);
  }
  
  private String getProxyHost() throws IOException {
    return ((String) getServletContext()
        .getAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE));
  }
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) 
  throws IOException{
    try {
      String userApprovedParamS = 
        req.getParameter(ProxyUriUtils.PROXY_APPROVAL_PARAM);
      boolean userWasWarned = false;
      boolean userApproved = 
        (userApprovedParamS != null && Boolean.valueOf(userApprovedParamS));
      boolean securityEnabled = isSecurityEnabled();
      final String remoteUser = req.getRemoteUser();
      final String pathInfo = req.getPathInfo();

      String parts[] = pathInfo.split("/", 3);
      if(parts.length < 2) {
        LOG.warn(remoteUser+" Gave an invalid proxy path "+pathInfo);
        notFound(resp, "Your path appears to be formatted incorrectly.");
        return;
      }
      //parts[0] is empty because path info always starts with a /
      String appId = parts[1];
      String rest = parts.length > 2 ? parts[2] : "";
      ApplicationId id = Apps.toAppID(appId);
      if(id == null) {
        LOG.warn(req.getRemoteUser()+" Attempting to access "+appId+
        " that is invalid");
        notFound(resp, appId+" appears to be formatted incorrectly.");
        return;
      }
      
      if(securityEnabled) {
        String cookieName = getCheckCookieName(id); 
        Cookie[] cookies = req.getCookies();
        if (cookies != null) {
          for (Cookie c : cookies) {
            if (cookieName.equals(c.getName())) {
              userWasWarned = true;
              userApproved = userApproved || Boolean.valueOf(c.getValue());
              break;
            }
          }
        }
      }
      
      boolean checkUser = securityEnabled && (!userWasWarned || !userApproved);

      ApplicationReport applicationReport = getApplicationReport(id);
      if(applicationReport == null) {
        LOG.warn(req.getRemoteUser()+" Attempting to access "+id+
            " that was not found");

        URI toFetch =
            ProxyUriUtils
                .getUriFromTrackingPlugins(id, this.trackingUriPlugins);
        if (toFetch != null)
        {
          resp.sendRedirect(resp.encodeRedirectURL(toFetch.toString()));
          return;
        }

        notFound(resp, "Application "+appId+" could not be found, " +
        		"please try the history server");
        return;
      }
      String original = applicationReport.getOriginalTrackingUrl();
      URI trackingUri = null;
      if (original != null) {
        trackingUri = ProxyUriUtils.getUriFromAMUrl(original);
      }
      // fallback to ResourceManager's app page if no tracking URI provided
      if(original == null || original.equals("N/A")) {
        resp.sendRedirect(resp.encodeRedirectURL(
            StringHelper.pjoin(rmAppPageUrlBase, id.toString())));
        return;
      }

      String runningUser = applicationReport.getUser();
      if(checkUser && !runningUser.equals(remoteUser)) {
        LOG.info("Asking "+remoteUser+" if they want to connect to the " +
            "app master GUI of "+appId+" owned by "+runningUser);
        warnUserPage(resp, ProxyUriUtils.getPathAndQuery(id, rest, 
            req.getQueryString(), true), runningUser, id);
        return;
      }
      
      URI toFetch = new URI(req.getScheme(), 
          trackingUri.getAuthority(),
          StringHelper.ujoin(trackingUri.getPath(), rest), req.getQueryString(),
          null);
      
      LOG.info(req.getRemoteUser()+" is accessing unchecked "+toFetch+
          " which is the app master GUI of "+appId+" owned by "+runningUser);

      switch(applicationReport.getYarnApplicationState()) {
      case KILLED:
      case FINISHED:
      case FAILED:
        resp.sendRedirect(resp.encodeRedirectURL(toFetch.toString()));
        return;
      }
      Cookie c = null;
      if(userWasWarned && userApproved) {
        c = makeCheckCookie(id, true);
      }
      proxyLink(req, resp, toFetch, c, getProxyHost());

    } catch(URISyntaxException e) {
      throw new IOException(e); 
    }
  }
}
