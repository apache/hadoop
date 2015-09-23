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
import java.io.ObjectInputStream;
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
import java.util.Set;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher.AppReportSource;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher.FetchedAppReport;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.StringHelper;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;
import org.apache.hadoop.yarn.webapp.MimeType;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebAppProxyServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxyServlet.class);
  private static final Set<String> passThroughHeaders = 
    new HashSet<>(Arrays.asList(
        "User-Agent",
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "Accept-Charset"));
  
  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

  private transient List<TrackingUriPlugin> trackingUriPlugins;
  private final String rmAppPageUrlBase;
  private final String ahsAppPageUrlBase;
  private transient YarnConfiguration conf;

  private static class _ implements Hamlet._ {
    //Empty
  }
  
  private static class Page extends Hamlet {
    Page(PrintWriter out) {
      super(out, 0, false);
    }
  
    public HTML<WebAppProxyServlet._> html() {
      return new HTML<>("html", null, EnumSet.of(EOpt.ENDTAG));
    }
  }

  /**
   * Default constructor
   */
  public WebAppProxyServlet() {
    super();
    conf = new YarnConfiguration();
    this.trackingUriPlugins =
        conf.getInstances(YarnConfiguration.YARN_TRACKING_URL_GENERATOR,
            TrackingUriPlugin.class);
    this.rmAppPageUrlBase = StringHelper.pjoin(
        WebAppUtils.getResolvedRMWebAppURLWithScheme(conf), "cluster", "app");
    this.ahsAppPageUrlBase = StringHelper.pjoin(
        WebAppUtils.getHttpSchemePrefix(conf) + WebAppUtils
        .getAHSWebAppURLWithoutScheme(conf), "applicationhistory", "apps");
  }

  /**
   * Output 404 with appropriate message.
   * @param resp the http response.
   * @param message the message to include on the page.
   * @throws IOException on any error.
   */
  private static void notFound(HttpServletResponse resp, String message) 
    throws IOException {
    ProxyUtils.notFound(resp, message);
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
      h1("WARNING: The following page may not be safe!").
      h3().
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
    DefaultHttpClient client = new DefaultHttpClient();
    client
        .getParams()
        .setParameter(ClientPNames.COOKIE_POLICY,
            CookiePolicy.BROWSER_COMPATIBILITY)
        .setBooleanParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
    // Make sure we send the request from the proxy address in the config
    // since that is what the AM filter checks against. IP aliasing or
    // similar could cause issues otherwise.
    InetAddress localAddress = InetAddress.getByName(proxyHost);
    if (LOG.isDebugEnabled()) {
      LOG.debug("local InetAddress for proxy host: {}", localAddress);
    }
    client.getParams()
        .setParameter(ConnRoutePNames.LOCAL_ADDRESS, localAddress);
    HttpGet httpGet = new HttpGet(link);
    @SuppressWarnings("unchecked")
    Enumeration<String> names = req.getHeaderNames();
    while(names.hasMoreElements()) {
      String name = names.nextElement();
      if(passThroughHeaders.contains(name)) {
        String value = req.getHeader(name);
        if (LOG.isDebugEnabled()) {
          LOG.debug("REQ HEADER: {} : {}", name, value);
        }
        httpGet.setHeader(name, value);
      }
    }

    String user = req.getRemoteUser();
    if (user != null && !user.isEmpty()) {
      httpGet.setHeader("Cookie",
          PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
    }
    OutputStream out = resp.getOutputStream();
    try {
      HttpResponse httpResp = client.execute(httpGet);
      resp.setStatus(httpResp.getStatusLine().getStatusCode());
      for (Header header : httpResp.getAllHeaders()) {
        resp.setHeader(header.getName(), header.getValue());
      }
      if (c != null) {
        resp.addCookie(c);
      }
      InputStream in = httpResp.getEntity().getContent();
      if (in != null) {
        IOUtils.copyBytes(in, out, 4096, true);
      }
    } finally {
      httpGet.releaseConnection();
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
    return b != null ? b : false;
  }
  
  private FetchedAppReport getApplicationReport(ApplicationId id)
      throws IOException, YarnException {
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
      boolean userApproved = Boolean.valueOf(userApprovedParamS);
      boolean securityEnabled = isSecurityEnabled();
      final String remoteUser = req.getRemoteUser();
      final String pathInfo = req.getPathInfo();

      String[] parts = pathInfo.split("/", 3);
      if(parts.length < 2) {
        LOG.warn("{} gave an invalid proxy path {}", remoteUser,  pathInfo);
        notFound(resp, "Your path appears to be formatted incorrectly.");
        return;
      }
      //parts[0] is empty because path info always starts with a /
      String appId = parts[1];
      String rest = parts.length > 2 ? parts[2] : "";
      ApplicationId id = Apps.toAppID(appId);
      if(id == null) {
        LOG.warn("{} attempting to access {} that is invalid",
            remoteUser, appId);
        notFound(resp, appId + " appears to be formatted incorrectly.");
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

      FetchedAppReport fetchedAppReport = null;
      ApplicationReport applicationReport = null;
      try {
        fetchedAppReport = getApplicationReport(id);
        if (fetchedAppReport != null) {
          if (fetchedAppReport.getAppReportSource() != AppReportSource.RM &&
              fetchedAppReport.getAppReportSource() != AppReportSource.AHS) {
            throw new UnsupportedOperationException("Application report not "
                + "fetched from RM or history server.");
          }
          applicationReport = fetchedAppReport.getApplicationReport();
        }
      } catch (ApplicationNotFoundException e) {
        applicationReport = null;
      }
      if(applicationReport == null) {
        LOG.warn("{} attempting to access {} that was not found",
            remoteUser, id);

        URI toFetch =
            ProxyUriUtils
                .getUriFromTrackingPlugins(id, this.trackingUriPlugins);
        if (toFetch != null) {
          ProxyUtils.sendRedirect(req, resp, toFetch.toString());
          return;
        }

        notFound(resp, "Application " + appId + " could not be found " +
            "in RM or history server");
        return;
      }
      String original = applicationReport.getOriginalTrackingUrl();
      URI trackingUri;
      if (original == null || original.equals("N/A") || original.equals("")) {
        if (fetchedAppReport.getAppReportSource() == AppReportSource.RM) {
          // fallback to ResourceManager's app page if no tracking URI provided
          // and Application Report was fetched from RM
          LOG.debug("Original tracking url is '{}'. Redirecting to RM app page",
              original == null? "NULL" : original);
          ProxyUtils.sendRedirect(req, resp,
              StringHelper.pjoin(rmAppPageUrlBase, id.toString()));
        } else if (fetchedAppReport.getAppReportSource()
              == AppReportSource.AHS) {
          // fallback to Application History Server app page if the application
          // report was fetched from AHS
          LOG.debug("Original tracking url is '{}'. Redirecting to AHS app page"
              , original == null? "NULL" : original);
          ProxyUtils.sendRedirect(req, resp,
              StringHelper.pjoin(ahsAppPageUrlBase, id.toString()));
        }
        return;
      } else {
        if (ProxyUriUtils.getSchemeFromUrl(original).isEmpty()) {
          trackingUri = ProxyUriUtils.getUriFromAMUrl(
              WebAppUtils.getHttpSchemePrefix(conf), original);
        } else {
          trackingUri = new URI(original);
        }
      }

      String runningUser = applicationReport.getUser();
      if(checkUser && !runningUser.equals(remoteUser)) {
        LOG.info("Asking {} if they want to connect to the "
            + "app master GUI of {} owned by {}",
            remoteUser, appId, runningUser);
        warnUserPage(resp, ProxyUriUtils.getPathAndQuery(id, rest, 
            req.getQueryString(), true), runningUser, id);
        return;
      }

      // Append the user-provided path and query parameter to the original
      // tracking url.
      List<NameValuePair> queryPairs =
          URLEncodedUtils.parse(req.getQueryString(), null);
      UriBuilder builder = UriBuilder.fromUri(trackingUri);
      for (NameValuePair pair : queryPairs) {
        builder.queryParam(pair.getName(), pair.getValue());
      }
      URI toFetch = builder.path(rest).build();

      LOG.info("{} is accessing unchecked {}"
          + " which is the app master GUI of {} owned by {}",
          remoteUser, toFetch, appId, runningUser);

      switch (applicationReport.getYarnApplicationState()) {
        case KILLED:
        case FINISHED:
        case FAILED:
          ProxyUtils.sendRedirect(req, resp, toFetch.toString());
          return;
        default:
          // fall out of the switch
      }
      Cookie c = null;
      if (userWasWarned && userApproved) {
        c = makeCheckCookie(id, true);
      }
      proxyLink(req, resp, toFetch, c, getProxyHost());

    } catch(URISyntaxException | YarnException e) {
      throw new IOException(e); 
    }
  }

  /**
   * This method is used by Java object deserialization, to fill in the
   * transient {@link #trackingUriPlugins} field.
   * See {@link ObjectInputStream#defaultReadObject()}
   * <p>
   *   <I>Do not remove</I>
   * <p>
   * Yarn isn't currently serializing this class, but findbugs
   * complains in its absence.
   * 
   * 
   * @param input source
   * @throws IOException IO failure
   * @throws ClassNotFoundException classloader fun
   */
  private void readObject(ObjectInputStream input)
      throws IOException, ClassNotFoundException {
    input.defaultReadObject();
    conf = new YarnConfiguration();
    this.trackingUriPlugins =
        conf.getInstances(YarnConfiguration.YARN_TRACKING_URL_GENERATOR,
            TrackingUriPlugin.class);
  }
}
