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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriBuilderException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
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
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebAppProxyServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(
      WebAppProxyServlet.class);
  private static final String REDIRECT = "/redirect";
  private static final Set<String> PASS_THROUGH_HEADERS =
    new HashSet<>(Arrays.asList(
        "User-Agent",
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "Accept-Charset",
        "Content-Type",
        "Origin",
        "Access-Control-Request-Method",
        "Access-Control-Request-Headers"));

  public static final String PROXY_USER_COOKIE_NAME = "proxy-user";

  private transient List<TrackingUriPlugin> trackingUriPlugins;
  private final String rmAppPageUrlBase;
  private final String ahsAppPageUrlBase;
  private final String failurePageUrlBase;
  private transient YarnConfiguration conf;

  /**
   * HTTP methods.
   */
  private enum HTTP { GET, POST, HEAD, PUT, DELETE };

  /**
   * Empty Hamlet class.
   */
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
    this.rmAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf),
          "cluster", "app");
    this.failurePageUrlBase =
        StringHelper.pjoin(WebAppUtils.getResolvedRMWebAppURLWithScheme(conf),
          "cluster", "failure");
    this.ahsAppPageUrlBase =
        StringHelper.pjoin(WebAppUtils.getHttpSchemePrefix(conf)
          + WebAppUtils.getAHSWebAppURLWithoutScheme(conf),
          "applicationhistory", "app");
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
   * @param proxyHost the proxy host
   * @param method the http method
   * @throws IOException on any error.
   */
  private static void proxyLink(final HttpServletRequest req,
      final HttpServletResponse resp, final URI link, final Cookie c,
      final String proxyHost, final HTTP method) throws IOException {
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

    HttpRequestBase base = null;
    if (method.equals(HTTP.GET)) {
      base = new HttpGet(link);
    } else if (method.equals(HTTP.PUT)) {
      base = new HttpPut(link);

      StringBuilder sb = new StringBuilder();
      BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(req.getInputStream(), "UTF-8"));
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }

      ((HttpPut) base).setEntity(new StringEntity(sb.toString()));
    } else {
      resp.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
      return;
    }

    @SuppressWarnings("unchecked")
    Enumeration<String> names = req.getHeaderNames();
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      if (PASS_THROUGH_HEADERS.contains(name)) {
        String value = req.getHeader(name);
        if (LOG.isDebugEnabled()) {
          LOG.debug("REQ HEADER: {} : {}", name, value);
        }
        base.setHeader(name, value);
      }
    }

    String user = req.getRemoteUser();
    if (user != null && !user.isEmpty()) {
      base.setHeader("Cookie",
          PROXY_USER_COOKIE_NAME + "=" + URLEncoder.encode(user, "ASCII"));
    }
    OutputStream out = resp.getOutputStream();
    try {
      HttpResponse httpResp = client.execute(base);
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
      base.releaseConnection();
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
      throws ServletException, IOException {
    methodAction(req, resp, HTTP.GET);
  }

  @Override
  protected final void doPut(final HttpServletRequest req,
      final HttpServletResponse resp) throws ServletException, IOException {
    methodAction(req, resp, HTTP.PUT);
  }

  /**
   * The action against the HTTP method.
   * @param req the HttpServletRequest
   * @param resp the HttpServletResponse
   * @param method the HTTP method
   * @throws ServletException
   * @throws IOException
   */
  private void methodAction(final HttpServletRequest req,
      final HttpServletResponse resp,
      final HTTP method) throws ServletException, IOException {
    try {
      String userApprovedParamS = 
        req.getParameter(ProxyUriUtils.PROXY_APPROVAL_PARAM);
      boolean userWasWarned = false;
      boolean userApproved = Boolean.parseBoolean(userApprovedParamS);
      boolean securityEnabled = isSecurityEnabled();
      boolean isRedirect = false;
      String pathInfo = req.getPathInfo();
      final String remoteUser = req.getRemoteUser();

      String[] parts = null;

      if (pathInfo != null) {
        // If there's a redirect, strip the redirect so that the path can be
        // parsed
        if (pathInfo.startsWith(REDIRECT)) {
          pathInfo = pathInfo.substring(REDIRECT.length());
          isRedirect = true;
        }

        parts = pathInfo.split("/", 3);
      }

      if ((parts == null) || (parts.length < 2)) {
        LOG.warn("{} gave an invalid proxy path {}", remoteUser,  pathInfo);
        notFound(resp, "Your path appears to be formatted incorrectly.");
        return;
      }

      //parts[0] is empty because path info always starts with a /
      String appId = parts[1];
      String rest = parts.length > 2 ? parts[2] : "";
      ApplicationId id = Apps.toAppID(appId);

      if (id == null) {
        LOG.warn("{} attempting to access {} that is invalid",
            remoteUser, appId);
        notFound(resp, appId + " appears to be formatted incorrectly.");
        return;
      }

      // If this call is from an AM redirect, we need to be careful about how
      // we handle it.  If this method returns true, it means the method
      // already redirected the response, so we can just return.
      if (isRedirect && handleRedirect(appId, req, resp)) {
        return;
      }

      if (securityEnabled) {
        String cookieName = getCheckCookieName(id); 
        Cookie[] cookies = req.getCookies();
        if (cookies != null) {
          for (Cookie c : cookies) {
            if (cookieName.equals(c.getName())) {
              userWasWarned = true;
              userApproved = userApproved || Boolean.parseBoolean(c.getValue());
              break;
            }
          }
        }
      }
      
      boolean checkUser = securityEnabled && (!userWasWarned || !userApproved);

      FetchedAppReport fetchedAppReport;

      try {
        fetchedAppReport = getFetchedAppReport(id);
      } catch (ApplicationNotFoundException e) {
        fetchedAppReport = null;
      }

      ApplicationReport applicationReport = null;

      if (fetchedAppReport != null) {
        applicationReport = fetchedAppReport.getApplicationReport();
      }

      if (applicationReport == null) {
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

      URI trackingUri = getTrackingUri(req, resp, id,
          applicationReport.getOriginalTrackingUrl(),
          fetchedAppReport.getAppReportSource());

      // If the tracking URI is null, there was a redirect, so just return.
      if (trackingUri == null) {
        return;
      }

      String runningUser = applicationReport.getUser();

      if (checkUser && !runningUser.equals(remoteUser)) {
        LOG.info("Asking {} if they want to connect to the "
            + "app master GUI of {} owned by {}",
            remoteUser, appId, runningUser);
        warnUserPage(resp, ProxyUriUtils.getPathAndQuery(id, rest, 
            req.getQueryString(), true), runningUser, id);

        return;
      }

      // Append the user-provided path and query parameter to the original
      // tracking url.
      URI toFetch = buildTrackingUrl(trackingUri, req, rest);

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
      proxyLink(req, resp, toFetch, c, getProxyHost(), method);

    } catch(URISyntaxException | YarnException e) {
      throw new IOException(e); 
    }
  }

  /**
   * Return a URL based on the {@code trackingUri} that includes the
   * user-provided path and query parameters.
   *
   * @param trackingUri the base tracking URI
   * @param req the service request
   * @param rest the user-provided path
   * @return the new tracking URI
   * @throws UriBuilderException if there's an error building the URL
   */
  private URI buildTrackingUrl(URI trackingUri, final HttpServletRequest req,
      String rest) throws UriBuilderException {
    UriBuilder builder = UriBuilder.fromUri(trackingUri);
    String queryString = req.getQueryString();

    if (queryString != null) {
      List<NameValuePair> queryPairs = URLEncodedUtils.parse(queryString, null);

      for (NameValuePair pair : queryPairs) {
        builder.queryParam(pair.getName(), pair.getValue());
      }
    }

    return builder.path(rest).build();
  }

  /**
   * Locate the tracking URI for the application based on the reported tracking
   * URI. If the reported URI is invalid, redirect to the history server or RM
   * app page.  If the URI is valid, covert it into a usable URI object with a
   * schema.  If the returned URI is null, that means there was a redirect.
   *
   * @param req the servlet request for redirects
   * @param resp the servlet response for redirects
   * @param id the application ID
   * @param originalUri the reported tracking URI
   * @param appReportSource the source of the application report
   * @return a valid tracking URI or null if redirected instead
   * @throws IOException thrown if the redirect fails
   * @throws URISyntaxException if the tracking URI is invalid
   */
  private URI getTrackingUri(HttpServletRequest req, HttpServletResponse resp,
      ApplicationId id, String originalUri, AppReportSource appReportSource)
      throws IOException, URISyntaxException {
    URI trackingUri = null;

    if ((originalUri == null) ||
        originalUri.equals("N/A") ||
        originalUri.equals("")) {
      if (appReportSource == AppReportSource.RM) {
        // fallback to ResourceManager's app page if no tracking URI provided
        // and Application Report was fetched from RM
        LOG.debug("Original tracking url is '{}'. Redirecting to RM app page",
            originalUri == null ? "NULL" : originalUri);
        ProxyUtils.sendRedirect(req, resp,
            StringHelper.pjoin(rmAppPageUrlBase, id.toString()));
      } else if (appReportSource == AppReportSource.AHS) {
        // fallback to Application History Server app page if the application
        // report was fetched from AHS
        LOG.debug("Original tracking url is '{}'. Redirecting to AHS app page",
            originalUri == null ? "NULL" : originalUri);
        ProxyUtils.sendRedirect(req, resp,
            StringHelper.pjoin(ahsAppPageUrlBase, id.toString()));
      }
    } else if (ProxyUriUtils.getSchemeFromUrl(originalUri).isEmpty()) {
      trackingUri =
          ProxyUriUtils.getUriFromAMUrl(WebAppUtils.getHttpSchemePrefix(conf),
            originalUri);
    } else {
      trackingUri = new URI(originalUri);
    }

    return trackingUri;
  }

  /**
   * Fetch the application report from the RM.
   *
   * @param id the app ID
   * @return the application report
   * @throws IOException if the request to the RM fails
   * @throws YarnException if the request to the RM fails
   */
  private FetchedAppReport getFetchedAppReport(ApplicationId id)
      throws IOException, YarnException {
    FetchedAppReport fetchedAppReport = getApplicationReport(id);

    if (fetchedAppReport != null) {
      if ((fetchedAppReport.getAppReportSource() != AppReportSource.RM) &&
          (fetchedAppReport.getAppReportSource() != AppReportSource.AHS)) {
        throw new UnsupportedOperationException("Application report not "
            + "fetched from RM or history server.");
      }
    }

    return fetchedAppReport;
  }

  /**
   * Check whether the request is a redirect from the AM and handle it
   * appropriately. This check exists to prevent the AM from forwarding back to
   * the web proxy, which would contact the AM again, which would forward
   * again... If this method returns true, there was a redirect, and
   * it was handled by redirecting the current request to an error page.
   *
   * @param path the part of the request path after the app id
   * @param id the app id
   * @param req the request object
   * @param resp the response object
   * @return whether there was a redirect
   * @throws IOException if a redirect fails
   */
  private boolean handleRedirect(String id, HttpServletRequest req,
      HttpServletResponse resp) throws IOException {
    // If this isn't a redirect, we don't care.
    boolean badRedirect = false;

    // If this is a redirect, check if we're calling ourselves.
    try {
      badRedirect = NetUtils.getLocalInetAddress(req.getRemoteHost()) != null;
    } catch (SocketException ex) {
      // This exception means we can't determine the calling host. Odds are
      // that means it's not us.  Let it go and hope it works out better next
      // time.
    }

    // If the proxy tries to call itself, it gets into an endless
    // loop and consumes all available handler threads until the
    // application completes.  Redirect to the app page with a flag
    // that tells it to print an appropriate error message.
    if (badRedirect) {
      LOG.error("The AM's web app redirected the RM web proxy's request back "
          + "to the web proxy. The typical cause is that the AM is resolving "
          + "the RM's address as something other than what it expects. Check "
          + "your network configuration and the value of the "
          + "yarn.web-proxy.address property. Once the host resolution issue "
          + "has been resolved, you will likely need to delete the "
          + "misbehaving application, " + id);
      String redirect = StringHelper.pjoin(failurePageUrlBase, id);
      LOG.error("REDIRECT: sending redirect to " + redirect);
      ProxyUtils.sendRedirect(req, resp, redirect);
    }

    return badRedirect;
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
