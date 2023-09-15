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
package org.apache.hadoop.yarn.webapp.util;

import static org.apache.hadoop.yarn.util.StringHelper.PATH_JOINER;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.http.HttpConfig.Policy;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.hadoop.yarn.webapp.BadRequestException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.servlet.http.HttpServletRequest;

@Private
@Evolving
public class WebAppUtils {
  public static final String WEB_APP_TRUSTSTORE_PASSWORD_KEY =
      "ssl.server.truststore.password";
  public static final String WEB_APP_KEYSTORE_PASSWORD_KEY =
      "ssl.server.keystore.password";
  public static final String WEB_APP_KEY_PASSWORD_KEY =
      "ssl.server.keystore.keypassword";
  public static final String HTTPS_PREFIX = "https://";
  public static final String HTTP_PREFIX = "http://";

  public static void setRMWebAppPort(Configuration conf, int port) {
    String hostname = getRMWebAppURLWithoutScheme(conf);
    hostname =
        (hostname.contains(":")) ? hostname.substring(0, hostname.indexOf(":"))
            : hostname;
    setRMWebAppHostnameAndPort(conf, hostname, port);
  }

  public static void setRMWebAppHostnameAndPort(Configuration conf,
      String hostname, int port) {
    String resolvedAddress = hostname + ":" + port;
    if (YarnConfiguration.useHttps(conf)) {
      conf.set(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, resolvedAddress);
    } else {
      conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, resolvedAddress);
    }
  }
  
  public static void setNMWebAppHostNameAndPort(Configuration conf,
      String hostName, int port) {
    if (YarnConfiguration.useHttps(conf)) {
      conf.set(YarnConfiguration.NM_WEBAPP_HTTPS_ADDRESS,
          hostName + ":" + port);
    } else {
      conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS,
          hostName + ":" + port);
    }
  }

  /**
   * Runs a certain function against the active RM. The function's first
   * argument is expected to be a string which contains the address of
   * the RM being tried.
   * @param conf configuration.
   * @param func throwing bi function.
   * @param arg T arg.
   * @param <T> Generic T.
   * @param <R> Generic R.
   * @throws Exception exception occurs.
   * @return instance of Generic R.
   */
  public static <T, R> R execOnActiveRM(Configuration conf,
      ThrowingBiFunction<String, T, R> func, T arg) throws Exception {
    int haIndex = 0;
    if (HAUtil.isHAEnabled(conf)) {
      String activeRMId = RMHAUtils.findActiveRMHAId(conf);
      if (activeRMId != null) {
        haIndex = new ArrayList<>(HAUtil.getRMHAIds(conf)).indexOf(activeRMId);
      } else {
        throw new ConnectException("No Active RM available");
      }
    }
    String rm1Address = getRMWebAppURLWithScheme(conf, haIndex);
    return func.apply(rm1Address, arg);
  }

  /** A BiFunction which throws on Exception. */
  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {
    R apply(T t, U u) throws Exception;
  }

  public static String getRMWebAppURLWithoutScheme(Configuration conf,
      boolean isHAEnabled, int haIdIndex)  {
    YarnConfiguration yarnConfig = new YarnConfiguration(conf);
    // set RM_ID if we have not configure it.
    if (isHAEnabled) {
      String rmId = yarnConfig.get(YarnConfiguration.RM_HA_ID);
      if (rmId == null || rmId.isEmpty()) {
        List<String> rmIds = new ArrayList<>(HAUtil.getRMHAIds(conf));
        if (rmIds != null && !rmIds.isEmpty()) {
          yarnConfig.set(YarnConfiguration.RM_HA_ID, rmIds.get(haIdIndex));
        }
      }
    }
    if (YarnConfiguration.useHttps(yarnConfig)) {
      if (isHAEnabled) {
        return HAUtil.getConfValueForRMInstance(
            YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS, yarnConfig);
      }
      return yarnConfig.get(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS);
    }else {
      if (isHAEnabled) {
        return HAUtil.getConfValueForRMInstance(
            YarnConfiguration.RM_WEBAPP_ADDRESS, yarnConfig);
      }
      return yarnConfig.get(YarnConfiguration.RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS);
    }
  }

  public static String getRMWebAppURLWithScheme(Configuration conf,
      int haIdIndex) {
    return getHttpSchemePrefix(conf) + getRMWebAppURLWithoutScheme(
        conf, HAUtil.isHAEnabled(conf), haIdIndex);
  }

  public static String getRMWebAppURLWithScheme(Configuration conf) {
    return getHttpSchemePrefix(conf) + getRMWebAppURLWithoutScheme(
        conf, HAUtil.isHAEnabled(conf), 0);
  }

  public static String getRMWebAppURLWithoutScheme(Configuration conf) {
    return getRMWebAppURLWithoutScheme(conf, false, 0);
  }

  public static String getRouterWebAppURLWithScheme(Configuration conf) {
    return getHttpSchemePrefix(conf) + getRouterWebAppURLWithoutScheme(conf);
  }

  public static String getRouterWebAppURLWithoutScheme(Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf.get(YarnConfiguration.ROUTER_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(YarnConfiguration.ROUTER_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_ROUTER_WEBAPP_ADDRESS);
    }
  }

  public static String getGPGWebAppURLWithoutScheme(Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf.get(YarnConfiguration.GPG_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_GPG_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(YarnConfiguration.GPG_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_GPG_WEBAPP_ADDRESS);
    }
  }

  public static List<String> getProxyHostsAndPortsForAmFilter(
      Configuration conf) {
    List<String> addrs = new ArrayList<String>();
    String proxyAddr = conf.get(YarnConfiguration.PROXY_ADDRESS);
    // If PROXY_ADDRESS isn't set, fallback to RM_WEBAPP(_HTTPS)_ADDRESS
    // There could be multiple if using RM HA
    if (proxyAddr == null || proxyAddr.isEmpty()) {
      // If RM HA is enabled, try getting those addresses
      if (HAUtil.isHAEnabled(conf)) {
        List<String> haAddrs =
            RMHAUtils.getRMHAWebappAddresses(new YarnConfiguration(conf));
        for (String addr : haAddrs) {
          try {
            InetSocketAddress socketAddr = NetUtils.createSocketAddr(addr);
            addrs.add(getResolvedAddress(socketAddr));
          } catch(IllegalArgumentException e) {
            // skip if can't resolve
          }
        }
      }
      // If couldn't resolve any of the addresses or not using RM HA, fallback
      if (addrs.isEmpty()) {
        addrs.add(getResolvedRMWebAppURLWithoutScheme(conf));
      }
    } else {
      addrs.add(proxyAddr);
    }
    return addrs;
  }
  
  public static String getProxyHostAndPort(Configuration conf) {
    String addr = conf.get(YarnConfiguration.PROXY_ADDRESS);
    if(addr == null || addr.isEmpty()) {
      addr = getResolvedRMWebAppURLWithoutScheme(conf);
    }
    return addr;
  }

  public static String getResolvedRemoteRMWebAppURLWithScheme(
      Configuration conf) {
    return getHttpSchemePrefix(conf)
        + getResolvedRemoteRMWebAppURLWithoutScheme(conf);
  }

  public static String getResolvedRMWebAppURLWithScheme(Configuration conf) {
    return getHttpSchemePrefix(conf)
        + getResolvedRMWebAppURLWithoutScheme(conf);
  }
  
  public static String getResolvedRemoteRMWebAppURLWithoutScheme(
      Configuration conf) {
    return getResolvedRemoteRMWebAppURLWithoutScheme(conf,
        YarnConfiguration.useHttps(conf) ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY);
  }

  public static String getResolvedRMWebAppURLWithoutScheme(Configuration conf) {
    return getResolvedRMWebAppURLWithoutScheme(conf,
        YarnConfiguration.useHttps(conf) ? Policy.HTTPS_ONLY : Policy.HTTP_ONLY);
  }
  
  public static String getResolvedRMWebAppURLWithoutScheme(Configuration conf,
      Policy httpPolicy) {
    InetSocketAddress address = null;
    if (httpPolicy == Policy.HTTPS_ONLY) {
      address =
          conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT);
    } else {
      address =
          conf.getSocketAddr(YarnConfiguration.RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
              YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);      
    }
    return getResolvedAddress(address);
  }

  public static String getResolvedRemoteRMWebAppURLWithoutScheme(Configuration conf,
      Policy httpPolicy) {
    String rmId = null;
    if (HAUtil.isHAEnabled(conf)) {
      // If HA enabled, pick one of the RM-IDs and rely on redirect to go to
      // the Active RM
      rmId = (String) HAUtil.getRMHAIds(conf).toArray()[0];
    }
    return getResolvedRemoteRMWebAppURLWithoutScheme(conf, httpPolicy, rmId);
  }

  public static String getResolvedRemoteRMWebAppURLWithoutScheme(
      Configuration conf, Policy httpPolicy, String rmId) {
    InetSocketAddress address = null;

    if (httpPolicy == Policy.HTTPS_ONLY) {
      address = conf.getSocketAddr(
          rmId == null ? YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS
              : HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS,
                  rmId),
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_PORT);
    } else {
      address = conf.getSocketAddr(
          rmId == null ? YarnConfiguration.RM_WEBAPP_ADDRESS
              : HAUtil.addSuffix(YarnConfiguration.RM_WEBAPP_ADDRESS, rmId),
          YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_RM_WEBAPP_PORT);
    }
    return getResolvedAddress(address);
  }

  public static String getResolvedAddress(InetSocketAddress address) {
    address = NetUtils.getConnectAddress(address);
    StringBuilder sb = new StringBuilder();
    InetAddress resolved = address.getAddress();
    if (resolved == null || resolved.isAnyLocalAddress() ||
        resolved.isLoopbackAddress()) {
      String lh = address.getHostName();
      try {
        lh = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        //Ignore and fallback.
      }
      sb.append(lh);
    } else {
      sb.append(address.getHostName());
    }
    sb.append(":").append(address.getPort());
    return sb.toString();
  }
  
  /**
   * Get the URL to use for binding where bind hostname can be specified
   * to override the hostname in the webAppURLWithoutScheme. Port specified in the
   * webAppURLWithoutScheme will be used.
   *
   * @param conf the configuration
   * @param hostProperty bind host property name
   * @param webAppURLWithoutScheme web app URL without scheme String
   * @return String representing bind URL
   */
  public static String getWebAppBindURL(
      Configuration conf,
      String hostProperty,
      String webAppURLWithoutScheme) {

    // If the bind-host setting exists then it overrides the hostname
    // portion of the corresponding webAppURLWithoutScheme
    String host = conf.getTrimmed(hostProperty);
    if (host != null && !host.isEmpty()) {
      if (webAppURLWithoutScheme.contains(":")) {
        webAppURLWithoutScheme = host + ":" + webAppURLWithoutScheme.split(":")[1];
      }
      else {
        throw new YarnRuntimeException("webAppURLWithoutScheme must include port specification but doesn't: " +
                                       webAppURLWithoutScheme);
      }
    }

    return webAppURLWithoutScheme;
  }

  public static String getNMWebAppURLWithoutScheme(Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf.get(YarnConfiguration.NM_WEBAPP_HTTPS_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(YarnConfiguration.NM_WEBAPP_ADDRESS,
        YarnConfiguration.DEFAULT_NM_WEBAPP_ADDRESS);
    }
  }

  public static String getAHSWebAppURLWithoutScheme(Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS,
          YarnConfiguration.DEFAULT_TIMELINE_SERVICE_WEBAPP_ADDRESS);
    }
  }

  public static String getTimelineReaderWebAppURLWithoutScheme(
      Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf
          .get(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_HTTPS_ADDRESS,
              YarnConfiguration.
                  DEFAULT_TIMELINE_SERVICE_READER_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf.get(YarnConfiguration.TIMELINE_SERVICE_READER_WEBAPP_ADDRESS,
          YarnConfiguration.
              DEFAULT_TIMELINE_SERVICE_READER_WEBAPP_ADDRESS);
    }
  }

  public static String getTimelineCollectorWebAppURLWithoutScheme(
      Configuration conf) {
    if (YarnConfiguration.useHttps(conf)) {
      return conf.get(
          YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_WEBAPP_HTTPS_ADDRESS,
          YarnConfiguration.
              DEFAULT_TIMELINE_SERVICE_COLLECTOR_WEBAPP_HTTPS_ADDRESS);
    } else {
      return conf
          .get(YarnConfiguration.TIMELINE_SERVICE_COLLECTOR_WEBAPP_ADDRESS,
              YarnConfiguration.
                  DEFAULT_TIMELINE_SERVICE_COLLECTOR_WEBAPP_ADDRESS);
    }
  }

  /**
   * if url has scheme then it will be returned as it is else it will return
   * url with scheme.
   * @param schemePrefix eg. http:// or https://
   * @param url url.
   * @return url with scheme
   */
  public static String getURLWithScheme(String schemePrefix, String url) {
    // If scheme is provided then it will be returned as it is
    if (url.indexOf("://") > 0) {
      return url;
    } else {
      return schemePrefix + url;
    }
  }

  public static String getRunningLogURL(
      String nodeHttpAddress, String containerId, String user) {
    if (nodeHttpAddress == null || nodeHttpAddress.isEmpty() ||
        containerId == null || containerId.isEmpty() ||
        user == null || user.isEmpty()) {
      return null;
    }
    return PATH_JOINER.join(
        nodeHttpAddress, "node", "containerlogs", containerId, user);
  }

  public static String getAggregatedLogURL(String serverHttpAddress,
      String allocatedNode, String containerId, String entity, String user) {
    if (serverHttpAddress == null || serverHttpAddress.isEmpty() ||
        allocatedNode == null || allocatedNode.isEmpty() ||
        containerId == null || containerId.isEmpty() ||
        entity == null || entity.isEmpty() ||
        user == null || user.isEmpty()) {
      return null;
    }
    return PATH_JOINER.join(serverHttpAddress, "applicationhistory", "logs",
        allocatedNode, containerId, entity, user);
  }

  /**
   * Choose which scheme (HTTP or HTTPS) to use when generating a URL based on
   * the configuration.
   *
   * @param conf configuration.
   * @return the scheme (HTTP / HTTPS)
   */
  public static String getHttpSchemePrefix(Configuration conf) {
    return YarnConfiguration.useHttps(conf) ? HTTPS_PREFIX : HTTP_PREFIX;
  }

  /**
   * Load the SSL keystore / truststore into the HttpServer builder.
   * @param builder the HttpServer2.Builder to populate with ssl config
   * @return HttpServer2.Builder instance (passed in as the first parameter)
   *         after loading SSL stores
   */
  public static HttpServer2.Builder loadSslConfiguration(
      HttpServer2.Builder builder) {
    return loadSslConfiguration(builder, null);
  }

  /**
   * Load the SSL keystore / truststore into the HttpServer builder.
   * @param builder the HttpServer2.Builder to populate with ssl config
   * @param conf the Configuration instance to load custom SSL config from
   *
   * @return HttpServer2.Builder instance (passed in as the first parameter)
   *         after loading SSL stores
   */
  public static HttpServer2.Builder loadSslConfiguration(
      HttpServer2.Builder builder, Configuration conf) {

    Configuration sslConf = new Configuration(false);

    sslConf.addResource(YarnConfiguration.YARN_SSL_SERVER_RESOURCE_DEFAULT);
    if (conf != null) {
      sslConf.addResource(conf);
    }
    boolean needsClientAuth = YarnConfiguration.YARN_SSL_CLIENT_HTTPS_NEED_AUTH_DEFAULT;
    return builder
        .needsClientAuth(needsClientAuth)
        .keyPassword(getPassword(sslConf, WEB_APP_KEY_PASSWORD_KEY))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
            getPassword(sslConf, WEB_APP_KEYSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
            getPassword(sslConf, WEB_APP_TRUSTSTORE_PASSWORD_KEY),
            sslConf.get("ssl.server.truststore.type", "jks"))
        .excludeCiphers(
            sslConf.get("ssl.server.exclude.cipher.list"));
  }

  /**
   * Leverages the Configuration.getPassword method to attempt to get
   * passwords from the CredentialProvider API before falling back to
   * clear text in config - if falling back is allowed.
   * @param conf Configuration instance
   * @param alias name of the credential to retreive
   * @return String credential value or null
   */
  static String getPassword(Configuration conf, String alias) {
    String password = null;
    try {
      char[] passchars = conf.getPassword(alias);
      if (passchars != null) {
        password = new String(passchars);
      }
    }
    catch (IOException ioe) {
      password = null;
    }
    return password;
  }

  public static ApplicationId parseApplicationId(RecordFactory recordFactory,
      String appId) {
    if (appId == null || appId.isEmpty()) {
      throw new NotFoundException("appId, " + appId + ", is empty or null");
    }
    ApplicationId aid = null;
    try {
      aid = ApplicationId.fromString(appId);
    } catch (Exception e) {
      throw new BadRequestException(e);
    }
    if (aid == null) {
      throw new NotFoundException("app with id " + appId + " not found");
    }
    return aid;
  }

  public static String getSupportedLogContentType(String format) {
    if (format.equalsIgnoreCase("text")) {
      return "text/plain";
    } else if (format.equalsIgnoreCase("octet-stream")) {
      return "application/octet-stream";
    }
    return null;
  }

  public static String getDefaultLogContentType() {
    return "text/plain";
  }

  public static List<String> listSupportedLogContentType() {
    return Arrays.asList("text", "octet-stream");
  }

  private static String getURLEncodedQueryString(HttpServletRequest request,
      String parameterToRemove) {
    String queryString = request.getQueryString();
    if (queryString != null && !queryString.isEmpty()) {
      String reqEncoding = request.getCharacterEncoding();
      if (reqEncoding == null || reqEncoding.isEmpty()) {
        reqEncoding = "ISO-8859-1";
      }
      Charset encoding = Charset.forName(reqEncoding);
      List<NameValuePair> params = URLEncodedUtils.parse(queryString,
          encoding);
      if (parameterToRemove != null && !parameterToRemove.isEmpty()) {
        Iterator<NameValuePair> paramIterator = params.iterator();
        while(paramIterator.hasNext()) {
          NameValuePair current = paramIterator.next();
          if (current.getName().equals(parameterToRemove)) {
            paramIterator.remove();
          }
        }
      }
      return URLEncodedUtils.format(params, encoding);
    }
    return null;
  }

  /**
   * Get a query string.
   * @param request HttpServletRequest with the request details
   * @return the query parameter string
  */
  public static List<NameValuePair> getURLEncodedQueryParam(
      HttpServletRequest request) {
    String queryString = request.getQueryString();
    if (queryString != null && !queryString.isEmpty()) {
      String reqEncoding = request.getCharacterEncoding();
      if (reqEncoding == null || reqEncoding.isEmpty()) {
        reqEncoding = "ISO-8859-1";
      }
      Charset encoding = Charset.forName(reqEncoding);
      List<NameValuePair> params = URLEncodedUtils.parse(queryString,
          encoding);
      return params;
    }
    return null;
  }

  /**
    * Get a query string which removes the passed parameter.
    * @param httpRequest HttpServletRequest with the request details
    * @param parameterName the query parameters must be removed
    * @return the query parameter string
    */
  public static String removeQueryParams(HttpServletRequest httpRequest,
      String parameterName) {
    return getURLEncodedQueryString(httpRequest, parameterName);
  }

  /**
   * Get a HTML escaped uri with the query parameters of the request.
   * @param request HttpServletRequest with the request details
   * @return HTML escaped uri with the query paramters
   */
  public static String getHtmlEscapedURIWithQueryString(
      HttpServletRequest request) {
    String urlEncodedQueryString = getURLEncodedQueryString(request, null);
    if (urlEncodedQueryString != null) {
      return HtmlQuoting.quoteHtmlChars(
          request.getRequestURI() + "?" + urlEncodedQueryString);
    }
    return HtmlQuoting.quoteHtmlChars(request.getRequestURI());
  }

  /**
   * Add the query params from a HttpServletRequest to the target uri passed.
   * @param request HttpServletRequest with the request details
   * @param targetUri the uri to which the query params must be added
   * @return URL encoded string containing the targetUri + "?" + query string
   */
  public static String appendQueryParams(HttpServletRequest request,
      String targetUri) {
    String ret = targetUri;
    String urlEncodedQueryString = getURLEncodedQueryString(request, null);
    if (urlEncodedQueryString != null) {
      ret += "?" + urlEncodedQueryString;
    }
    return ret;
  }
}
