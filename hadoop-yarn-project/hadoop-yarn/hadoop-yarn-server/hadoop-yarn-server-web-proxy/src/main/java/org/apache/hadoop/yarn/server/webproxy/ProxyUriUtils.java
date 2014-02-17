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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.TrackingUriPlugin;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.List;

import static org.apache.hadoop.yarn.util.StringHelper.ujoin;

public class ProxyUriUtils {
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(ProxyUriUtils.class);
  
  /**Name of the servlet to use when registering the proxy servlet. */
  public static final String PROXY_SERVLET_NAME = "proxy";
  /**Base path where the proxy servlet will handle requests.*/
  public static final String PROXY_BASE = "/proxy/";
  /**Path Specification for the proxy servlet.*/
  public static final String PROXY_PATH_SPEC = PROXY_BASE+"*";
  /**Query Parameter indicating that the URI was approved.*/
  public static final String PROXY_APPROVAL_PARAM = "proxyapproved";
  
  private static String uriEncode(Object o) {
    try {
      assert (o != null) : "o canot be null";
      return URLEncoder.encode(o.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      //This should never happen
      throw new RuntimeException("UTF-8 is not supported by this system?", e);
    }
  }
  
  /**
   * Get the proxied path for an application.
   * @param id the application id to use.
   * @return the base path to that application through the proxy.
   */
  public static String getPath(ApplicationId id) {
    if(id == null) {
      throw new IllegalArgumentException("Application id cannot be null ");
    }
    return ujoin(PROXY_BASE, uriEncode(id));
  }

  /**
   * Get the proxied path for an application.
   * @param id the application id to use.
   * @param path the rest of the path to the application.
   * @return the base path to that application through the proxy.
   */
  public static String getPath(ApplicationId id, String path) {
    if(path == null) {
      return getPath(id);
    } else {
      return ujoin(getPath(id), path);
    }
  }
  
  /**
   * Get the proxied path for an application
   * @param id the id of the application
   * @param path the path of the application.
   * @param query the query parameters
   * @param approved true if the user has approved accessing this app.
   * @return the proxied path for this app.
   */
  public static String getPathAndQuery(ApplicationId id, String path, 
      String query, boolean approved) {
    StringBuilder newp = new StringBuilder();
    newp.append(getPath(id, path));
    boolean first = appendQuery(newp, query, true);
    if(approved) {
      appendQuery(newp, PROXY_APPROVAL_PARAM+"=true", first);
    }
    return newp.toString();
  }
  
  private static boolean appendQuery(StringBuilder builder, String query, 
      boolean first) {
    if(query != null && !query.isEmpty()) {
      if(first && !query.startsWith("?")) {
        builder.append('?');
      }
      if(!first && !query.startsWith("&")) {
        builder.append('&');
      }
      builder.append(query);
      return false;
    }
    return first;
  }
  
  /**
   * Get a proxied URI for the original URI.
   * @param originalUri the original URI to go through the proxy, or null if
   * a default path "/" can be used. 
   * @param proxyUri the URI of the proxy itself, scheme, host and port are used.
   * @param id the id of the application
   * @return the proxied URI
   */
  public static URI getProxyUri(URI originalUri, URI proxyUri,
      ApplicationId id) {
    try {
      String path = getPath(id, originalUri == null ? "/" : originalUri.getPath());
      return new URI(proxyUri.getScheme(), proxyUri.getAuthority(), path,
          originalUri == null ? null : originalUri.getQuery(),
          originalUri == null ? null : originalUri.getFragment());
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not proxify "+originalUri,e);
    }
  }
  
  /**
   * Create a URI form a no scheme Url, such as is returned by the AM.
   * @param noSchemeUrl the URL formate returned by an AM
   * @return a URI with an http scheme
   * @throws URISyntaxException if the url is not formatted correctly.
   */
  public static URI getUriFromAMUrl(String scheme, String noSchemeUrl)
      throws URISyntaxException {
      if (getSchemeFromUrl(noSchemeUrl).isEmpty()) {
        /*
         * check is made to make sure if AM reports with scheme then it will be
         * used by default otherwise it will default to the one configured using
         * "yarn.http.policy".
         */
        return new URI(scheme + noSchemeUrl);
      } else {
        return new URI(noSchemeUrl);
      }
    }

  /**
   * Returns the first valid tracking link, if any, from the given id from the
   * given list of plug-ins, if any.
   * 
   * @param id the id of the application for which the tracking link is desired
   * @param trackingUriPlugins list of plugins from which to get the tracking link
   * @return the desired link if possible, otherwise null
   * @throws URISyntaxException
   */
  public static URI getUriFromTrackingPlugins(ApplicationId id,
      List<TrackingUriPlugin> trackingUriPlugins)
      throws URISyntaxException {
    URI toRet = null;
    for(TrackingUriPlugin plugin : trackingUriPlugins)
    {
      toRet = plugin.getTrackingUri(id);
      if (toRet != null)
      {
        return toRet;
      }
    }
    return null;
  }
  
  /**
   * Returns the scheme if present in the url
   * eg. "https://issues.apache.org/jira/browse/YARN" > "https"
   */
  public static String getSchemeFromUrl(String url) {
    int index = 0;
    if (url != null) {
      index = url.indexOf("://");
    }
    if (index > 0) {
      return url.substring(0, index);
    } else {
      return "";
    }
  }
}
