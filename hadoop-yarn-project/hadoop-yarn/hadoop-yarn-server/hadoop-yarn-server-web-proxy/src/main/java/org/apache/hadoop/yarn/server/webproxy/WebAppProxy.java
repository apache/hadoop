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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.service.AbstractService;

import org.apache.hadoop.fs.CommonConfigurationKeys;

public class WebAppProxy extends AbstractService {
  public static final String FETCHER_ATTRIBUTE= "AppUrlFetcher";
  public static final String IS_SECURITY_ENABLED_ATTRIBUTE = "IsSecurityEnabled";
  public static final String PROXY_HOST_ATTRIBUTE = "proxyHost";
  private static final Log LOG = LogFactory.getLog(WebAppProxy.class);
  
  private HttpServer proxyServer = null;
  private String bindAddress = null;
  private int port = 0;
  private AccessControlList acl = null;
  private AppReportFetcher fetcher = null;
  private boolean isSecurityEnabled = false;
  private String proxyHost = null;
  
  public WebAppProxy() {
    super(WebAppProxy.class.getName());
  }
  
  @Override
  public void init(Configuration conf) {
    String auth =  conf.get(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION);
    if (auth == null || "simple".equals(auth)) {
      isSecurityEnabled = false;
    } else if ("kerberos".equals(auth)) {
      isSecurityEnabled = true;
    } else {
      LOG.warn("Unrecongized attribute value for " +
          CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION +
          " of " + auth);
    }
    String proxy = YarnConfiguration.getProxyHostAndPort(conf);
    String[] proxyParts = proxy.split(":");
    proxyHost = proxyParts[0];

    fetcher = new AppReportFetcher(conf);
    bindAddress = conf.get(YarnConfiguration.PROXY_ADDRESS);
    if(bindAddress == null || bindAddress.isEmpty()) {
      throw new YarnException(YarnConfiguration.PROXY_ADDRESS + 
          " is not set so the proxy will not run.");
    }
    LOG.info("Instantiating Proxy at " + bindAddress);
    String[] parts = StringUtils.split(bindAddress, ':');
    port = 0;
    if (parts.length == 2) {
      bindAddress = parts[0];
      port = Integer.parseInt(parts[1]);
    }
    acl = new AccessControlList(conf.get(YarnConfiguration.YARN_ADMIN_ACL, 
        YarnConfiguration.DEFAULT_YARN_ADMIN_ACL));
    super.init(conf);
  }
  
  @Override
  public void start() {
    try {
      proxyServer = new HttpServer("proxy", bindAddress, port,
          port == 0, getConfig(), acl);
      proxyServer.addServlet(ProxyUriUtils.PROXY_SERVLET_NAME, 
          ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
      proxyServer.setAttribute(FETCHER_ATTRIBUTE, fetcher);
      proxyServer.setAttribute(IS_SECURITY_ENABLED_ATTRIBUTE, isSecurityEnabled);
      proxyServer.setAttribute(PROXY_HOST_ATTRIBUTE, proxyHost);
      proxyServer.start();
    } catch (IOException e) {
      LOG.fatal("Could not start proxy web server",e);
      throw new YarnException("Could not start proxy web server",e);
    }
    super.start();
  }
  
  @Override
  public void stop() {
    if(proxyServer != null) {
      try {
        proxyServer.stop();
      } catch (Exception e) {
        LOG.fatal("Error stopping proxy web server", e);
        throw new YarnException("Error stopping proxy web server",e);
      }
    }
    super.stop();
  }

  public void join() {
    if(proxyServer != null) {
      try {
        proxyServer.join();
      } catch (InterruptedException e) {
      }
    }
  }
}
