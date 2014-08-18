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

package org.apache.hadoop.yarn.server.security.http;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;

@Unstable
public class RMAuthenticationFilterInitializer extends FilterInitializer {

  String configPrefix;
  String signatureSecretFileProperty;
  String kerberosPrincipalProperty;
  String cookiePath;

  public RMAuthenticationFilterInitializer() {
    this.configPrefix = "hadoop.http.authentication.";
    this.signatureSecretFileProperty =
        AuthenticationFilter.SIGNATURE_SECRET + ".file";
    this.kerberosPrincipalProperty = KerberosAuthenticationHandler.PRINCIPAL;
    this.cookiePath = "/";
  }

  protected Map<String, String> createFilterConfig(Configuration conf) {
    Map<String, String> filterConfig = new HashMap<String, String>();

    // setting the cookie path to root '/' so it is used for all resources.
    filterConfig.put(AuthenticationFilter.COOKIE_PATH, cookiePath);

    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(configPrefix)) {
        String value = conf.get(name);
        name = name.substring(configPrefix.length());
        filterConfig.put(name, value);
      }
    }

    String signatureSecretFile = filterConfig.get(signatureSecretFileProperty);
    if (signatureSecretFile != null) {
      Reader reader = null;
      try {
        StringBuilder secret = new StringBuilder();
        reader =
            new InputStreamReader(new FileInputStream(signatureSecretFile),
              "UTF-8");
        int c = reader.read();
        while (c > -1) {
          secret.append((char) c);
          c = reader.read();
        }
        filterConfig.put(AuthenticationFilter.SIGNATURE_SECRET,
          secret.toString());
      } catch (IOException ex) {
        // if running in non-secure mode, this filter only gets added
        // because the user has not setup his own filter so just generate
        // a random secret. in secure mode, the user needs to setup security
        if (UserGroupInformation.isSecurityEnabled()) {
          throw new RuntimeException(
            "Could not read HTTP signature secret file: " + signatureSecretFile);
        }
      } finally {
        IOUtils.closeQuietly(reader);
      }
    }

    // Resolve _HOST into bind address
    String bindAddress = conf.get(HttpServer2.BIND_ADDRESS);
    String principal = filterConfig.get(kerberosPrincipalProperty);
    if (principal != null) {
      try {
        principal = SecurityUtil.getServerPrincipal(principal, bindAddress);
      } catch (IOException ex) {
        throw new RuntimeException(
          "Could not resolve Kerberos principal name: " + ex.toString(), ex);
      }
      filterConfig.put(KerberosAuthenticationHandler.PRINCIPAL, principal);
    }
    return filterConfig;
  }

  @Override
  public void initFilter(FilterContainer container, Configuration conf) {

    Map<String, String> filterConfig = createFilterConfig(conf);
    container.addFilter("RMAuthenticationFilter",
      RMAuthenticationFilter.class.getName(), filterConfig);
  }

}
