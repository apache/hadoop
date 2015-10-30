/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.HttpURLConnection;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.ACCESS_TOKEN_PROVIDER_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.Utils.notNull;

/**
 * Configure a connection to use OAuth2 authentication.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OAuth2ConnectionConfigurator implements ConnectionConfigurator {

  public static final String HEADER = "Bearer ";

  private final AccessTokenProvider accessTokenProvider;

  private ConnectionConfigurator sslConfigurator = null;

  public OAuth2ConnectionConfigurator(Configuration conf) {
    this(conf, null);
  }

  @SuppressWarnings("unchecked")
  public OAuth2ConnectionConfigurator(Configuration conf,
                                      ConnectionConfigurator sslConfigurator) {
    this.sslConfigurator = sslConfigurator;

    notNull(conf, ACCESS_TOKEN_PROVIDER_KEY);

    Class accessTokenProviderClass = conf.getClass(ACCESS_TOKEN_PROVIDER_KEY,
        ConfCredentialBasedAccessTokenProvider.class,
        AccessTokenProvider.class);

    accessTokenProvider = (AccessTokenProvider) ReflectionUtils
        .newInstance(accessTokenProviderClass, conf);
    accessTokenProvider.setConf(conf);
  }

  @Override
  public HttpURLConnection configure(HttpURLConnection conn)
      throws IOException {
    if(sslConfigurator != null) {
      sslConfigurator.configure(conn);
    }

    String accessToken = accessTokenProvider.getAccessToken();

    conn.setRequestProperty("AUTHORIZATION", HEADER + accessToken);

    return conn;
  }
}
