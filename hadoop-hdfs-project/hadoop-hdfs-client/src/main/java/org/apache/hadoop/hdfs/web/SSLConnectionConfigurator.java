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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.ssl.SSLFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;

/**
 * Configure a connection to use SSL authentication.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving

public class SSLConnectionConfigurator implements ConnectionConfigurator {
  private final SSLFactory factory;
  private final SSLSocketFactory sf;
  private final HostnameVerifier hv;
  private final int connectTimeout;
  private final int readTimeout;

  SSLConnectionConfigurator(int connectTimeout, int readTimeout,
      Configuration conf) throws IOException, GeneralSecurityException {
    factory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
    factory.init();
    sf = factory.createSSLSocketFactory();
    hv = factory.getHostnameVerifier();
    this.connectTimeout = connectTimeout;
    this.readTimeout = readTimeout;
  }

  @Override
  public HttpURLConnection configure(HttpURLConnection conn) {
    if (conn instanceof HttpsURLConnection) {
      HttpsURLConnection c = (HttpsURLConnection) conn;
      c.setSSLSocketFactory(sf);
      c.setHostnameVerifier(hv);
    }
    conn.setConnectTimeout(connectTimeout);
    conn.setReadTimeout(readTimeout);
    return conn;
  }

  void destroy() {
    factory.destroy();
  }
}
