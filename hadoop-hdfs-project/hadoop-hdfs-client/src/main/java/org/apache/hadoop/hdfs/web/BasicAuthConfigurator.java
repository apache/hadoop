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
package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Base64;

/**
 * This class adds basic authentication to the connection,
 * allowing users to access webhdfs over HTTP with basic authentication,
 * for example when using Apache Knox.
 */
public class BasicAuthConfigurator implements ConnectionConfigurator {
  private final ConnectionConfigurator parent;
  private final String credentials;

  /**
   * @param credentials a string of the form "username:password"
   */
  public BasicAuthConfigurator(
      ConnectionConfigurator parent,
      String credentials
  ) {
    this.parent = parent;
    this.credentials = credentials;
  }

  @Override
  public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
    if (parent != null) {
      parent.configure(conn);
    }

    if (credentials != null && !credentials.equals("")) {
      conn.setRequestProperty(
          "AUTHORIZATION",
          "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes())
      );
    }

    return conn;
  }

  public void destroy() {
    if (parent != null && parent instanceof SSLConnectionConfigurator) {
      ((SSLConnectionConfigurator)parent).destroy();
    }
  }
}
