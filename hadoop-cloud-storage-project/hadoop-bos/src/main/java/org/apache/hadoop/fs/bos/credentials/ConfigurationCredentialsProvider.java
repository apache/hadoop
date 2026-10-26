/*
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

package org.apache.hadoop.fs.bos.credentials;

import com.baidubce.auth.DefaultBceSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.bos.BaiduBosConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Credentials provider that reads access key, secret key,
 * and optional session token from the Hadoop configuration.
 */
public class ConfigurationCredentialsProvider
    implements HadoopCredentialsProvider {

  /**
   * Constructs a ConfigurationCredentialsProvider.
   */
  public ConfigurationCredentialsProvider() {
  }

  private final Logger log =
      LoggerFactory.getLogger(
          ConfigurationCredentialsProvider.class);
  private Configuration conf;

  /**
   * Sets the Hadoop configuration.
   *
   * @param conf the Hadoop configuration
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Gets credentials from the Hadoop configuration. If a
   * bucket-specific configuration exists for the given URI,
   * it takes precedence.
   *
   * @param uri  the filesystem URI, may be null
   * @param user the user name
   * @return the session credentials
   * @throws IllegalArgumentException if access key or
   *         secret key is null
   */
  @Override
  public DefaultBceSessionCredentials getCredentials(
      URI uri, String user) {
    String accessKey;
    String secretAccessKey;
    String sessionToken;
    if (uri != null) {
      String host = uri.getHost();
      accessKey = conf.get(
          "fs.bos.bucket." + host + ".access.key",
          null);
      secretAccessKey = conf.get(
          "fs.bos.bucket." + host
              + ".secret.access.key",
          null);
      sessionToken = conf.get(
          "fs.bos.bucket." + host
              + ".session.token.key",
          null);
      if (accessKey != null
          && secretAccessKey != null) {
        if (sessionToken == null) {
          return new DefaultBceSessionCredentials(
              accessKey, secretAccessKey, " ");
        }
        return new DefaultBceSessionCredentials(
            accessKey, secretAccessKey, sessionToken);
      }
    }

    accessKey = conf.get(
        BaiduBosConstants.BOS_ACCESS_KEY, null);
    secretAccessKey = conf.get(
        BaiduBosConstants.BOS_SECRET_KEY, null);
    sessionToken = conf.get(
        BaiduBosConstants.BOS_SESSION_TOKEN, null);

    if (accessKey == null
        || secretAccessKey == null) {
      throw new IllegalArgumentException(
          "accessKey and secretAccessKey"
              + " should not be null");
    }

    if (sessionToken == null
        || sessionToken.trim().isEmpty()) {
      return new DefaultBceSessionCredentials(
          accessKey, secretAccessKey, " ");
    } else {
      return new DefaultBceSessionCredentials(
          accessKey, secretAccessKey, sessionToken);
    }
  }

  /**
   * Gets credentials by user ID, delegating to
   * {@link #getCredentials(URI, String)}.
   *
   * @param uri    the filesystem URI
   * @param userId the user ID
   * @return the session credentials
   */
  @Override
  public DefaultBceSessionCredentials getCredentialsById(
      URI uri, String userId) {
    return getCredentials(uri, userId);
  }
}
