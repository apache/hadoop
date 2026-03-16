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
import org.apache.hadoop.fs.bos.BaiduBosConstants;
import org.apache.hadoop.fs.bos.BaiduBosFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Credentials provider that reads access key, secret key,
 * and optional session token from environment variables.
 */
public class EnvironmentVariableCredentialsProvider
    implements BceCredentialsProvider {

  /**
   * Constructs an EnvironmentVariableCredentialsProvider.
   */
  public EnvironmentVariableCredentialsProvider() {
  }

  /** Logger for this provider. */
  public static final Logger LOG =
      LoggerFactory.getLogger(
          BaiduBosFileSystem.class);

  /**
   * Gets credentials from environment variables.
   *
   * @param uri  the filesystem URI, may be null
   * @param user the user name
   * @return the session credentials, or null if access key
   *         or secret key is missing
   */
  public DefaultBceSessionCredentials getCredentials(
      URI uri, String user) {
    String accessKey =
        System.getenv(BaiduBosConstants.BOS_AK_ENV);
    String secretAccessKey =
        System.getenv(BaiduBosConstants.BOS_SK_ENV);
    String sessionToken = System.getenv(
        BaiduBosConstants.BOS_STS_TOKEN_ENV);

    if (LOG.isDebugEnabled()) {
      LOG.debug("accessKey:{}", accessKey);
      LOG.debug("secretAccessKey:{}", secretAccessKey);
      LOG.debug("sessionToken:{}", sessionToken);
    }
    if (accessKey == null
        || secretAccessKey == null) {
      LOG.error(
          "accessKey and secretAccessKey"
              + " should not be null");
      return null;
    }

    if (sessionToken == null) {
      return new DefaultBceSessionCredentials(
          accessKey, secretAccessKey, " ");
    }
    return new DefaultBceSessionCredentials(
        accessKey, secretAccessKey, sessionToken);
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
