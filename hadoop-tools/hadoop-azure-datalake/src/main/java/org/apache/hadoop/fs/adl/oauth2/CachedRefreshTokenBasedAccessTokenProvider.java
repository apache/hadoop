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
 *
 */

package org.apache.hadoop.fs.adl.oauth2;

import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.oauth2.AccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.ConfRefreshTokenBasedAccessTokenProvider;
import org.apache.hadoop.hdfs.web.oauth2.PrivateCachedRefreshTokenBasedAccessTokenProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_CLIENT_ID_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.OAUTH_REFRESH_URL_KEY;
import static org.apache.hadoop.hdfs.web.oauth2.ConfRefreshTokenBasedAccessTokenProvider.OAUTH_REFRESH_TOKEN_KEY;

/**
 * Share refresh tokens across all ADLS instances with a common client ID. The
 * {@link AccessTokenProvider} can be shared across multiple instances,
 * amortizing the cost of refreshing tokens.
 */
public class CachedRefreshTokenBasedAccessTokenProvider
    extends PrivateCachedRefreshTokenBasedAccessTokenProvider {

  public static final String FORCE_REFRESH = "adl.force.token.refresh";

  private static final Logger LOG =
      LoggerFactory.getLogger(CachedRefreshTokenBasedAccessTokenProvider.class);

  /** Limit size of provider cache. */
  static final int MAX_PROVIDERS = 10;
  @SuppressWarnings("serial")
  private static final Map<String, AccessTokenProvider> CACHE =
      new LinkedHashMap<String, AccessTokenProvider>() {
        @Override
        public boolean removeEldestEntry(
            Map.Entry<String, AccessTokenProvider> e) {
          return size() > MAX_PROVIDERS;
        }
      };

  private AccessTokenProvider instance = null;

  /**
   * Create handle for cached instance.
   */
  public CachedRefreshTokenBasedAccessTokenProvider() {
  }

  /**
   * Gets the access token from internally cached
   * ConfRefreshTokenBasedAccessTokenProvider instance.
   *
   * @return Valid OAuth2 access token for the user.
   * @throws IOException when system error, internal server error or user error
   */
  @Override
  public synchronized String getAccessToken() throws IOException {
    return instance.getAccessToken();
  }

  /**
   * @return A cached Configuration consistent with the parameters of this
   * instance.
   */
  @Override
  public synchronized Configuration getConf() {
    return instance.getConf();
  }

  /**
   * Configure cached instance. Note that the Configuration instance returned
   * from subsequent calls to {@link #getConf() getConf} may be from a
   * previous, cached entry.
   * @param conf Configuration instance
   */
  @Override
  public synchronized void setConf(Configuration conf) {
    String id = conf.get(OAUTH_CLIENT_ID_KEY);
    if (null == id) {
      throw new IllegalArgumentException("Missing client ID");
    }
    synchronized (CACHE) {
      instance = CACHE.get(id);
      if (null == instance
          || conf.getBoolean(FORCE_REFRESH, false)
          || replace(instance, conf)) {
        instance = newInstance();
        // clone configuration
        instance.setConf(new Configuration(conf));
        CACHE.put(id, instance);
        LOG.debug("Created new client {}", id);
      }
    }
  }

  AccessTokenProvider newInstance() {
    return new ConfRefreshTokenBasedAccessTokenProvider();
  }

  private static boolean replace(AccessTokenProvider cached, Configuration c2) {
    // ConfRefreshTokenBasedAccessTokenProvider::setConf asserts !null
    final Configuration c1 = cached.getConf();
    for (String key : new String[] {
        OAUTH_REFRESH_TOKEN_KEY, OAUTH_REFRESH_URL_KEY }) {
      if (!c1.get(key).equals(c2.get(key))) {
        // replace cached instance for this clientID
        return true;
      }
    }
    return false;
  }

}
