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
 * Interface for providing BCE (Baidu Cloud Engine)
 * credentials. Implementations supply access keys and
 * optional session tokens for authenticating with BOS.
 */
public interface BceCredentialsProvider {

  /** Logger for credentials provider operations. */
  Logger LOG = LoggerFactory.getLogger(
      BceCredentialsProvider.class);

  /**
   * Creates a credentials provider instance based on the
   * class name specified in the Hadoop configuration.
   *
   * @param configuration the Hadoop configuration
   * @return the credentials provider instance
   */
  static BceCredentialsProvider
      getBceCredentialsProviderImpl(
          Configuration configuration) {
    try {
      String className = configuration.get(
          BaiduBosConstants.BOS_CREDENTIALS_PROVIDER,
          BaiduBosConstants
              .BOS_CONFIG_CREDENTIALS_PROVIDER);
      LOG.debug(
          "getBceCredentialsProviderImpl use"
              + " configuration value: {}",
          className);
      Class<?> providerClass =
          Class.forName(className);
      BceCredentialsProvider provider =
          (BceCredentialsProvider) providerClass
              .getDeclaredConstructor().newInstance();
      provider.setConf(configuration);
      return provider;
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets credentials for the specified user.
   *
   * @param user the user name
   * @return the session credentials
   */
  default DefaultBceSessionCredentials getCredentials(
      String user) {
    throw new UnsupportedOperationException(
        "getCredentials(String) not implemented");
  }

  /**
   * Gets credentials for the specified URI and user.
   *
   * @param uri  the filesystem URI, may be null
   * @param user the user name
   * @return the session credentials
   */
  default DefaultBceSessionCredentials getCredentials(
      URI uri, String user) {
    return getCredentials(user);
  }

  /**
   * Gets credentials by user ID.
   *
   * @param userId the user ID
   * @return the session credentials
   * @throws UnsupportedOperationException by default
   */
  default DefaultBceSessionCredentials getCredentialsById(
      String userId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets credentials by URI and user ID.
   *
   * @param uri    the filesystem URI, may be null
   * @param userId the user ID
   * @return the session credentials
   */
  default DefaultBceSessionCredentials getCredentialsById(
      URI uri, String userId) {
    return getCredentialsById(userId);
  }

  /**
   * Sets the Hadoop configuration on this provider.
   *
   * @param configs the Hadoop configuration
   */
  default void setConf(Configuration configs) {
    // do nothing default
  }
}
