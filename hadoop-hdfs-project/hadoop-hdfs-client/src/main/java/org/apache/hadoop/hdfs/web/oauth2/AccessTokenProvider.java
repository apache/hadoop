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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Provide an OAuth2 access token to be used to authenticate http calls in
 * WebHDFS.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AccessTokenProvider implements Configurable {
  private Configuration conf;

  /**
   * Obtain the access token that should be added to http connection's header.
   * Will be called for each connection, so implementations should be
   * performant. Implementations are responsible for any refreshing of
   * the token.
   *
   * @return Access token to be added to connection header.
   */
  public abstract String getAccessToken() throws IOException;

  /**
   * Return the conf.
   *
   * @return the conf.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Set the conf.
   *
   * @param configuration  New configuration.
   */
  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }
}
