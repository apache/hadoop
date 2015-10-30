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
import org.apache.hadoop.util.Timer;

import static org.apache.hadoop.hdfs.web.oauth2.Utils.notNull;

/**
 * Obtain an access token via a a credential (provided through the
 * Configuration) using the
 * <a href="https://tools.ietf.org/html/rfc6749#section-4.4">
 *   Client Credentials Grant workflow</a>.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ConfCredentialBasedAccessTokenProvider
    extends CredentialBasedAccessTokenProvider {
  private String credential;

  public ConfCredentialBasedAccessTokenProvider() {
  }

  public ConfCredentialBasedAccessTokenProvider(Timer timer) {
    super(timer);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    credential = notNull(conf, OAUTH_CREDENTIAL_KEY);
  }

  @Override
  public String getCredential() {
    if(credential == null) {
      throw new IllegalArgumentException("Credential has not been " +
          "provided in configuration");
    }

    return credential;
  }
}
