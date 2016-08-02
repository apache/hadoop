/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.services.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractSecurityStoreGenerator implements
    SecurityStoreGenerator {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSecurityStoreGenerator.class);

  protected CertificateManager certificateMgr;

  public AbstractSecurityStoreGenerator(CertificateManager certificateMgr) {
    this.certificateMgr = certificateMgr;
  }

  protected String getStorePassword(Map<String, List<String>> credentials,
                                    MapOperations compOps, String role)
      throws SliderException, IOException {
    String password = getPassword(compOps);
    if (password == null) {
      // need to leverage credential provider
      String alias = getAlias(compOps);
      LOG.debug("Alias {} found for role {}", alias, role);
      if (alias == null) {
        throw new SliderException("No store password or credential provider "
                                  + "alias found");
      }
      if (credentials.isEmpty()) {
        LOG.info("Credentials can not be retrieved for store generation since "
                 + "no CP paths are configured");
      }
      synchronized (this) {
        for (Map.Entry<String, List<String>> cred : credentials.entrySet()) {
          String provider = cred.getKey();
          Configuration c = new Configuration();
          c.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, provider);
          LOG.debug("Configured provider {}", provider);
          CredentialProvider cp =
              CredentialProviderFactory.getProviders(c).get(0);
          LOG.debug("Aliases: {}", cp.getAliases());
          char[] credential = c.getPassword(alias);
          if (credential != null) {
            LOG.info("Credential found for role {}", role);
            return String.valueOf(credential);
          }
        }
      }

      if (password == null) {
        LOG.info("No store credential found for alias {}.  "
                 + "Generation of store for {} is not possible.", alias, role);

      }
    }

    return password;

  }

  @Override
  public boolean isStoreRequested(MapOperations compOps) {
    return compOps.getOptionBool(SliderKeys.COMP_STORES_REQUIRED_KEY, false);
  }

  abstract String getPassword(MapOperations compOps);

  abstract String getAlias(MapOperations compOps);
}
