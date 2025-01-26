/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.tos.auth;

import com.volcengine.tos.TosException;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES;
import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES_DEFAULT;

public class DefaultCredentialsProviderChain extends AbstractCredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCredentialsProviderChain.class);

  private final List<AbstractCredentialsProvider> providers = new LinkedList<>();
  private volatile AbstractCredentialsProvider lastUsedProvider;

  @Override
  public void initialize(Configuration config, String bucketName) {
    super.initialize(config, bucketName);
    loadAllCredentialProviders();
  }

  private void loadAllCredentialProviders() {
    for (String providerClazz : getCustomProviderClasses()) {
      try {
        Class<?> clazz = Class.forName(providerClazz);
        AbstractCredentialsProvider credentialsProvider =
            (AbstractCredentialsProvider) clazz.getDeclaredConstructor().newInstance();
        credentialsProvider.initialize(conf(), bucket());
        providers.add(credentialsProvider);
      } catch (Exception e) {
        LOG.error("Failed to initialize credential provider for {}", providerClazz, e);
        // throw exception directly since the configurations are invalid.
        throw new TosException(e);
      }
    }
  }

  private String[] getCustomProviderClasses() {
    String[] classes = conf().getStringCollection(FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES)
        .toArray(new String[0]);
    if (classes.length == 0) {
      classes = FS_TOS_CUSTOM_CREDENTIAL_PROVIDER_CLASSES_DEFAULT;
    }
    return classes;
  }

  @Override
  protected ExpireableCredential createCredential() {
    if (lastUsedProvider != null) {
      return lastUsedProvider.credential();
    } else {
      List<Exception> exceptions = new LinkedList<>();
      for (AbstractCredentialsProvider provider : providers) {
        try {
          ExpireableCredential credential = provider.credential();
          LOG.debug("Access credential from {} successfully, choose it as the candidate provider",
              provider.getClass().getName());
          lastUsedProvider = provider;
          return credential;
        } catch (Exception e) {
          LOG.debug("Failed to access credential from provider {}", provider.getClass().getName(),
              e);
          exceptions.add(e);
        }
      }
      String errorMsg = "Unable to load TOS credentials from any provider in the chain.";
      RuntimeException runtimeException = new RuntimeException(errorMsg);
      exceptions.forEach(runtimeException::addSuppressed);
      throw runtimeException;
    }
  }

  @VisibleForTesting
  AbstractCredentialsProvider lastUsedProvider() {
    Preconditions.checkNotNull(lastUsedProvider, "provider cannot be null");
    return lastUsedProvider;
  }
}
