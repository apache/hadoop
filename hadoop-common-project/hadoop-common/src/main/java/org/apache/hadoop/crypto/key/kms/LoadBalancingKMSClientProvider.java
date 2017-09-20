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

package org.apache.hadoop.crypto.key.kms;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A simple LoadBalancing KMSClientProvider that round-robins requests
 * across a provided array of KMSClientProviders. It also retries failed
 * requests on the next available provider in the load balancer group. It
 * only retries failed requests that result in an IOException, sending back
 * all other Exceptions to the caller without retry.
 */
public class LoadBalancingKMSClientProvider extends KeyProvider implements
    CryptoExtension,
    KeyProviderDelegationTokenExtension.DelegationTokenExtension {

  public static Logger LOG =
      LoggerFactory.getLogger(LoadBalancingKMSClientProvider.class);

  static interface ProviderCallable<T> {
    public T call(KMSClientProvider provider) throws IOException, Exception;
  }

  @SuppressWarnings("serial")
  static class WrapperException extends RuntimeException {
    public WrapperException(Throwable cause) {
      super(cause);
    }
  }

  private final KMSClientProvider[] providers;
  private final AtomicInteger currentIdx;

  public LoadBalancingKMSClientProvider(KMSClientProvider[] providers,
      Configuration conf) {
    this(shuffle(providers), Time.monotonicNow(), conf);
  }

  @VisibleForTesting
  LoadBalancingKMSClientProvider(KMSClientProvider[] providers, long seed,
      Configuration conf) {
    super(conf);
    this.providers = providers;
    this.currentIdx = new AtomicInteger((int)(seed % providers.length));
  }

  @VisibleForTesting
  KMSClientProvider[] getProviders() {
    return providers;
  }

  private <T> T doOp(ProviderCallable<T> op, int currPos)
      throws IOException {
    IOException ex = null;
    for (int i = 0; i < providers.length; i++) {
      KMSClientProvider provider = providers[(currPos + i) % providers.length];
      try {
        return op.call(provider);
      } catch (IOException ioe) {
        LOG.warn("KMS provider at [{}] threw an IOException!! {}",
            provider.getKMSUrl(), StringUtils.stringifyException(ioe));
        ex = ioe;
      } catch (Exception e) {
        if (e instanceof RuntimeException) {
          throw (RuntimeException)e;
        } else {
          throw new WrapperException(e);
        }
      }
    }
    if (ex != null) {
      LOG.warn("Aborting since the Request has failed with all KMS"
          + " providers in the group. !!");
      throw ex;
    }
    throw new IOException("No providers configured !!");
  }

  private int nextIdx() {
    while (true) {
      int current = currentIdx.get();
      int next = (current + 1) % providers.length;
      if (currentIdx.compareAndSet(current, next)) {
        return current;
      }
    }
  }

  @Override
  public Token<?>[]
      addDelegationTokens(final String renewer, final Credentials credentials)
          throws IOException {
    return doOp(new ProviderCallable<Token<?>[]>() {
      @Override
      public Token<?>[] call(KMSClientProvider provider) throws IOException {
        return provider.addDelegationTokens(renewer, credentials);
      }
    }, nextIdx());
  }

  @Override
  public long renewDelegationToken(final Token<?> token) throws IOException {
    return doOp(new ProviderCallable<Long>() {
      @Override
      public Long call(KMSClientProvider provider) throws IOException {
        return provider.renewDelegationToken(token);
      }
    }, nextIdx());
  }

  @Override
  public Void cancelDelegationToken(final Token<?> token) throws IOException {
    return doOp(new ProviderCallable<Void>() {
      @Override
      public Void call(KMSClientProvider provider) throws IOException {
        provider.cancelDelegationToken(token);
        return null;
      }
    }, nextIdx());
  }

  // This request is sent to all providers in the load-balancing group
  @Override
  public void warmUpEncryptedKeys(String... keyNames) throws IOException {
    Preconditions.checkArgument(providers.length > 0,
        "No providers are configured");
    boolean success = false;
    IOException e = null;
    for (KMSClientProvider provider : providers) {
      try {
        provider.warmUpEncryptedKeys(keyNames);
        success = true;
      } catch (IOException ioe) {
        e = ioe;
        LOG.error(
            "Error warming up keys for provider with url"
            + "[" + provider.getKMSUrl() + "]", ioe);
      }
    }
    if (!success && e != null) {
      throw e;
    }
  }

  // This request is sent to all providers in the load-balancing group
  @Override
  public void drain(String keyName) {
    for (KMSClientProvider provider : providers) {
      provider.drain(keyName);
    }
  }

  // This request is sent to all providers in the load-balancing group
  @Override
  public void invalidateCache(String keyName) throws IOException {
    for (KMSClientProvider provider : providers) {
      provider.invalidateCache(keyName);
    }
  }

  @Override
  public EncryptedKeyVersion
      generateEncryptedKey(final String encryptionKeyName)
          throws IOException, GeneralSecurityException {
    try {
      return doOp(new ProviderCallable<EncryptedKeyVersion>() {
        @Override
        public EncryptedKeyVersion call(KMSClientProvider provider)
            throws IOException, GeneralSecurityException {
          return provider.generateEncryptedKey(encryptionKeyName);
        }
      }, nextIdx());
    } catch (WrapperException we) {
      if (we.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) we.getCause();
      }
      throw new IOException(we.getCause());
    }
  }

  @Override
  public KeyVersion
      decryptEncryptedKey(final EncryptedKeyVersion encryptedKeyVersion)
          throws IOException, GeneralSecurityException {
    try {
      return doOp(new ProviderCallable<KeyVersion>() {
        @Override
        public KeyVersion call(KMSClientProvider provider)
            throws IOException, GeneralSecurityException {
          return provider.decryptEncryptedKey(encryptedKeyVersion);
        }
      }, nextIdx());
    } catch (WrapperException we) {
      if (we.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) we.getCause();
      }
      throw new IOException(we.getCause());
    }
  }

  @Override
  public EncryptedKeyVersion reencryptEncryptedKey(
      final EncryptedKeyVersion ekv)
      throws IOException, GeneralSecurityException {
    try {
      return doOp(new ProviderCallable<EncryptedKeyVersion>() {
        @Override
        public EncryptedKeyVersion call(KMSClientProvider provider)
            throws IOException, GeneralSecurityException {
          return provider.reencryptEncryptedKey(ekv);
        }
      }, nextIdx());
    } catch (WrapperException we) {
      if (we.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) we.getCause();
      }
      throw new IOException(we.getCause());
    }
  }

  @Override
  public void reencryptEncryptedKeys(final List<EncryptedKeyVersion> ekvs)
      throws IOException, GeneralSecurityException {
    try {
      doOp(new ProviderCallable<Void>() {
        @Override
        public Void call(KMSClientProvider provider)
            throws IOException, GeneralSecurityException {
          provider.reencryptEncryptedKeys(ekvs);
          return null;
        }
      }, nextIdx());
    } catch (WrapperException we) {
      if (we.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) we.getCause();
      }
      throw new IOException(we.getCause());
    }
  }

  @Override
  public KeyVersion getKeyVersion(final String versionName) throws IOException {
    return doOp(new ProviderCallable<KeyVersion>() {
      @Override
      public KeyVersion call(KMSClientProvider provider) throws IOException {
        return provider.getKeyVersion(versionName);
      }
    }, nextIdx());
  }

  @Override
  public List<String> getKeys() throws IOException {
    return doOp(new ProviderCallable<List<String>>() {
      @Override
      public List<String> call(KMSClientProvider provider) throws IOException {
        return provider.getKeys();
      }
    }, nextIdx());
  }

  @Override
  public Metadata[] getKeysMetadata(final String... names) throws IOException {
    return doOp(new ProviderCallable<Metadata[]>() {
      @Override
      public Metadata[] call(KMSClientProvider provider) throws IOException {
        return provider.getKeysMetadata(names);
      }
    }, nextIdx());
  }

  @Override
  public List<KeyVersion> getKeyVersions(final String name) throws IOException {
    return doOp(new ProviderCallable<List<KeyVersion>>() {
      @Override
      public List<KeyVersion> call(KMSClientProvider provider)
          throws IOException {
        return provider.getKeyVersions(name);
      }
    }, nextIdx());
  }

  @Override
  public KeyVersion getCurrentKey(final String name) throws IOException {
    return doOp(new ProviderCallable<KeyVersion>() {
      @Override
      public KeyVersion call(KMSClientProvider provider) throws IOException {
        return provider.getCurrentKey(name);
      }
    }, nextIdx());
  }
  @Override
  public Metadata getMetadata(final String name) throws IOException {
    return doOp(new ProviderCallable<Metadata>() {
      @Override
      public Metadata call(KMSClientProvider provider) throws IOException {
        return provider.getMetadata(name);
      }
    }, nextIdx());
  }

  @Override
  public KeyVersion createKey(final String name, final byte[] material,
      final Options options) throws IOException {
    return doOp(new ProviderCallable<KeyVersion>() {
      @Override
      public KeyVersion call(KMSClientProvider provider) throws IOException {
        return provider.createKey(name, material, options);
      }
    }, nextIdx());
  }

  @Override
  public KeyVersion createKey(final String name, final Options options)
      throws NoSuchAlgorithmException, IOException {
    try {
      return doOp(new ProviderCallable<KeyVersion>() {
        @Override
        public KeyVersion call(KMSClientProvider provider) throws IOException,
            NoSuchAlgorithmException {
          return provider.createKey(name, options);
        }
      }, nextIdx());
    } catch (WrapperException e) {
      if (e.getCause() instanceof GeneralSecurityException) {
        throw (NoSuchAlgorithmException) e.getCause();
      }
      throw new IOException(e.getCause());
    }
  }

  @Override
  public void deleteKey(final String name) throws IOException {
    doOp(new ProviderCallable<Void>() {
      @Override
      public Void call(KMSClientProvider provider) throws IOException {
        provider.deleteKey(name);
        return null;
      }
    }, nextIdx());
  }

  @Override
  public KeyVersion rollNewVersion(final String name, final byte[] material)
      throws IOException {
    final KeyVersion newVersion = doOp(new ProviderCallable<KeyVersion>() {
      @Override
      public KeyVersion call(KMSClientProvider provider) throws IOException {
        return provider.rollNewVersion(name, material);
      }
    }, nextIdx());
    invalidateCache(name);
    return newVersion;
  }

  @Override
  public KeyVersion rollNewVersion(final String name)
      throws NoSuchAlgorithmException, IOException {
    try {
      final KeyVersion newVersion = doOp(new ProviderCallable<KeyVersion>() {
        @Override
        public KeyVersion call(KMSClientProvider provider) throws IOException,
            NoSuchAlgorithmException {
          return provider.rollNewVersion(name);
        }
      }, nextIdx());
      invalidateCache(name);
      return newVersion;
    } catch (WrapperException e) {
      if (e.getCause() instanceof GeneralSecurityException) {
        throw (NoSuchAlgorithmException) e.getCause();
      }
      throw new IOException(e.getCause());
    }
  }

  // Close all providers in the LB group
  @Override
  public void close() throws IOException {
    for (KMSClientProvider provider : providers) {
      try {
        provider.close();
      } catch (IOException ioe) {
        LOG.error("Error closing provider with url"
            + "[" + provider.getKMSUrl() + "]");
      }
    }
  }


  @Override
  public void flush() throws IOException {
    for (KMSClientProvider provider : providers) {
      try {
        provider.flush();
      } catch (IOException ioe) {
        LOG.error("Error flushing provider with url"
            + "[" + provider.getKMSUrl() + "]");
      }
    }
  }

  private static KMSClientProvider[] shuffle(KMSClientProvider[] providers) {
    List<KMSClientProvider> list = Arrays.asList(providers);
    Collections.shuffle(list);
    return list.toArray(providers);
  }
}
