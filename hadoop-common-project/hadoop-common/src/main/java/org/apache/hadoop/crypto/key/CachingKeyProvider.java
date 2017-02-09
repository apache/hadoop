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
package org.apache.hadoop.crypto.key;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A <code>KeyProviderExtension</code> implementation providing a short lived
 * cache for <code>KeyVersions</code> and <code>Metadata</code>to avoid burst
 * of requests to hit the underlying <code>KeyProvider</code>.
 */
public class CachingKeyProvider extends
    KeyProviderExtension<CachingKeyProvider.CacheExtension> {

  static class CacheExtension implements KeyProviderExtension.Extension {
    private final KeyProvider provider;
    private LoadingCache<String, KeyVersion> keyVersionCache;
    private LoadingCache<String, KeyVersion> currentKeyCache;
    private LoadingCache<String, Metadata> keyMetadataCache;

    CacheExtension(KeyProvider prov, long keyTimeoutMillis,
        long currKeyTimeoutMillis) {
      this.provider = prov;
      keyVersionCache =
          CacheBuilder.newBuilder().expireAfterAccess(keyTimeoutMillis,
              TimeUnit.MILLISECONDS)
              .build(new CacheLoader<String, KeyVersion>() {
                @Override
                public KeyVersion load(String key) throws Exception {
                  KeyVersion kv = provider.getKeyVersion(key);
                  if (kv == null) {
                    throw new KeyNotFoundException();
                  }
                  return kv;
                }
              });
      keyMetadataCache =
          CacheBuilder.newBuilder().expireAfterAccess(keyTimeoutMillis,
              TimeUnit.MILLISECONDS)
              .build(new CacheLoader<String, Metadata>() {
                @Override
                public Metadata load(String key) throws Exception {
                  Metadata meta = provider.getMetadata(key);
                  if (meta == null) {
                    throw new KeyNotFoundException();
                  }
                  return meta;
                }
              });
      currentKeyCache =
          CacheBuilder.newBuilder().expireAfterWrite(currKeyTimeoutMillis,
          TimeUnit.MILLISECONDS)
          .build(new CacheLoader<String, KeyVersion>() {
            @Override
            public KeyVersion load(String key) throws Exception {
              KeyVersion kv = provider.getCurrentKey(key);
              if (kv == null) {
                throw new KeyNotFoundException();
              }
              return kv;
            }
          });
    }
  }

  @SuppressWarnings("serial")
  private static class KeyNotFoundException extends Exception { }

  public CachingKeyProvider(KeyProvider keyProvider, long keyTimeoutMillis,
      long currKeyTimeoutMillis) {
    super(keyProvider, new CacheExtension(keyProvider, keyTimeoutMillis,
        currKeyTimeoutMillis));
  }

  @Override
  public KeyVersion getCurrentKey(String name) throws IOException {
    try {
      return getExtension().currentKeyCache.get(name);
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof KeyNotFoundException) {
        return null;
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }
  }

  @Override
  public KeyVersion getKeyVersion(String versionName)
      throws IOException {
    try {
      return getExtension().keyVersionCache.get(versionName);
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof KeyNotFoundException) {
        return null;
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }
  }

  @Override
  public void deleteKey(String name) throws IOException {
    getKeyProvider().deleteKey(name);
    getExtension().currentKeyCache.invalidate(name);
    getExtension().keyMetadataCache.invalidate(name);
    // invalidating all key versions as we don't know
    // which ones belonged to the deleted key
    getExtension().keyVersionCache.invalidateAll();
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    KeyVersion key = getKeyProvider().rollNewVersion(name, material);
    invalidateCache(name);
    return key;
  }

  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    KeyVersion key = getKeyProvider().rollNewVersion(name);
    invalidateCache(name);
    return key;
  }

  @Override
  public void invalidateCache(String name) throws IOException {
    getKeyProvider().invalidateCache(name);
    getExtension().currentKeyCache.invalidate(name);
    getExtension().keyMetadataCache.invalidate(name);
    // invalidating all key versions as we don't know
    // which ones belonged to the deleted key
    getExtension().keyVersionCache.invalidateAll();
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    try {
      return getExtension().keyMetadataCache.get(name);
    } catch (ExecutionException ex) {
      Throwable cause = ex.getCause();
      if (cause instanceof KeyNotFoundException) {
        return null;
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new IOException(cause);
      }
    }
  }

}
