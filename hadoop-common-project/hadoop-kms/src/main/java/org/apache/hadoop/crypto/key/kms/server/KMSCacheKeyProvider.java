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
package org.apache.hadoop.crypto.key.kms.server;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.crypto.key.KeyProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A <code>KeyProvider</code> proxy implementation providing a short lived
 * cache for <code>KeyVersions</code> to avoid burst of requests to hit the
 * underlying <code>KeyProvider</code>.
 */
public class KMSCacheKeyProvider extends KeyProvider {
  private final KeyProvider provider;
  private LoadingCache<String, KeyVersion> keyVersionCache;
  private LoadingCache<String, KeyVersion> currentKeyCache;

  private static class KeyNotFoundException extends Exception {
    private static final long serialVersionUID = 1L;
  }

  public KMSCacheKeyProvider(KeyProvider prov, long timeoutMillis) {
    this.provider =  prov;
    keyVersionCache = CacheBuilder.newBuilder().expireAfterAccess(timeoutMillis,
        TimeUnit.MILLISECONDS).build(new CacheLoader<String, KeyVersion>() {
      @Override
      public KeyVersion load(String key) throws Exception {
        KeyVersion kv = provider.getKeyVersion(key);
        if (kv == null) {
          throw new KeyNotFoundException();
        }
        return kv;
      }
    });
    // for current key we don't want to go stale for more than 1 sec
    currentKeyCache = CacheBuilder.newBuilder().expireAfterWrite(1000,
        TimeUnit.MILLISECONDS).build(new CacheLoader<String, KeyVersion>() {
      @Override
      public KeyVersion load(String key) throws Exception {
        KeyVersion kv =  provider.getCurrentKey(key);
        if (kv == null) {
          throw new KeyNotFoundException();
        }
        return kv;
      }
    });
  }

  @Override
  public KeyVersion getCurrentKey(String name) throws IOException {
    try {
      return currentKeyCache.get(name);
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
      return keyVersionCache.get(versionName);
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
  public List<String> getKeys() throws IOException {
    return provider.getKeys();
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name)
      throws IOException {
    return provider.getKeyVersions(name);
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    return provider.getMetadata(name);
  }

  @Override
  public KeyVersion createKey(String name, byte[] material,
      Options options) throws IOException {
    return provider.createKey(name, material, options);
  }

  @Override
  public KeyVersion createKey(String name,
      Options options)
      throws NoSuchAlgorithmException, IOException {
    return provider.createKey(name, options);
  }

  @Override
  public void deleteKey(String name) throws IOException {
    provider.deleteKey(name);
    currentKeyCache.invalidate(name);
    // invalidating all key versions as we don't know which ones belonged to the
    // deleted key
    keyVersionCache.invalidateAll();
  }

  @Override
  public KeyVersion rollNewVersion(String name, byte[] material)
      throws IOException {
    KeyVersion key = provider.rollNewVersion(name, material);
    currentKeyCache.invalidate(name);
    return key;
  }

  @Override
  public KeyVersion rollNewVersion(String name)
      throws NoSuchAlgorithmException, IOException {
    KeyVersion key = provider.rollNewVersion(name);
    currentKeyCache.invalidate(name);
    return key;
  }

  @Override
  public void flush() throws IOException {
    provider.flush();
  }

  @Override
  public Metadata[] getKeysMetadata(String ... keyNames)
      throws IOException {
    return provider.getKeysMetadata(keyNames);
  }

  @Override
  public boolean isTransient() {
    return provider.isTransient();
  }

}
