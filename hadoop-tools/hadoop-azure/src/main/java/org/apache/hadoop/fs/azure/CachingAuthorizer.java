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

package org.apache.hadoop.fs.azure;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * Class that provides caching for Authorize and getSasUri calls
 * @param <K> -  The cache key type
 * @param <V> - The cached value type
 */
public class CachingAuthorizer<K, V> {

  public static final Logger LOG = LoggerFactory
      .getLogger(CachingAuthorizer.class);

  private Cache<K, V> cache;
  private boolean isEnabled = false;
  private long cacheEntryExpiryPeriodInMinutes;
  private String label;

  public static final String KEY_AUTH_SERVICE_CACHING_ENABLE =
      "fs.azure.authorization.caching.enable";

  public static final boolean KEY_AUTH_SERVICE_CACHING_ENABLE_DEFAULT = false;

  public static final String KEY_AUTH_SERVICE_CACHING_MAX_ENTRIES =
      "fs.azure.authorization.caching.maxentries";

  public static final int KEY_AUTH_SERVICE_CACHING_MAX_ENTRIES_DEFAULT = 512;

  public CachingAuthorizer(long ttlInMinutes, String label) {
    cacheEntryExpiryPeriodInMinutes = ttlInMinutes;
    this.label = label;
    if (cacheEntryExpiryPeriodInMinutes <= 0) {
      isEnabled = false;
    }
  }


  public void init(Configuration conf) {

    isEnabled = conf.getBoolean(KEY_AUTH_SERVICE_CACHING_ENABLE, KEY_AUTH_SERVICE_CACHING_ENABLE_DEFAULT);

    if (isEnabled) {
      LOG.debug("{} : Initializing CachingAuthorizer instance", label);
      cache = CacheBuilder.newBuilder()
          .maximumSize(
              conf.getInt(
                  KEY_AUTH_SERVICE_CACHING_MAX_ENTRIES,
                  KEY_AUTH_SERVICE_CACHING_MAX_ENTRIES_DEFAULT
              )
          )
          .expireAfterWrite(cacheEntryExpiryPeriodInMinutes, TimeUnit.MINUTES)
          .build();
    }
  }

  /**
   * @param key - Cache key
   * @return null on cache-miss. true/false on cache-hit
   */
  public V get(K key) {
    if (!isEnabled) {
      return null;
    }

    V result = cache.getIfPresent(key);
    if (result == null) {
      LOG.debug("{}: CACHE MISS: {}", label, key.toString());
    }
    else {
      LOG.debug("{}: CACHE HIT: {}, {}", label, key.toString(), result.toString());
    }
    return result;
  }

  public void put(K key, V value) {
    if (isEnabled) {
      LOG.debug("{}: CACHE PUT: {}, {}", label, key.toString(), value.toString());
      cache.put(key, value);
    }
  }

  public void clear() {
    if (isEnabled) {
      cache.invalidateAll();
    }
  }
}

/**
 * POJO representing the cache key for authorization calls
 */
class CachedAuthorizerEntry {

  private String path;
  private String accessType;
  private String owner;

  CachedAuthorizerEntry(String path, String accessType, String owner) {
    this.path = path;
    this.accessType = accessType;
    this.owner = owner;
  }

  public String getPath() {
    return path;
  }

  public String getAccessType() {
    return accessType;
  }

  public String getOwner() {
    return owner;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (!(o instanceof CachedAuthorizerEntry)) {
      return false;
    }

    CachedAuthorizerEntry c = (CachedAuthorizerEntry) o;
    return
      this.getPath().equals(c.getPath())
      && this.getAccessType().equals(c.getAccessType())
      && this.getOwner().equals(c.getOwner());
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public String toString() {
    return path + ":" + accessType + ":" + owner;
  }

}


/**
 * POJO representing the cache key for sas-key calls
 */
class CachedSASKeyEntry {

  private String storageAccount;
  private String container;
  private String path;

  CachedSASKeyEntry(String storageAccount, String container, String path) {
    this.storageAccount = storageAccount;
    this.container = container;
    this.path = path;
  }

  public String getStorageAccount() {
    return storageAccount;
  }

  public String getContainer() {
    return container;
  }

  public String getPath() {
    return path;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (!(o instanceof CachedSASKeyEntry)) {
      return false;
    }

    CachedSASKeyEntry c = (CachedSASKeyEntry) o;
    return
      this.getStorageAccount().equals(c.getStorageAccount())
      && this.getContainer().equals(c.getContainer())
      && this.getPath().equals(c.getPath());
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public String toString() {
    return storageAccount + ":" + container + ":" + path;
  }
}