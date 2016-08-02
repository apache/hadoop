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

package org.apache.slider.server.appmaster.web.rest.application.resources;

import com.google.common.base.Preconditions;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cached resource is one that can be stored and served up, with a refresh 
 * only taking place when the expiry happens.
 * 
 * The refresh check/refresh is synchronized.
 * @param <T> type to return
 */
public class CachedContent<T> {
  private static final Logger log =
      LoggerFactory.getLogger(CachedContent.class);
  private T cachedValue;
  private long expires;
  private final long lifespan;
  private final ResourceRefresher<T> refresh;
  private int refreshCounter;

  public CachedContent(long lifespan,
      ResourceRefresher<T> refresh) {
    this.lifespan = lifespan;
    this.refresh = refresh;
  }

  /**
   * Get the value, triggering a refresh if needed
   * @return the cached or latest value.
   */
  public T get() throws Exception {
    maybeRefresh();
    return getCachedValue();
  }

  /**
   * Get the cached value without any expiry check
   * @return the last value set. May be null.
   */
  public synchronized T getCachedValue() {
    return cachedValue;
  }

  public synchronized int getRefreshCounter() {
    return refreshCounter;
  }

  /**
   * Get the lifespan in millis of the cached value
   * @return the lifespan
   */
  public long getLifespan() {
    return lifespan;
  }

  /**
   * Maybe refresh the content
   * @return true if a refresh took place.
   */
  public synchronized boolean maybeRefresh() throws Exception {
    long now = now();
    if (cachedValue == null || now >= expires) {
      log.debug("Refreshing at time {}", now);
      forceRefresh();
      log.debug("Refreshed value now {}", cachedValue);
      return true;
    }
    return false;
  }

  protected long now() {
    return Time.monotonicNow();
  }

  /**
   * Force a refresh and reset the expiry counter
   * @return the new value
   */
  protected synchronized T forceRefresh() throws Exception {
    refreshCounter ++;
    T updated = refresh.refresh();
    Preconditions.checkNotNull(updated);
    cachedValue = updated;
    expires = now() + lifespan;
    return cachedValue;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("CachedContent{");
    sb.append("  expires=").append(expires);
    sb.append(", lifespan=").append(lifespan);
    sb.append(", refresh=").append(refresh);
    sb.append(", refreshCounter=").append(refreshCounter);
    sb.append(", cached=").append(cachedValue);
    sb.append('}');
    return sb.toString();
  }
}
