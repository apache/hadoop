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

package org.apache.slider.server.appmaster.web.rest.application;

import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.rest.application.resources.AggregateModelRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.AppconfRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.CachedContent;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveComponentsRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveContainersRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveNodesRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveResourcesRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveStatisticsRefresher;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;

public class ApplicationResouceContentCacheFactory {
  public static final int LIFESPAN = 500;

  /**
   * Build the content cache
   * @param state state view
   */
  public static ContentCache createContentCache(
      StateAccessForProviders state) {
    ContentCache cache = new ContentCache();
    cache.put(LIVE_RESOURCES, new CachedContent<>(LIFESPAN, new LiveResourcesRefresher(state)));
    cache.put(LIVE_CONTAINERS, new CachedContent<>(LIFESPAN, new LiveContainersRefresher(state)));
    cache.put(LIVE_COMPONENTS, new CachedContent<>(LIFESPAN, new LiveComponentsRefresher(state)));
    cache.put(LIVE_NODES, new CachedContent<>(LIFESPAN, new LiveNodesRefresher(state)));
    cache.put(MODEL_DESIRED,
        new CachedContent<>(LIFESPAN, new AggregateModelRefresher(state, false)));
    cache.put(MODEL_RESOLVED,
        new CachedContent<>(LIFESPAN, new AggregateModelRefresher(state, true)));
    cache.put(MODEL_RESOLVED_APPCONF,
        new CachedContent<>(LIFESPAN, new AppconfRefresher(state, false, false)));
    cache.put(MODEL_RESOLVED_RESOURCES,
        new CachedContent<>(LIFESPAN, new AppconfRefresher(state, false, true)));
    cache.put(MODEL_DESIRED_APPCONF,
        new CachedContent<>(LIFESPAN, new AppconfRefresher(state, true, false)));
    cache.put(MODEL_DESIRED_RESOURCES,
        new CachedContent<>(LIFESPAN, new AppconfRefresher(state, true, true)));
    cache.put(LIVE_STATISTICS, new CachedContent<>(LIFESPAN, new LiveStatisticsRefresher(state)));
    return cache;
  }
}
