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
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Timer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A user-to-groups mapping service.
 * 
 * {@link Groups} allows for server to get the various group memberships
 * of a given user via the {@link #getGroups(String)} call, thus ensuring 
 * a consistent user-to-groups mapping and protects against vagaries of 
 * different mappings on servers and clients in a Hadoop cluster. 
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class Groups {
  private static final Log LOG = LogFactory.getLog(Groups.class);
  
  private final GroupMappingServiceProvider impl;

  private final LoadingCache<String, List<String>> cache;
  private final AtomicReference<Map<String, List<String>>> staticMapRef =
      new AtomicReference<>();
  private final long cacheTimeout;
  private final long negativeCacheTimeout;
  private final long warningDeltaMs;
  private final Timer timer;
  private Set<String> negativeCache;
  private final boolean reloadGroupsInBackground;
  private final int reloadGroupsThreadCount;

  private final AtomicLong backgroundRefreshSuccess =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshException =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshQueued =
      new AtomicLong(0);
  private final AtomicLong backgroundRefreshRunning =
      new AtomicLong(0);

  public Groups(Configuration conf) {
    this(conf, new Timer());
  }

  public Groups(Configuration conf, final Timer timer) {
    impl = 
      ReflectionUtils.newInstance(
          conf.getClass(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING, 
                        ShellBasedUnixGroupsMapping.class, 
                        GroupMappingServiceProvider.class), 
          conf);

    cacheTimeout = 
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 
          CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;
    negativeCacheTimeout =
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS,
          CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS_DEFAULT) * 1000;
    warningDeltaMs =
      conf.getLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS,
        CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_WARN_AFTER_MS_DEFAULT);
    reloadGroupsInBackground =
      conf.getBoolean(
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD,
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_DEFAULT);
    reloadGroupsThreadCount  =
      conf.getInt(
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS,
          CommonConfigurationKeys.
              HADOOP_SECURITY_GROUPS_CACHE_BACKGROUND_RELOAD_THREADS_DEFAULT);
    parseStaticMapping(conf);

    this.timer = timer;
    this.cache = CacheBuilder.newBuilder()
      .refreshAfterWrite(cacheTimeout, TimeUnit.MILLISECONDS)
      .ticker(new TimerToTickerAdapter(timer))
      .expireAfterWrite(10 * cacheTimeout, TimeUnit.MILLISECONDS)
      .build(new GroupCacheLoader());

    if(negativeCacheTimeout > 0) {
      Cache<String, Boolean> tempMap = CacheBuilder.newBuilder()
        .expireAfterWrite(negativeCacheTimeout, TimeUnit.MILLISECONDS)
        .ticker(new TimerToTickerAdapter(timer))
        .build();
      negativeCache = Collections.newSetFromMap(tempMap.asMap());
    }

    if(LOG.isDebugEnabled())
      LOG.debug("Group mapping impl=" + impl.getClass().getName() + 
          "; cacheTimeout=" + cacheTimeout + "; warningDeltaMs=" +
          warningDeltaMs);
  }
  
  @VisibleForTesting
  Set<String> getNegativeCache() {
    return negativeCache;
  }

  /*
   * Parse the hadoop.user.group.static.mapping.overrides configuration to
   * staticUserToGroupsMap
   */
  private void parseStaticMapping(Configuration conf) {
    String staticMapping = conf.get(
        CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
        CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT);
    Collection<String> mappings = StringUtils.getStringCollection(
        staticMapping, ";");
    Map<String, List<String>> staticUserToGroupsMap =
        new HashMap<String, List<String>>();
    for (String users : mappings) {
      Collection<String> userToGroups = StringUtils.getStringCollection(users,
          "=");
      if (userToGroups.size() < 1 || userToGroups.size() > 2) {
        throw new HadoopIllegalArgumentException("Configuration "
            + CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES
            + " is invalid");
      }
      String[] userToGroupsArray = userToGroups.toArray(new String[userToGroups
          .size()]);
      String user = userToGroupsArray[0];
      List<String> groups = Collections.emptyList();
      if (userToGroupsArray.length == 2) {
        groups = (List<String>) StringUtils
            .getStringCollection(userToGroupsArray[1]);
      }
      staticUserToGroupsMap.put(user, groups);
    }
    staticMapRef.set(
        staticUserToGroupsMap.isEmpty() ? null : staticUserToGroupsMap);
  }

  private boolean isNegativeCacheEnabled() {
    return negativeCacheTimeout > 0;
  }

  private IOException noGroupsForUser(String user) {
    return new IOException("No groups found for user " + user);
  }

  /**
   * Get the group memberships of a given user.
   * If the user's group is not cached, this method may block.
   * @param user User's name
   * @return the group memberships of the user
   * @throws IOException if user does not exist
   */
  public List<String> getGroups(final String user) throws IOException {
    // No need to lookup for groups of static users
    Map<String, List<String>> staticUserToGroupsMap = staticMapRef.get();
    if (staticUserToGroupsMap != null) {
      List<String> staticMapping = staticUserToGroupsMap.get(user);
      if (staticMapping != null) {
        return staticMapping;
      }
    }

    // Check the negative cache first
    if (isNegativeCacheEnabled()) {
      if (negativeCache.contains(user)) {
        throw noGroupsForUser(user);
      }
    }

    try {
      return cache.get(user);
    } catch (ExecutionException e) {
      throw (IOException)e.getCause();
    }
  }

  public long getBackgroundRefreshSuccess() {
    return backgroundRefreshSuccess.get();
  }

  public long getBackgroundRefreshException() {
    return backgroundRefreshException.get();
  }

  public long getBackgroundRefreshQueued() {
    return backgroundRefreshQueued.get();
  }

  public long getBackgroundRefreshRunning() {
    return backgroundRefreshRunning.get();
  }

  /**
   * Convert millisecond times from hadoop's timer to guava's nanosecond ticker.
   */
  private static class TimerToTickerAdapter extends Ticker {
    private Timer timer;

    public TimerToTickerAdapter(Timer timer) {
      this.timer = timer;
    }

    @Override
    public long read() {
      final long NANOSECONDS_PER_MS = 1000000;
      return timer.monotonicNow() * NANOSECONDS_PER_MS;
    }
  }

  /**
   * Deals with loading data into the cache.
   */
  private class GroupCacheLoader extends CacheLoader<String, List<String>> {

    private ListeningExecutorService executorService;

    GroupCacheLoader() {
      if (reloadGroupsInBackground) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Group-Cache-Reload")
            .setDaemon(true)
            .build();
        // With coreThreadCount == maxThreadCount we effectively
        // create a fixed size thread pool. As allowCoreThreadTimeOut
        // has been set, all threads will die after 60 seconds of non use
        ThreadPoolExecutor parentExecutor =  new ThreadPoolExecutor(
            reloadGroupsThreadCount,
            reloadGroupsThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            threadFactory);
        parentExecutor.allowCoreThreadTimeOut(true);
        executorService = MoreExecutors.listeningDecorator(parentExecutor);
      }
    }

    /**
     * This method will block if a cache entry doesn't exist, and
     * any subsequent requests for the same user will wait on this
     * request to return. If a user already exists in the cache,
     * and when the key expires, the first call to reload the key
     * will block, but subsequent requests will return the old
     * value until the blocking thread returns.
     * If reloadGroupsInBackground is true, then the thread that
     * needs to refresh an expired key will not block either. Instead
     * it will return the old cache value and schedule a background
     * refresh
     * @param user key of cache
     * @return List of groups belonging to user
     * @throws IOException to prevent caching negative entries
     */
    @Override
    public List<String> load(String user) throws Exception {
      TraceScope scope = null;
      Tracer tracer = Tracer.curThreadTracer();
      if (tracer != null) {
        scope = tracer.newScope("Groups#fetchGroupList");
        scope.addKVAnnotation("user", user);
      }
      List<String> groups = null;
      try {
        groups = fetchGroupList(user);
      } finally {
        if (scope != null) {
          scope.close();
        }
      }

      if (groups.isEmpty()) {
        if (isNegativeCacheEnabled()) {
          negativeCache.add(user);
        }

        // We throw here to prevent Cache from retaining an empty group
        throw noGroupsForUser(user);
      }

      // return immutable de-duped list
      return Collections.unmodifiableList(
          new ArrayList<>(new LinkedHashSet<>(groups)));
    }

    /**
     * Override the reload method to provide an asynchronous implementation. If
     * reloadGroupsInBackground is false, then this method defers to the super
     * implementation, otherwise is arranges for the cache to be updated later
     */
    @Override
    public ListenableFuture<List<String>> reload(final String key,
                                                 List<String> oldValue)
        throws Exception {
      if (!reloadGroupsInBackground) {
        return super.reload(key, oldValue);
      }

      backgroundRefreshQueued.incrementAndGet();
      ListenableFuture<List<String>> listenableFuture =
          executorService.submit(new Callable<List<String>>() {
            @Override
            public List<String> call() throws Exception {
              backgroundRefreshQueued.decrementAndGet();
              backgroundRefreshRunning.incrementAndGet();
              List<String> results = load(key);
              return results;
            }
          });
      Futures.addCallback(listenableFuture, new FutureCallback<List<String>>() {
        @Override
        public void onSuccess(List<String> result) {
          backgroundRefreshSuccess.incrementAndGet();
          backgroundRefreshRunning.decrementAndGet();
        }
        @Override
        public void onFailure(Throwable t) {
          backgroundRefreshException.incrementAndGet();
          backgroundRefreshRunning.decrementAndGet();
        }
      });
      return listenableFuture;
    }

    /**
     * Queries impl for groups belonging to the user. This could involve I/O and take awhile.
     */
    private List<String> fetchGroupList(String user) throws IOException {
      long startMs = timer.monotonicNow();
      List<String> groupList = impl.getGroups(user);
      long endMs = timer.monotonicNow();
      long deltaMs = endMs - startMs ;
      UserGroupInformation.metrics.addGetGroups(deltaMs);
      if (deltaMs > warningDeltaMs) {
        LOG.warn("Potential performance problem: getGroups(user=" + user +") " +
          "took " + deltaMs + " milliseconds.");
      }

      return groupList;
    }
  }

  /**
   * Refresh all user-to-groups mappings.
   */
  public void refresh() {
    LOG.info("clearing userToGroupsMap cache");
    try {
      impl.cacheGroupsRefresh();
    } catch (IOException e) {
      LOG.warn("Error refreshing groups cache", e);
    }
    cache.invalidateAll();
    if(isNegativeCacheEnabled()) {
      negativeCache.clear();
    }
  }

  /**
   * Add groups to cache
   *
   * @param groups list of groups to add to cache
   */
  public void cacheGroupsAdd(List<String> groups) {
    try {
      impl.cacheGroupsAdd(groups);
    } catch (IOException e) {
      LOG.warn("Error caching groups", e);
    }
  }

  private static Groups GROUPS = null;
  
  /**
   * Get the groups being used to map user-to-groups.
   * @return the groups being used to map user-to-groups.
   */
  public static Groups getUserToGroupsMappingService() {
    return getUserToGroupsMappingService(new Configuration()); 
  }

  /**
   * Get the groups being used to map user-to-groups.
   * @param conf
   * @return the groups being used to map user-to-groups.
   */
  public static synchronized Groups getUserToGroupsMappingService(
    Configuration conf) {

    if(GROUPS == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug(" Creating new Groups object");
      }
      GROUPS = new Groups(conf);
    }
    return GROUPS;
  }

  /**
   * Create new groups used to map user-to-groups with loaded configuration.
   * @param conf
   * @return the groups being used to map user-to-groups.
   */
  @Private
  public static synchronized Groups
      getUserToGroupsMappingServiceWithLoadedConfiguration(
          Configuration conf) {

    GROUPS = new Groups(conf);
    return GROUPS;
  }
}
