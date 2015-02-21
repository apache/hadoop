/**
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
package org.apache.hadoop.util.curator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableScheduledExecutorService;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.utils.PathUtils;

/**
 * This is a copy of Curator 2.7.1's ChildReaper class, modified to work with
 * Guava 11.0.2.  The problem is the 'paths' Collection, which calls Guava's
 * Sets.newConcurrentHashSet(), which was added in Guava 15.0.
 * <p>
 * Utility to reap empty child nodes of a parent node. Periodically calls getChildren on
 * the node and adds empty nodes to an internally managed {@link Reaper}
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ChildReaper implements Closeable
{
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final Reaper reaper;
  private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
  private final CuratorFramework client;
  private final Collection<String> paths = newConcurrentHashSet();
  private final Reaper.Mode mode;
  private final CloseableScheduledExecutorService executor;
  private final int reapingThresholdMs;

  private volatile Future<?> task;

  // This is copied from Curator's Reaper class
  static final int DEFAULT_REAPING_THRESHOLD_MS = (int)TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  // This is copied from Guava
  /**
   * Creates a thread-safe set backed by a hash map. The set is backed by a
   * {@link ConcurrentHashMap} instance, and thus carries the same concurrency
   * guarantees.
   *
   * <p>Unlike {@code HashSet}, this class does NOT allow {@code null} to be
   * used as an element. The set is serializable.
   *
   * @return a new, empty thread-safe {@code Set}
   * @since 15.0
   */
  public static <E> Set<E> newConcurrentHashSet() {
    return Sets.newSetFromMap(new ConcurrentHashMap<E, Boolean>());
  }

  private enum State
  {
    LATENT,
    STARTED,
    CLOSED
  }

  /**
   * @param client the client
   * @param path path to reap children from
   * @param mode reaping mode
   */
  public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode)
  {
    this(client, path, mode, newExecutorService(), DEFAULT_REAPING_THRESHOLD_MS, null);
  }

  /**
   * @param client the client
   * @param path path to reap children from
   * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
   * @param mode reaping mode
   */
  public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, int reapingThresholdMs)
  {
    this(client, path, mode, newExecutorService(), reapingThresholdMs, null);
  }

  /**
   * @param client the client
   * @param path path to reap children from
   * @param executor executor to use for background tasks
   * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
   * @param mode reaping mode
   */
  public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs)
  {
    this(client, path, mode, executor, reapingThresholdMs, null);
  }

  /**
   * @param client the client
   * @param path path to reap children from
   * @param executor executor to use for background tasks
   * @param reapingThresholdMs threshold in milliseconds that determines that a path can be deleted
   * @param mode reaping mode
   * @param leaderPath if not null, uses a leader selection so that only 1 reaper is active in the cluster
   */
  public ChildReaper(CuratorFramework client, String path, Reaper.Mode mode, ScheduledExecutorService executor, int reapingThresholdMs, String leaderPath)
  {
    this.client = client;
    this.mode = mode;
    this.executor = new CloseableScheduledExecutorService(executor);
    this.reapingThresholdMs = reapingThresholdMs;
    this.reaper = new Reaper(client, executor, reapingThresholdMs, leaderPath);
    addPath(path);
  }

  /**
   * The reaper must be started
   *
   * @throws Exception errors
   */
  public void start() throws Exception
  {
    Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

    task = executor.scheduleWithFixedDelay
        (
            new Runnable()
            {
              @Override
              public void run()
              {
                doWork();
              }
            },
            reapingThresholdMs,
            reapingThresholdMs,
            TimeUnit.MILLISECONDS
        );

    reaper.start();
  }

  @Override
  public void close() throws IOException
  {
    if ( state.compareAndSet(State.STARTED, State.CLOSED) )
    {
      CloseableUtils.closeQuietly(reaper);
      task.cancel(true);
    }
  }

  /**
   * Add a path to reap children from
   *
   * @param path the path
   * @return this for chaining
   */
  public ChildReaper addPath(String path)
  {
    paths.add(PathUtils.validatePath(path));
    return this;
  }

  /**
   * Remove a path from reaping
   *
   * @param path the path
   * @return true if the path existed and was removed
   */
  public boolean removePath(String path)
  {
    return paths.remove(PathUtils.validatePath(path));
  }

  private static ScheduledExecutorService newExecutorService()
  {
    return ThreadUtils.newFixedThreadScheduledPool(2, "ChildReaper");
  }

  private void doWork()
  {
    for ( String path : paths )
    {
      try
      {
        List<String> children = client.getChildren().forPath(path);
        for ( String name : children )
        {
          String thisPath = ZKPaths.makePath(path, name);
          Stat stat = client.checkExists().forPath(thisPath);
          if ( (stat != null) && (stat.getNumChildren() == 0) )
          {
            reaper.addPath(thisPath, mode);
          }
        }
      }
      catch ( Exception e )
      {
        log.error("Could not get children for path: " + path, e);
      }
    }
  }
}
