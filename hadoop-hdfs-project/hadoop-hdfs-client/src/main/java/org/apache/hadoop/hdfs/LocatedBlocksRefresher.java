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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_CONTEXT_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_THREADS_KEY;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically refresh the underlying cached {@link LocatedBlocks} for eligible registered
 * {@link DFSInputStream}s.  DFSInputStreams are eligible for refreshing if they have any
 * deadNodes or any blocks are lacking local replicas.
 * Disabled by default, unless an interval is configured.
 */
public class LocatedBlocksRefresher extends Daemon {
  private static final Logger LOG =
      LoggerFactory.getLogger(LocatedBlocksRefresher.class);

  private static final String THREAD_PREFIX = "located-block-refresher-";

  private final String name;
  private final long interval;
  private final long jitter;
  private final ExecutorService refreshThreadPool;

  // Use WeakHashMap so that we don't hold onto references that might have not been explicitly
  // closed because they were created and thrown away.
  private final Set<DFSInputStream> registeredInputStreams =
      Collections.newSetFromMap(new WeakHashMap<>());

  private int runCount;
  private int refreshCount;

  LocatedBlocksRefresher(String name, Configuration conf, DfsClientConf dfsClientConf) {
    this.name = name;
    this.interval = dfsClientConf.getLocatedBlocksRefresherInterval();
    this.jitter = Math.round(this.interval * 0.1);
    int rpcThreads = conf.getInt(DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_THREADS_KEY,
        DFS_CLIENT_REFRESH_READ_BLOCK_LOCATIONS_THREADS_DEFAULT);

    String threadPrefix;
    if (name.equals(DFS_CLIENT_CONTEXT_DEFAULT)) {
      threadPrefix = THREAD_PREFIX;
    } else {
      threadPrefix = THREAD_PREFIX + name + "-";
    }

    this.refreshThreadPool = Executors.newFixedThreadPool(rpcThreads, new Daemon.DaemonFactory() {
      private final AtomicInteger threadIndex = new AtomicInteger(0);

      @Override
      public Thread newThread(Runnable r) {
        Thread t = super.newThread(r);
        t.setName(threadPrefix + threadIndex.getAndIncrement());
        return t;
      }
    });

    setName(threadPrefix + "main");

    LOG.info("Start located block refresher for DFSClient {}.", this.name);
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {

      if (!waitForInterval()) {
        return;
      }

      LOG.debug("Running refresh for {} streams", registeredInputStreams.size());
      long start = Time.monotonicNow();
      AtomicInteger neededRefresh = new AtomicInteger(0);

      Phaser phaser = new Phaser(1);

      Map<String, InetSocketAddress> addressCache = new ConcurrentHashMap<>();

      for (DFSInputStream inputStream : getInputStreams()) {
        phaser.register();
        refreshThreadPool.submit(() -> {
          try {
            if (isInputStreamTracked(inputStream) &&
                inputStream.refreshBlockLocations(addressCache)) {
              neededRefresh.incrementAndGet();
            }
          } finally {
            phaser.arriveAndDeregister();
          }
        });
      }

      phaser.arriveAndAwaitAdvance();

      synchronized (this) {
        runCount++;
        refreshCount += neededRefresh.get();
      }

      LOG.debug(
          "Finished refreshing {} of {} streams in {}ms",
          neededRefresh,
          registeredInputStreams.size(),
          Time.monotonicNow() - start
      );
    }
  }

  public synchronized int getRunCount() {
    return runCount;
  }

  public synchronized int getRefreshCount() {
    return refreshCount;
  }

  private boolean waitForInterval() {
    try {
      Thread.sleep(interval + ThreadLocalRandom.current().nextLong(-jitter, jitter));
      return true;
    } catch (InterruptedException e) {
      LOG.debug("Interrupted during wait interval", e);
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Shutdown all the threads.
   */
  public void shutdown() {
    if (isAlive()) {
      interrupt();
      try {
        join();
      } catch (InterruptedException e) {
      }
    }
    refreshThreadPool.shutdown();
  }

  /**
   * Collects the DFSInputStreams to a list within synchronization, so that we can iterate them
   * without potentially blocking callers to {@link #addInputStream(DFSInputStream)} or
   * {@link #removeInputStream(DFSInputStream)}. We don't care so much about missing additions,
   * and we'll guard against removals by doing an additional
   * {@link #isInputStreamTracked(DFSInputStream)} track during iteration.
   */
  private synchronized Collection<DFSInputStream> getInputStreams() {
    return new ArrayList<>(registeredInputStreams);
  }

  public synchronized void addInputStream(DFSInputStream dfsInputStream) {
    LOG.trace("Registering {} for {}", dfsInputStream, dfsInputStream.getSrc());
    registeredInputStreams.add(dfsInputStream);
  }

  public synchronized void removeInputStream(DFSInputStream dfsInputStream) {
    if (isInputStreamTracked(dfsInputStream)) {
      LOG.trace("De-registering {} for {}", dfsInputStream, dfsInputStream.getSrc());
      registeredInputStreams.remove(dfsInputStream);
    }
  }

  public synchronized boolean isInputStreamTracked(DFSInputStream dfsInputStream) {
    return registeredInputStreams.contains(dfsInputStream);
  }

  public long getInterval() {
    return interval;
  }
}
