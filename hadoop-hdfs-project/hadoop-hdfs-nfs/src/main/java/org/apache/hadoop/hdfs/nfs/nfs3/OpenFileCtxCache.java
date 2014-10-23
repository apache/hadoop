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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfigKeys;
import org.apache.hadoop.hdfs.nfs.conf.NfsConfiguration;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * A cache saves OpenFileCtx objects for different users. Each cache entry is
 * used to maintain the writing context for a single file.
 */
class OpenFileCtxCache {
  private static final Log LOG = LogFactory.getLog(OpenFileCtxCache.class);
  // Insert and delete with openFileMap are synced
  private final ConcurrentMap<FileHandle, OpenFileCtx> openFileMap = Maps
      .newConcurrentMap();

  private final int maxStreams;
  private final long streamTimeout;
  private final StreamMonitor streamMonitor;

  OpenFileCtxCache(NfsConfiguration config, long streamTimeout) {
    maxStreams = config.getInt(NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_KEY,
        NfsConfigKeys.DFS_NFS_MAX_OPEN_FILES_DEFAULT);
    LOG.info("Maximum open streams is " + maxStreams);
    this.streamTimeout = streamTimeout;
    streamMonitor = new StreamMonitor();
  }

  /**
   * The entry to be evicted is based on the following rules:<br>
   * 1. if the OpenFileCtx has any pending task, it will not be chosen.<br>
   * 2. if there is inactive OpenFileCtx, the first found one is to evict. <br>
   * 3. For OpenFileCtx entries don't belong to group 1 or 2, the idlest one 
   * is select. If it's idle longer than OUTPUT_STREAM_TIMEOUT_MIN_DEFAULT, it
   * will be evicted. Otherwise, the whole eviction request is failed.
   */
  @VisibleForTesting
  Entry<FileHandle, OpenFileCtx> getEntryToEvict() {
    Iterator<Entry<FileHandle, OpenFileCtx>> it = openFileMap.entrySet()
        .iterator();
    if (LOG.isTraceEnabled()) {
      LOG.trace("openFileMap size:" + openFileMap.size());
    }

    Entry<FileHandle, OpenFileCtx> idlest = null;
    
    while (it.hasNext()) {
      Entry<FileHandle, OpenFileCtx> pairs = it.next();
      OpenFileCtx ctx = pairs.getValue();
      if (!ctx.getActiveState()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got one inactive stream: " + ctx);
        }
        return pairs;
      }
      if (ctx.hasPendingWork()) {
        // Always skip files with pending work.
        continue;
      }
      if (idlest == null) {
        idlest = pairs;
      } else {
        if (ctx.getLastAccessTime() < idlest.getValue().getLastAccessTime()) {
          idlest = pairs;
        }
      }
    }

    if (idlest == null) {
      LOG.warn("No eviction candidate. All streams have pending work.");
      return null;
    } else {
      long idleTime = Time.monotonicNow()
          - idlest.getValue().getLastAccessTime();
      if (idleTime < NfsConfigKeys.DFS_NFS_STREAM_TIMEOUT_MIN_DEFAULT) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("idlest stream's idle time:" + idleTime);
        }
        LOG.warn("All opened streams are busy, can't remove any from cache.");
        return null;
      } else {
        return idlest;
      }
    }
  }

  boolean put(FileHandle h, OpenFileCtx context) {
    OpenFileCtx toEvict = null;
    synchronized (this) {
      Preconditions.checkState(openFileMap.size() <= this.maxStreams,
          "stream cache size " + openFileMap.size()
              + "  is larger than maximum" + this.maxStreams);
      if (openFileMap.size() == this.maxStreams) {
        Entry<FileHandle, OpenFileCtx> pairs = getEntryToEvict();
        if (pairs ==null) {
          return false;
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Evict stream ctx: " + pairs.getValue());
          }
          toEvict = openFileMap.remove(pairs.getKey());
          Preconditions.checkState(toEvict == pairs.getValue(),
              "The deleted entry is not the same as odlest found.");
        }
      }
      openFileMap.put(h, context);
    }
    
    // Cleanup the old stream outside the lock
    if (toEvict != null) {
      toEvict.cleanup();
    }
    return true;
  }

  @VisibleForTesting
  void scan(long streamTimeout) {
    ArrayList<OpenFileCtx> ctxToRemove = new ArrayList<OpenFileCtx>();
    Iterator<Entry<FileHandle, OpenFileCtx>> it = openFileMap.entrySet()
        .iterator();
    if (LOG.isTraceEnabled()) {
      LOG.trace("openFileMap size:" + openFileMap.size());
    }

    while (it.hasNext()) {
      Entry<FileHandle, OpenFileCtx> pairs = it.next();
      FileHandle handle = pairs.getKey();
      OpenFileCtx ctx = pairs.getValue();
      if (!ctx.streamCleanup(handle.getFileId(), streamTimeout)) {
        continue;
      }

      // Check it again inside lock before removing
      synchronized (this) {
        OpenFileCtx ctx2 = openFileMap.get(handle);
        if (ctx2 != null) {
          if (ctx2.streamCleanup(handle.getFileId(), streamTimeout)) {
            openFileMap.remove(handle);
            if (LOG.isDebugEnabled()) {
              LOG.debug("After remove stream " + handle.getFileId()
                  + ", the stream number:" + openFileMap.size());
            }
            ctxToRemove.add(ctx2);
          }
        }
      }
    }

    // Invoke the cleanup outside the lock
    for (OpenFileCtx ofc : ctxToRemove) {
      ofc.cleanup();
    }
  }

  OpenFileCtx get(FileHandle key) {
    return openFileMap.get(key);
  }

  int size() {
    return openFileMap.size();
  }

  void start() {
    streamMonitor.start();
  }

  // Evict all entries
  void cleanAll() {
    ArrayList<OpenFileCtx> cleanedContext = new ArrayList<OpenFileCtx>();
    synchronized (this) {
      Iterator<Entry<FileHandle, OpenFileCtx>> it = openFileMap.entrySet()
          .iterator();
      if (LOG.isTraceEnabled()) {
        LOG.trace("openFileMap size:" + openFileMap.size());
      }

      while (it.hasNext()) {
        Entry<FileHandle, OpenFileCtx> pairs = it.next();
        OpenFileCtx ctx = pairs.getValue();
        it.remove();
        cleanedContext.add(ctx);
      }
    }

    // Invoke the cleanup outside the lock
    for (OpenFileCtx ofc : cleanedContext) {
      ofc.cleanup();
    }
  }

  void shutdown() {
    // stop the dump thread
    if (streamMonitor.isAlive()) {
      streamMonitor.shouldRun(false);
      streamMonitor.interrupt();
      try {
        streamMonitor.join(3000);
      } catch (InterruptedException ignored) {
      }
    }
    
    cleanAll();
  }

  /**
   * StreamMonitor wakes up periodically to find and closes idle streams.
   */
  class StreamMonitor extends Daemon {
    private final static int rotation = 5 * 1000; // 5 seconds
    private long lastWakeupTime = 0;
    private boolean shouldRun = true;
    
    void shouldRun(boolean shouldRun) {
      this.shouldRun = shouldRun;
    }
    
    @Override
    public void run() {
      while (shouldRun) {
        scan(streamTimeout);

        // Check if it can sleep
        try {
          long workedTime = Time.monotonicNow() - lastWakeupTime;
          if (workedTime < rotation) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("StreamMonitor can still have a sleep:"
                  + ((rotation - workedTime) / 1000));
            }
            Thread.sleep(rotation - workedTime);
          }
          lastWakeupTime = Time.monotonicNow();

        } catch (InterruptedException e) {
          LOG.info("StreamMonitor got interrupted");
          return;
        }
      }
    }
  }
}
