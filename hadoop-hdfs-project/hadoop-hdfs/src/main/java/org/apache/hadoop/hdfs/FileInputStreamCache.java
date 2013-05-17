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

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * FileInputStream cache is used to cache FileInputStream objects that we
 * have received from the DataNode.
 */
class FileInputStreamCache {
  private final static Log LOG = LogFactory.getLog(FileInputStreamCache.class);

  /**
   * The executor service that runs the cacheCleaner.  There is only one of
   * these per VM.
   */
  private final static ScheduledThreadPoolExecutor executor
      = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().
          setDaemon(true).setNameFormat("FileInputStreamCache Cleaner").
          build());
  
  /**
   * The CacheCleaner for this FileInputStreamCache.  We don't create this
   * and schedule it until it becomes necessary.
   */
  private CacheCleaner cacheCleaner;
  
  /**
   * Maximum number of entries to allow in the cache.
   */
  private final int maxCacheSize;
  
  /**
   * The minimum time in milliseconds to preserve an element in the cache.
   */
  private final long expiryTimeMs;
  
  /**
   * True if the FileInputStreamCache is closed.
   */
  private boolean closed = false;
  
  /**
   * Cache entries.
   */
  private final LinkedListMultimap<Key, Value> map = LinkedListMultimap.create();

  /**
   * Expiry thread which makes sure that the file descriptors get closed
   * after a while.
   */
  private static class CacheCleaner implements Runnable, Closeable {
    private WeakReference<FileInputStreamCache> cacheRef;
    private ScheduledFuture<?> future;
    
    CacheCleaner(FileInputStreamCache cache) {
      this.cacheRef = new WeakReference<FileInputStreamCache>(cache);
    }
    
    @Override
    public void run() {
      FileInputStreamCache cache = cacheRef.get();
      if (cache == null) return;
      synchronized(cache) {
        if (cache.closed) return;
        long curTime = Time.monotonicNow();
        for (Iterator<Entry<Key, Value>> iter =
                  cache.map.entries().iterator(); iter.hasNext();
              iter = cache.map.entries().iterator()) {
          Entry<Key, Value> entry = iter.next();
          if (entry.getValue().getTime() + cache.expiryTimeMs >= curTime) {
            break;
          }
          entry.getValue().close();
          iter.remove();
        }
      }
    }

    @Override
    public void close() throws IOException {
      if (future != null) {
        future.cancel(false);
      }
    }
    
    public void setFuture(ScheduledFuture<?> future) {
      this.future = future;
    }
  }

  /**
   * The key identifying a FileInputStream array.
   */
  static class Key {
    private final DatanodeID datanodeID;
    private final ExtendedBlock block;
    
    public Key(DatanodeID datanodeID, ExtendedBlock block) {
      this.datanodeID = datanodeID;
      this.block = block;
    }
    
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof FileInputStreamCache.Key)) {
        return false;
      }
      FileInputStreamCache.Key otherKey = (FileInputStreamCache.Key)other;
      return (block.equals(otherKey.block) &&
          (block.getGenerationStamp() == otherKey.block.getGenerationStamp()) &&
          datanodeID.equals(otherKey.datanodeID));
    }

    @Override
    public int hashCode() {
      return block.hashCode();
    }
  }

  /**
   * The value containing a FileInputStream array and the time it was added to
   * the cache.
   */
  static class Value {
    private final FileInputStream fis[];
    private final long time;
    
    public Value (FileInputStream fis[]) {
      this.fis = fis;
      this.time = Time.monotonicNow();
    }

    public FileInputStream[] getFileInputStreams() {
      return fis;
    }

    public long getTime() {
      return time;
    }
    
    public void close() {
      IOUtils.cleanup(LOG, fis);
    }
  }
  
  /**
   * Create a new FileInputStream
   *
   * @param maxCacheSize         The maximum number of elements to allow in 
   *                             the cache.
   * @param expiryTimeMs         The minimum time in milliseconds to preserve
   *                             elements in the cache.
   */
  public FileInputStreamCache(int maxCacheSize, long expiryTimeMs) {
    this.maxCacheSize = maxCacheSize;
    this.expiryTimeMs = expiryTimeMs;
  }
  
  /**
   * Put an array of FileInputStream objects into the cache.
   *
   * @param datanodeID          The DatanodeID to store the streams under.
   * @param block               The Block to store the streams under.
   * @param fis                 The streams.
   */
  public void put(DatanodeID datanodeID, ExtendedBlock block,
      FileInputStream fis[]) {
    boolean inserted = false;
    try {
      synchronized(this) {
        if (closed) return;
        if (map.size() + 1 > maxCacheSize) {
          Iterator<Entry<Key, Value>> iter = map.entries().iterator();
          if (!iter.hasNext()) return;
          Entry<Key, Value> entry = iter.next();
          entry.getValue().close();
          iter.remove();
        }
        if (cacheCleaner == null) {
          cacheCleaner = new CacheCleaner(this);
          ScheduledFuture<?> future = 
              executor.scheduleAtFixedRate(cacheCleaner, expiryTimeMs, expiryTimeMs,
                  TimeUnit.MILLISECONDS);
          cacheCleaner.setFuture(future);
        }
        map.put(new Key(datanodeID, block), new Value(fis));
        inserted = true;
      }
    } finally {
      if (!inserted) {
        IOUtils.cleanup(LOG, fis);
      }
    }
  }
  
  /**
   * Find and remove an array of FileInputStream objects from the cache.
   *
   * @param datanodeID          The DatanodeID to search for.
   * @param block               The Block to search for.
   *
   * @return                    null if no streams can be found; the
   *                            array otherwise.  If this is non-null, the
   *                            array will have been removed from the cache.
   */
  public synchronized FileInputStream[] get(DatanodeID datanodeID,
      ExtendedBlock block) {
    Key key = new Key(datanodeID, block);
    List<Value> ret = map.get(key);
    if (ret.isEmpty()) return null;
    Value val = ret.get(0);
    map.remove(key, val);
    return val.getFileInputStreams();
  }
  
  /**
   * Close the cache and free all associated resources.
   */
  public synchronized void close() {
    if (closed) return;
    closed = true;
    IOUtils.cleanup(LOG, cacheCleaner);
    for (Iterator<Entry<Key, Value>> iter = map.entries().iterator();
          iter.hasNext();) {
      Entry<Key, Value> entry = iter.next();
      entry.getValue().close();
      iter.remove();
    }
  }
  
  public synchronized String toString() {
    StringBuilder bld = new StringBuilder();
    bld.append("FileInputStreamCache(");
    String prefix = "";
    for (Entry<Key, Value> entry : map.entries()) {
      bld.append(prefix);
      bld.append(entry.getKey());
      prefix = ", ";
    }
    bld.append(")");
    return bld.toString();
  }
  
  public long getExpiryTimeMs() {
    return expiryTimeMs;
  }
  
  public int getMaxCacheSize() {
    return maxCacheSize;
  }
}
