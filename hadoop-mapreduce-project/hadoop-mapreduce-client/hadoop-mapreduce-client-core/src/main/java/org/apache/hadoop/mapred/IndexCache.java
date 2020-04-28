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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexCache {

  private final JobConf conf;
  private final int totalMemoryAllowed;
  private AtomicInteger totalMemoryUsed = new AtomicInteger();
  private static final Logger LOG = LoggerFactory.getLogger(IndexCache.class);

  private final ConcurrentHashMap<String,IndexInformation> cache =
    new ConcurrentHashMap<String,IndexInformation>();
  
  private final LinkedBlockingQueue<String> queue = 
    new LinkedBlockingQueue<String>();

  public IndexCache(JobConf conf) {
    this.conf = conf;
    totalMemoryAllowed =
      conf.getInt(TTConfig.TT_INDEX_CACHE, 10) * 1024 * 1024;
    LOG.info("IndexCache created with max memory = " + totalMemoryAllowed);
  }

  /**
   * This method gets the index information for the given mapId and reduce.
   * It reads the index file into cache if it is not already present.
   * @param mapId
   * @param reduce
   * @param fileName The file to read the index information from if it is not
   *                 already present in the cache
   * @param expectedIndexOwner The expected owner of the index file
   * @return The Index Information
   * @throws IOException
   */
  public IndexRecord getIndexInformation(String mapId, int reduce,
                                         Path fileName, String expectedIndexOwner)
    throws IOException {

    IndexInformation info = cache.get(mapId);

    if (info == null) {
      info = readIndexFileToCache(fileName, mapId, expectedIndexOwner);
    } else {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId {} found", mapId);
    }

    if (info.mapSpillRecord.size() == 0 ||
        info.mapSpillRecord.size() <= reduce) {
      throw new IOException("Invalid request " +
        " Map Id = " + mapId + " Reducer = " + reduce +
        " Index Info Length = " + info.mapSpillRecord.size());
    }
    return info.mapSpillRecord.getIndex(reduce);
  }

  private boolean isUnderConstruction(IndexInformation info) {
    synchronized(info) {
      return (null == info.mapSpillRecord);
    }
  }

  private IndexInformation readIndexFileToCache(Path indexFileName,
                                                String mapId,
                                                String expectedIndexOwner)
    throws IOException {
    IndexInformation info;
    IndexInformation newInd = new IndexInformation();
    if ((info = cache.putIfAbsent(mapId, newInd)) != null) {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          try {
            info.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for construction", e);
          }
        }
      }
      LOG.debug("IndexCache HIT: MapId {} found", mapId);
      return info;
    }
    LOG.debug("IndexCache MISS: MapId {} not found", mapId);
    SpillRecord tmp = null;
    boolean success = false;
    try { 
      tmp = new SpillRecord(indexFileName, conf, expectedIndexOwner);
      success = true;
    } catch (Throwable e) {
      tmp = new SpillRecord(0);
      cache.remove(mapId);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IOException("Error Reading IndexFile", e);
    } finally {
      synchronized (newInd) {
        newInd.mapSpillRecord = tmp;
        if (success) {
          // Only add mapId to the queue for successful read and after added to
          // the cache. Once in the queue, it is now eligible for removal once
          // construction is finished.
          queue.add(mapId);
          if (totalMemoryUsed.addAndGet(newInd.getSize()) > totalMemoryAllowed) {
            freeIndexInformation();
          }
        }
        newInd.notifyAll();
      } 
    } 

    return newInd;
  }

  /**
   * This method removes the map from the cache if it is present in the queue.
   * @param mapId The taskID of this map.
   */
  public void removeMap(String mapId) throws IOException {
    // Successfully removing the mapId from the queue enters into a contract
    // that this thread will remove the corresponding mapId from the cache.
    if (!queue.remove(mapId)) {
      LOG.debug("Map ID {} not found in queue", mapId);
      return;
    }
    removeMapInternal(mapId);
  }

  /** This method should only be called upon successful removal of mapId from
   * the queue. The mapId will be removed from the cache and totalUsedMemory
   * will be decremented.
   * @param mapId the cache item to be removed
   * @throws IOException
   */
  private void removeMapInternal(String mapId) throws IOException {
    IndexInformation info = cache.remove(mapId);
    if (info == null) {
      // Inconsistent state as presence in queue implies presence in cache
      LOG.warn("Map ID " + mapId + " not found in cache");
      return;
    }
    try {
      synchronized(info) {
        while (isUnderConstruction(info)) {
          info.wait();
        }
        totalMemoryUsed.getAndAdd(-info.getSize());
      }
    } catch (InterruptedException e) {
      totalMemoryUsed.getAndAdd(-info.getSize());
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for construction", e);
    }
  }

  /**
   * This method checks if cache and totalMemoryUsed is consistent.
   * It is only used for unit test.
   * @return True if cache and totalMemoryUsed is consistent
   */
  boolean checkTotalMemoryUsed() {
    int totalSize = 0;
    for (IndexInformation info : cache.values()) {
      totalSize += info.getSize();
    }
    return totalSize == totalMemoryUsed.get();
  }

  /**
   * Bring memory usage below totalMemoryAllowed.
   */
  private synchronized void freeIndexInformation() throws IOException {
    while (totalMemoryUsed.get() > totalMemoryAllowed) {
      if(queue.isEmpty()) {
        break;
      }
      String mapId = queue.remove();
      removeMapInternal(mapId);
    }
  }

  private static class IndexInformation {
    SpillRecord mapSpillRecord;

    int getSize() {
      return mapSpillRecord == null
        ? 0
        : mapSpillRecord.size() * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;
    }
  }
}
