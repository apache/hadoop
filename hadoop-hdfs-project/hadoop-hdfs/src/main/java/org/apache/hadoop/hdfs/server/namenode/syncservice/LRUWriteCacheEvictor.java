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
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.security.SecurityUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A write cache evictor based on LRU policy.
 */
public class LRUWriteCacheEvictor extends FIFOWriteCacheEvictor {
  private Set<Long> evictSet;
  private long lastTxid;

  public LRUWriteCacheEvictor(Configuration conf,
      FSNamesystem fsNamesystem) {
    super(conf, fsNamesystem);
    this.evictSet = ConcurrentHashMap.newKeySet();
    this.lastTxid = initTxid();
    long accessPrecision = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY,
        DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_DEFAULT);
    if (accessPrecision > 600000L) {
      log.warn("Provided storage write cache will not be evicted by " +
          "strictly " + "following LRU policy due to access precision " +
          "value " + accessPrecision + "(ms) is set too large.");
    }
  }

  /**
   * After cluster starts, the cache evictor will get the last written txid
   * from edit log to init {@link LRUWriteCacheEvictor#lastTxid}. There may
   * exist some gap between it and the last updated txid before cluster
   * restarts. We think the gap will not have a large impact on the eviction.
   * And this gap can be narrowed by setting a smaller access time precision
   * and setting a smaller scheduling interval for
   * {@link WriteCacheEvictor#evictExecutor}.
   */
  public long initTxid() {
    return fsNamesystem.getEditLog().isOpenForWrite() ?
        fsNamesystem.getEditLog().getLastWrittenTxId() : -1;
  }

  @Override
  protected void evict() {
    updateCacheByLRU();
    Set<Long> blkCollectIdRemoved = removeCache();
    evictSet.removeAll(blkCollectIdRemoved);
  }

  @Override
  protected void restore() {
    super.restore();
    evictSet.addAll(evictQueue);
  }

  void updateCacheByLRU() {
    try {
      List<Event> eventList = fetchAccessModifyEvent();
      for (Event event : eventList) {
        assert event instanceof MetadataUpdateEvent;
        String eventPath = ((MetadataUpdateEvent) event).getPath();
        long blkCollectId = fsNamesystem.getMountManager().
            getSyncMountManager().getBlkCollectId(eventPath);
        updateEvictQueue(blkCollectId);
      }
    } catch (IOException e) {
      // Just report a warning.
      log.warn("Failed to update eviction order according to LRU policy.",
          e.getMessage());
    }
  }

  @Override
  protected synchronized void add(long blkCollectId) {
    // To make sure unique block collection ID is kept in evictQueue.
    if (evictSet.contains(blkCollectId)) {
      evictQueue.remove(blkCollectId);
    }
    super.add(blkCollectId);
    this.evictSet.add(blkCollectId);
  }

  /**
   * For any read, write (append, truncate) operation on synced data,
   * the evict queue order needs to be updated.
   */
  public synchronized void updateEvictQueue(long blkCollectId)
      throws IOException {
    if (!evictSet.contains(blkCollectId)) {
      return;
    }
    // Move given block collection id to the tail.
    evictQueue.remove(blkCollectId);
    evictQueue.add(blkCollectId);
  }

  private List<Event> fetchAccessModifyEvent() throws IOException {
    if (lastTxid == -1) {
      log.warn("Last txid is -1, skip fetching event from edit log");
      lastTxid = initTxid();
      return Collections.emptyList();
    }
    FSEditLog editLog = fsNamesystem.getFSImage().getEditLog();
    long syncTxid = editLog.getSyncTxId();
    boolean readInProgress = syncTxid > 0;
    EventBatchList el = SecurityUtil.doAsLoginUser(
        () -> NameNodeRpcServer.getEventBatchList(
            0, lastTxid, editLog, readInProgress, 100));
    if (el.getLastTxid() == -1) {
      log.debug("No edit is found after {}", lastTxid);
      return Collections.emptyList();
    }
    updateTxid(el);
    return filterEvent(el);
  }

  public List<Event> filterEvent(EventBatchList events) {
    List<Event> ret = new LinkedList<>();
    for (EventBatch eventBatch : events.getBatches()) {
      ret.addAll(Arrays.stream(eventBatch.getEvents())
          .filter(this::predicator)
          .collect(Collectors.toList()));
    }
    return ret;
  }

  public boolean predicator(Event event) {
    if (event.getEventType() != Event.EventType.METADATA) {
      return false;
    }
    MetadataUpdateEvent metaUpdateEvent = (MetadataUpdateEvent) event;
    if (metaUpdateEvent.getMetadataType() !=
        MetadataUpdateEvent.MetadataType.TIMES) {
      return false;
    }
    if (!(metaUpdateEvent.getAtime() > 0 || metaUpdateEvent.getMtime() > 0)) {
      return false;
    }
    List<ProvidedVolumeInfo> providedVols = fsNamesystem
        .getMountManager()
        .getSyncMountManager()
        .getWriteBackMounts();
    List<String> writeBackPaths = providedVols.stream()
        .map(providedVol -> providedVol.getMountPath())
        .collect(Collectors.toList());
    boolean isUnderMountPath = false;
    for (String mountPath : writeBackPaths) {
      isUnderMountPath |= metaUpdateEvent.getPath().startsWith(mountPath);
    }
    return isUnderMountPath;
  }

  /**
   * Update txid with the fetched event's last txid.
   *
   * @param currentEvents the current fetched events.
   */
  public void updateTxid(EventBatchList currentEvents) {
    this.lastTxid = currentEvents.getLastTxid();
  }

  public Set<Long> getEvictSet() {
    return Collections.unmodifiableSet(evictSet);
  }

  @Override
  public List<Long> removeCacheUnderMount(String mountPath)
      throws IOException {
    List<Long> blkCollectIds = super.removeCacheUnderMount(mountPath);
    evictSet.removeAll(blkCollectIds);
    return blkCollectIds;
  }
}
