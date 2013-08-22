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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.PathCacheDirective;
import org.apache.hadoop.hdfs.protocol.PathCacheEntry;
import org.apache.hadoop.hdfs.protocol.AddPathCacheDirectiveException.UnexpectedAddPathCacheDirectiveException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.InvalidIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.NoSuchIdException;
import org.apache.hadoop.hdfs.protocol.RemovePathCacheEntryException.UnexpectedRemovePathCacheEntryException;
import org.apache.hadoop.util.Fallible;

/**
 * The Cache Manager handles caching on DataNodes.
 */
final class CacheManager {
  public static final Log LOG = LogFactory.getLog(CacheManager.class);

  /**
   * Cache entries, sorted by ID.
   *
   * listPathCacheEntries relies on the ordering of elements in this map 
   * to track what has already been listed by the client.
   */
  private final TreeMap<Long, PathCacheEntry> entriesById =
      new TreeMap<Long, PathCacheEntry>();

  /**
   * Cache entries, sorted by directive.
   */
  private final TreeMap<PathCacheDirective, PathCacheEntry> entriesByDirective =
      new TreeMap<PathCacheDirective, PathCacheEntry>();

  /**
   * The entry ID to use for a new entry.
   */
  private long nextEntryId;

  CacheManager(FSDirectory dir, Configuration conf) {
    // TODO: support loading and storing of the CacheManager state
    clear();
  }

  synchronized void clear() {
    entriesById.clear();
    entriesByDirective.clear();
    nextEntryId = 1;
  }

  synchronized long getNextEntryId() throws IOException {
    if (nextEntryId == Long.MAX_VALUE) {
      throw new IOException("no more available IDs");
    }
    return nextEntryId++;
  }

  private synchronized Fallible<PathCacheEntry> addDirective(
        PathCacheDirective directive) {
    try {
      directive.validate();
    } catch (IOException ioe) {
      return new Fallible<PathCacheEntry>(ioe);
    }
    // Check if we already have this entry.
    PathCacheEntry existing = entriesByDirective.get(directive);
    if (existing != null) {
      // Entry already exists: return existing entry.
      return new Fallible<PathCacheEntry>(existing);
    }
    // Add a new entry with the next available ID.
    PathCacheEntry entry;
    try {
      entry = new PathCacheEntry(getNextEntryId(), directive);
    } catch (IOException ioe) {
      return new Fallible<PathCacheEntry>(
          new UnexpectedAddPathCacheDirectiveException(directive));
    }
    entriesByDirective.put(directive, entry);
    entriesById.put(entry.getEntryId(), entry);
    return new Fallible<PathCacheEntry>(entry);
  }

  public synchronized List<Fallible<PathCacheEntry>> addDirectives(
      List<PathCacheDirective> directives) {
    ArrayList<Fallible<PathCacheEntry>> results = 
        new ArrayList<Fallible<PathCacheEntry>>(directives.size());
    for (PathCacheDirective directive: directives) {
      results.add(addDirective(directive));
    }
    return results;
  }

  private synchronized Fallible<Long> removeEntry(long entryId) {
    // Check for invalid IDs.
    if (entryId <= 0) {
      return new Fallible<Long>(new InvalidIdException(entryId));
    }
    // Find the entry.
    PathCacheEntry existing = entriesById.get(entryId);
    if (existing == null) {
      return new Fallible<Long>(new NoSuchIdException(entryId));
    }
    // Remove the corresponding entry in entriesByDirective.
    if (entriesByDirective.remove(existing.getDirective()) == null) {
      return new Fallible<Long>(
          new UnexpectedRemovePathCacheEntryException(entryId));
    }
    entriesById.remove(entryId);
    return new Fallible<Long>(entryId);
  }

  public synchronized List<Fallible<Long>> removeEntries(List<Long> entryIds) {
    ArrayList<Fallible<Long>> results = 
        new ArrayList<Fallible<Long>>(entryIds.size());
    for (Long entryId : entryIds) {
      results.add(removeEntry(entryId));
    }
    return results;
  }

  public synchronized List<PathCacheEntry> listPathCacheEntries(long prevId,
      String pool, int maxReplies) {
    final int MAX_PRE_ALLOCATED_ENTRIES = 16;
    ArrayList<PathCacheEntry> replies =
        new ArrayList<PathCacheEntry>(Math.min(MAX_PRE_ALLOCATED_ENTRIES, maxReplies));
    int numReplies = 0;
    SortedMap<Long, PathCacheEntry> tailMap = entriesById.tailMap(prevId + 1);
    for (Entry<Long, PathCacheEntry> cur : tailMap.entrySet()) {
      if (numReplies >= maxReplies) {
        return replies;
      }
      if (pool.isEmpty() || cur.getValue().getDirective().
            getPool().equals(pool)) {
        replies.add(cur.getValue());
        numReplies++;
      }
    }
    return replies;
  }
}
