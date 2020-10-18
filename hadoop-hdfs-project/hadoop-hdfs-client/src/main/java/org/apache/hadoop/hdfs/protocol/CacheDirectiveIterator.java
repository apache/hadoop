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

package org.apache.hadoop.hdfs.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.InvalidRequestException;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/**
 * CacheDirectiveIterator is a remote iterator that iterates cache directives.
 * It supports retrying in case of namenode failover.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CacheDirectiveIterator
    extends BatchedRemoteIterator<Long, CacheDirectiveEntry> {

  private CacheDirectiveInfo filter;
  private final ClientProtocol namenode;
  private final Tracer tracer;

  public CacheDirectiveIterator(ClientProtocol namenode,
      CacheDirectiveInfo filter, Tracer tracer) {
    super(0L);
    this.namenode = namenode;
    this.filter = filter;
    this.tracer = tracer;
  }

  private static CacheDirectiveInfo removeIdFromFilter(CacheDirectiveInfo filter) {
    CacheDirectiveInfo.Builder builder = new CacheDirectiveInfo.Builder(filter);
    builder.setId(null);
    return builder.build();
  }

  /**
   * Used for compatibility when communicating with a server version that
   * does not support filtering directives by ID.
   */
  private static class SingleEntry implements
      BatchedEntries<CacheDirectiveEntry> {

    private final CacheDirectiveEntry entry;

    public SingleEntry(final CacheDirectiveEntry entry) {
      this.entry = entry;
    }

    @Override
    public CacheDirectiveEntry get(int i) {
      if (i > 0) {
        return null;
      }
      return entry;
    }

    @Override
    public int size() {
      return 1;
    }

    @Override
    public boolean hasMore() {
      return false;
    }
  }

  @Override
  public BatchedEntries<CacheDirectiveEntry> makeRequest(Long prevKey)
      throws IOException {
    BatchedEntries<CacheDirectiveEntry> entries;
    try (TraceScope ignored = tracer.newScope("listCacheDirectives")) {
      entries = namenode.listCacheDirectives(prevKey, filter);
    } catch (IOException e) {
      if (e.getMessage().contains("Filtering by ID is unsupported")) {
        // Retry case for old servers, do the filtering client-side
        long id = filter.getId();
        filter = removeIdFromFilter(filter);
        // Using id - 1 as prevId should get us a window containing the id
        // This is somewhat brittle, since it depends on directives being
        // returned in order of ascending ID.
        entries = namenode.listCacheDirectives(id - 1, filter);
        for (int i = 0; i < entries.size(); i++) {
          CacheDirectiveEntry entry = entries.get(i);
          if (entry.getInfo().getId().equals(id)) {
            return new SingleEntry(entry);
          }
        }
        throw new RemoteException(InvalidRequestException.class.getName(),
            "Did not find requested id " + id);
      }
      throw e;
    }
    Preconditions.checkNotNull(entries);
    return entries;
  }

  @Override
  public Long elementToPrevKey(CacheDirectiveEntry entry) {
    return entry.getInfo().getId();
  }
}
