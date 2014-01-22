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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A RemoteIterator that fetches elements in batches.
 */
public abstract class BatchedRemoteIterator<K, E> implements RemoteIterator<E> {
  public interface BatchedEntries<E> {
    public E get(int i);
    public int size();
    public boolean hasMore();
  }

  public static class BatchedListEntries<E> implements BatchedEntries<E> {
    private final List<E> entries;
    private final boolean hasMore;

    public BatchedListEntries(List<E> entries, boolean hasMore) {
      this.entries = entries;
      this.hasMore = hasMore;
    }

    public E get(int i) {
      return entries.get(i);
    }

    public int size() {
      return entries.size();
    }

    public boolean hasMore() {
      return hasMore;
    }
  }

  private K prevKey;
  private BatchedEntries<E> entries;
  private int idx;

  public BatchedRemoteIterator(K prevKey) {
    this.prevKey = prevKey;
    this.entries = null;
    this.idx = -1;
  }

  /**
   * Perform the actual remote request.
   * 
   * @param prevKey The key to send.
   * @return A list of replies.
   */
  public abstract BatchedEntries<E> makeRequest(K prevKey) throws IOException;

  private void makeRequest() throws IOException {
    idx = 0;
    entries = null;
    entries = makeRequest(prevKey);
    if (entries.size() == 0) {
      entries = null;
    }
  }

  private void makeRequestIfNeeded() throws IOException {
    if (idx == -1) {
      makeRequest();
    } else if ((entries != null) && (idx >= entries.size())) {
      if (!entries.hasMore()) {
        // Last time, we got fewer entries than requested.
        // So we should be at the end.
        entries = null;
      } else {
        makeRequest();
      }
    }
  }

  @Override
  public boolean hasNext() throws IOException {
    makeRequestIfNeeded();
    return (entries != null);
  }

  /**
   * Return the next list key associated with an element.
   */
  public abstract K elementToPrevKey(E element);

  @Override
  public E next() throws IOException {
    makeRequestIfNeeded();
    if (entries == null) {
      throw new NoSuchElementException();
    }
    E entry = entries.get(idx++);
    prevKey = elementToPrevKey(entry);
    return entry;
  }
}
