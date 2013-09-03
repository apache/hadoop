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
  }
  
  public static class BatchedListEntries<E> implements BatchedEntries<E> {
    private final List<E> entries;

    public BatchedListEntries(List<E> entries) {
      this.entries = entries;
    }

    public E get(int i) {
      return entries.get(i);
      
    }

    public int size() {
      return entries.size();
    }
  }

  private K nextKey;
  private final int maxRepliesPerRequest;
  private BatchedEntries<E> entries;
  private int idx;

  public BatchedRemoteIterator(K nextKey, int maxRepliesPerRequest) {
    this.nextKey = nextKey;
    this.maxRepliesPerRequest = maxRepliesPerRequest;
    this.entries = null;
    this.idx = -1;
  }

  /**
   * Perform the actual remote request.
   *
   * @param key                    The key to send.
   * @param maxRepliesPerRequest   The maximum number of replies to allow.
   * @return                       A list of replies.
   */
  public abstract BatchedEntries<E> makeRequest(K nextKey, int maxRepliesPerRequest)
      throws IOException;

  private void makeRequest() throws IOException {
    idx = 0;
    entries = null;
    entries = makeRequest(nextKey, maxRepliesPerRequest);
    if (entries.size() > maxRepliesPerRequest) {
      throw new IOException("invalid number of replies returned: got " +
          entries.size() + ", expected " + maxRepliesPerRequest +
          " at most.");
    }
    if (entries.size() == 0) {
      entries = null;
    }
  }

  private void makeRequestIfNeeded() throws IOException {
    if (idx == -1) {
      makeRequest();
    } else if ((entries != null) && (idx >= entries.size())) {
      if (entries.size() < maxRepliesPerRequest) {
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
  public abstract K elementToNextKey(E element);

  @Override
  public E next() throws IOException {
    makeRequestIfNeeded();
    if (entries == null) {
      throw new NoSuchElementException();
    }
    E entry = entries.get(idx++);
    nextKey = elementToNextKey(entry);
    return entry;
  }
}
