/**
* Copyright 2010 The Apache Software Foundation
*
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
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This class delegates to the BlockingQueue but wraps all Stores in
 * compaction requests that hold the priority and the date requested.
 *
 * Implementation Note: With an elevation time of -1 there is the potential for
 * starvation of the lower priority compaction requests as long as there is a
 * constant stream of high priority requests.
 */
public class PriorityCompactionQueue implements BlockingQueue<CompactionRequest> {
  static final Log LOG = LogFactory.getLog(PriorityCompactionQueue.class);

  /** The actual blocking queue we delegate to */
  protected final BlockingQueue<CompactionRequest> queue =
    new PriorityBlockingQueue<CompactionRequest>();

  /** Hash map of the Stores contained within the Compaction Queue */
  private final HashMap<Pair<HRegion, Store>, CompactionRequest> storesInQueue =
    new HashMap<Pair<HRegion, Store>, CompactionRequest>();

  /** Creates a new PriorityCompactionQueue with no priority elevation time */
  public PriorityCompactionQueue() {
    LOG.debug("Create PriorityCompactionQueue");
  }

  protected Pair<HRegion, Store> toPair(CompactionRequest cr) {
    return Pair.newPair(cr.getHRegion(), cr.getStore());
  }

  /** If the store is not already in the queue it will add it and return a
   * new compaction request object.  If it is already present in the queue
   * then it will return null.
   * @param p If null it will use the default priority
   * @return returns a compaction request if it isn't already in the queue
   */
  protected CompactionRequest addToCompactionQueue(CompactionRequest newRequest) {
    CompactionRequest queuedRequest = null;
    synchronized (storesInQueue) {
      queuedRequest = storesInQueue.get(toPair(newRequest));
      if (queuedRequest == null ||
          newRequest.getPriority() < queuedRequest.getPriority()) {
        String reason = "";
        if (queuedRequest != null) {
          if (newRequest.getPriority() < queuedRequest.getPriority()) {
            reason = "Reason : priority changed from " +
              queuedRequest.getPriority() + " to " +
              newRequest.getPriority() + ". ";
          }
        }
        LOG.debug("Inserting store in queue. " + reason + newRequest);
        storesInQueue.put(toPair(newRequest), newRequest);
      } else {
        LOG.debug("Store already in queue, skipping. Queued: " + queuedRequest +
          ", requested: " + newRequest);
        newRequest = null; // It is already present so don't add it
      }
    }

    if (newRequest != null && queuedRequest != null) {
      // Remove the lower priority request
      queue.remove(queuedRequest);
    }

    return newRequest;
  }

  /** Removes the request from the stores in queue
   * @param remove
   */
  protected CompactionRequest removeFromQueue(CompactionRequest c) {
    if (c == null) return null;

    synchronized (storesInQueue) {
      CompactionRequest cr = storesInQueue.remove(toPair(c));
      if (cr != null && !cr.equals(c))
      {
        //Because we don't synchronize across both this.regionsInQueue and this.queue
        //a rare race condition exists where a higher priority compaction request replaces
        //the lower priority request in this.regionsInQueue but the lower priority request
        //is taken off this.queue before the higher can be added to this.queue.
        //So if we didn't remove what we were expecting we put it back on.
        storesInQueue.put(toPair(cr), cr);
      }
      if (cr == null) {
        LOG.warn("Removed a compaction request it couldn't find in storesInQueue: " +
            "region = " + c.getHRegion() + ", store = " + c.getStore());
      }
      return cr;
    }
  }

  @Override
  public boolean add(CompactionRequest e) {
    CompactionRequest request = this.addToCompactionQueue(e);
    if (request != null) {
      boolean result = queue.add(request);
      return result;
    } else {
      return false;
    }
  }

  @Override
  public boolean offer(CompactionRequest e) {
    CompactionRequest request = this.addToCompactionQueue(e);
    return (request != null)? queue.offer(request): false;
  }

  @Override
  public void put(CompactionRequest e) throws InterruptedException {
    CompactionRequest request = this.addToCompactionQueue(e);
    if (request != null) {
      queue.put(request);
    }
  }

  @Override
  public boolean offer(CompactionRequest e, long timeout, TimeUnit unit)
  throws InterruptedException {
    CompactionRequest request = this.addToCompactionQueue(e);
    return (request != null)? queue.offer(request, timeout, unit): false;
  }

  @Override
  public CompactionRequest take() throws InterruptedException {
    CompactionRequest cr = queue.take();
    if (cr != null) {
      removeFromQueue(cr);
      return cr;
    }
    return null;
  }

  @Override
  public CompactionRequest poll(long timeout, TimeUnit unit) throws InterruptedException {
    CompactionRequest cr = queue.poll(timeout, unit);
    if (cr != null) {
      removeFromQueue(cr);
      return cr;
    }
    return null;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof CompactionRequest) {
      CompactionRequest cr = removeFromQueue((CompactionRequest) o);
      if (cr != null) {
        return queue.remove(cr);
      }
    }

    return false;
  }

  @Override
  public CompactionRequest remove() {
    CompactionRequest cr = queue.remove();
    if (cr != null) {
      removeFromQueue(cr);
      return cr;
    }
    return null;
  }

  @Override
  public CompactionRequest poll() {
    CompactionRequest cr = queue.poll();
    if (cr != null) {
      removeFromQueue(cr);
      return cr;
    }
    return null;
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public boolean contains(Object r) {
    if (r instanceof CompactionRequest) {
      synchronized (storesInQueue) {
        return storesInQueue.containsKey(toPair((CompactionRequest) r));
      }
    } else if (r instanceof CompactionRequest) {
      return queue.contains(r);
    }
    return false;
  }

  @Override
  public CompactionRequest element() {
    CompactionRequest cr = queue.element();
    return (cr != null)? cr: null;
  }

  @Override
  public CompactionRequest peek() {
    CompactionRequest cr = queue.peek();
    return (cr != null)? cr: null;
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public void clear() {
    storesInQueue.clear();
    queue.clear();
  }

  // Unimplemented methods, collection methods

  @Override
  public Iterator<CompactionRequest> iterator() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean addAll(Collection<? extends CompactionRequest> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int drainTo(Collection<? super CompactionRequest> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int drainTo(Collection<? super CompactionRequest> c, int maxElements) {
    throw new UnsupportedOperationException("Not supported.");
  }
}
