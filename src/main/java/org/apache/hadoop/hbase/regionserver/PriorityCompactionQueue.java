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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.CompactSplitThread.Priority;

/**
 * This class delegates to the BlockingQueue but wraps all HRegions in
 * compaction requests that hold the priority and the date requested.
 *
 * Implementation Note: With an elevation time of -1 there is the potential for
 * starvation of the lower priority compaction requests as long as there is a
 * constant stream of high priority requests.
 */
public class PriorityCompactionQueue implements BlockingQueue<HRegion> {
  static final Log LOG = LogFactory.getLog(PriorityCompactionQueue.class);

  /**
   * This class represents a compaction request and holds the region, priority,
   * and time submitted.
   */
  private class CompactionRequest implements Comparable<CompactionRequest> {
    private final HRegion r;
    private final Priority p;
    private final Date date;

    public CompactionRequest(HRegion r, Priority p) {
      this(r, p, null);
    }

    public CompactionRequest(HRegion r, Priority p, Date d) {
      if (r == null) {
        throw new NullPointerException("HRegion cannot be null");
      }

      if (p == null) {
        p = Priority.NORMAL; //the default priority
      }

      if (d == null) {
        d = new Date();
      }

      this.r = r;
      this.p = p;
      this.date = d;
    }

    /**
     * This function will define where in the priority queue the request will
     * end up.  Those with the highest priorities will be first.  When the
     * priorities are the same it will It will first compare priority then date
     * to maintain a FIFO functionality.
     *
     * <p>Note: The date is only accurate to the millisecond which means it is
     * possible that two requests were inserted into the queue within a
     * millisecond.  When that is the case this function will break the tie
     * arbitrarily.
     */
    @Override
    public int compareTo(CompactionRequest request) {
      //NOTE: The head of the priority queue is the least element
      if (this.equals(request)) {
        return 0; //they are the same request
      }
      int compareVal;

      compareVal = p.compareTo(request.p); //compare priority
      if (compareVal != 0) {
        return compareVal;
      }

      compareVal = date.compareTo(request.date);
      if (compareVal != 0) {
        return compareVal;
      }

      //break the tie arbitrarily
      return -1;
    }

    /** Gets the HRegion for the request */
    HRegion getHRegion() {
      return r;
    }

    /** Gets the priority for the request */
    Priority getPriority() {
      return p;
    }

    public String toString() {
      return "regionName=" + r.getRegionNameAsString() +
        ", priority=" + p + ", date=" + date;
    }
  }

  /** The actual blocking queue we delegate to */
  protected final BlockingQueue<CompactionRequest> queue =
    new PriorityBlockingQueue<CompactionRequest>();

  /** Hash map of the HRegions contained within the Compaction Queue */
  private final HashMap<HRegion, CompactionRequest> regionsInQueue =
    new HashMap<HRegion, CompactionRequest>();

  /** Creates a new PriorityCompactionQueue with no priority elevation time */
  public PriorityCompactionQueue() {
    LOG.debug("Create PriorityCompactionQueue");
  }

  /** If the region is not already in the queue it will add it and return a
   * new compaction request object.  If it is already present in the queue
   * then it will return null.
   * @param p If null it will use the default priority
   * @return returns a compaction request if it isn't already in the queue
   */
  protected CompactionRequest addToRegionsInQueue(HRegion r, Priority p) {
    CompactionRequest queuedRequest = null;
    CompactionRequest newRequest = new CompactionRequest(r, p);
    synchronized (regionsInQueue) {
      queuedRequest = regionsInQueue.get(r);
      if (queuedRequest == null ||
          newRequest.getPriority().compareTo(queuedRequest.getPriority()) < 0) {
        LOG.trace("Inserting region in queue. " + newRequest);
        regionsInQueue.put(r, newRequest);
      } else {
        LOG.trace("Region already in queue, skipping. Queued: " + queuedRequest +
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

  /** Removes the request from the regions in queue
   * @param p If null it will use the default priority
   */
  protected CompactionRequest removeFromRegionsInQueue(HRegion r) {
    if (r == null) return null;

    synchronized (regionsInQueue) {
      CompactionRequest cr = regionsInQueue.remove(r);
      if (cr == null) {
        LOG.warn("Removed a region it couldn't find in regionsInQueue: " + r);
      }
      return cr;
    }
  }

  public boolean add(HRegion e, Priority p) {
    CompactionRequest request = this.addToRegionsInQueue(e, p);
    if (request != null) {
      boolean result = queue.add(request);
      queue.peek();
      return result;
    } else {
      return false;
    }
  }

  @Override
  public boolean add(HRegion e) {
    return add(e, null);
  }

  public boolean offer(HRegion e, Priority p) {
    CompactionRequest request = this.addToRegionsInQueue(e, p);
    return (request != null)? queue.offer(request): false;
  }

  @Override
  public boolean offer(HRegion e) {
    return offer(e, null);
  }

  public void put(HRegion e, Priority p) throws InterruptedException {
    CompactionRequest request = this.addToRegionsInQueue(e, p);
    if (request != null) {
      queue.put(request);
    }
  }

  @Override
  public void put(HRegion e) throws InterruptedException {
    put(e, null);
  }

  public boolean offer(HRegion e, Priority p, long timeout, TimeUnit unit)
  throws InterruptedException {
    CompactionRequest request = this.addToRegionsInQueue(e, p);
    return (request != null)? queue.offer(request, timeout, unit): false;
  }

  @Override
  public boolean offer(HRegion e, long timeout, TimeUnit unit)
  throws InterruptedException {
    return offer(e, null, timeout, unit);
  }

  @Override
  public HRegion take() throws InterruptedException {
    CompactionRequest cr = queue.take();
    if (cr != null) {
      removeFromRegionsInQueue(cr.getHRegion());
      return cr.getHRegion();
    }
    return null;
  }

  @Override
  public HRegion poll(long timeout, TimeUnit unit) throws InterruptedException {
    CompactionRequest cr = queue.poll(timeout, unit);
    if (cr != null) {
      removeFromRegionsInQueue(cr.getHRegion());
      return cr.getHRegion();
    }
    return null;
  }

  @Override
  public boolean remove(Object r) {
    if (r instanceof HRegion) {
      CompactionRequest cr = removeFromRegionsInQueue((HRegion) r);
      if (cr != null) {
        return queue.remove(cr);
      }
    }

    return false;
  }

  @Override
  public HRegion remove() {
    CompactionRequest cr = queue.remove();
    if (cr != null) {
      removeFromRegionsInQueue(cr.getHRegion());
      return cr.getHRegion();
    }
    return null;
  }

  @Override
  public HRegion poll() {
    CompactionRequest cr = queue.poll();
    if (cr != null) {
      removeFromRegionsInQueue(cr.getHRegion());
      return cr.getHRegion();
    }
    return null;
  }

  @Override
  public int remainingCapacity() {
    return queue.remainingCapacity();
  }

  @Override
  public boolean contains(Object r) {
    if (r instanceof HRegion) {
      synchronized (regionsInQueue) {
        return regionsInQueue.containsKey((HRegion) r);
      }
    } else if (r instanceof CompactionRequest) {
      return queue.contains(r);
    }
    return false;
  }

  @Override
  public HRegion element() {
    CompactionRequest cr = queue.element();
    return (cr != null)? cr.getHRegion(): null;
  }

  @Override
  public HRegion peek() {
    CompactionRequest cr = queue.peek();
    return (cr != null)? cr.getHRegion(): null;
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
    regionsInQueue.clear();
    queue.clear();
  }

  // Unimplemented methods, collection methods

  @Override
  public Iterator<HRegion> iterator() {
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
  public boolean addAll(Collection<? extends HRegion> c) {
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
  public int drainTo(Collection<? super HRegion> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int drainTo(Collection<? super HRegion> c, int maxElements) {
    throw new UnsupportedOperationException("Not supported.");
  }
}