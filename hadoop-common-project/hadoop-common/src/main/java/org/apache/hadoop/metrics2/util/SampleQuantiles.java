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

package org.apache.hadoop.metrics2.util;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm
 * for streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 * 
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 * 
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 * 
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * 
 */
@InterfaceAudience.Private
public class SampleQuantiles implements QuantileEstimator {

  /**
   * Total number of items in stream
   */
  private long count = 0;

  /**
   * Current list of sampled items, maintained in sorted order with error bounds
   */
  private LinkedList<SampleItem> samples;

  /**
   * Buffers incoming items to be inserted in batch. Items are inserted into 
   * the buffer linearly. When the buffer fills, it is flushed into the samples
   * array in its entirety.
   */
  private long[] buffer = new long[500];
  private int bufferCount = 0;

  /**
   * Array of Quantiles that we care about, along with desired error.
   */
  private final Quantile quantiles[];

  public SampleQuantiles(Quantile[] quantiles) {
    this.quantiles = quantiles;
    this.samples = new LinkedList<SampleItem>();
  }

  /**
   * Specifies the allowable error for this rank, depending on which quantiles
   * are being targeted.
   * 
   * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
   * the range of this rank can be.
   * 
   * @param rank
   *          the index in the list of samples
   */
  private double allowableError(int rank) {
    int size = samples.size();
    double minError = size + 1;
    for (Quantile q : quantiles) {
      double error;
      if (rank <= q.quantile * size) {
        error = (2.0 * q.error * (size - rank)) / (1.0 - q.quantile);
      } else {
        error = (2.0 * q.error * rank) / q.quantile;
      }
      if (error < minError) {
        minError = error;
      }
    }

    return minError;
  }

  /**
   * Add a new value from the stream.
   * 
   * @param v
   */
  synchronized public void insert(long v) {
    buffer[bufferCount] = v;
    bufferCount++;

    count++;

    if (bufferCount == buffer.length) {
      insertBatch();
      compress();
    }
  }

  /**
   * Merges items from buffer into the samples array in one pass.
   * This is more efficient than doing an insert on every item.
   */
  private void insertBatch() {
    if (bufferCount == 0) {
      return;
    }

    Arrays.sort(buffer, 0, bufferCount);

    // Base case: no samples
    int start = 0;
    if (samples.size() == 0) {
      SampleItem newItem = new SampleItem(buffer[0], 1, 0);
      samples.add(newItem);
      start++;
    }

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem item = it.next();
    for (int i = start; i < bufferCount; i++) {
      long v = buffer[i];
      while (it.nextIndex() < samples.size() && item.value < v) {
        item = it.next();
      }
      // If we found that bigger item, back up so we insert ourselves before it
      if (item.value > v) {
        it.previous();
      }
      // We use different indexes for the edge comparisons, because of the above
      // if statement that adjusts the iterator
      int delta;
      if (it.previousIndex() == 0 || it.nextIndex() == samples.size()) {
        delta = 0;
      } else {
        delta = ((int) Math.floor(allowableError(it.nextIndex()))) - 1;
      }
      SampleItem newItem = new SampleItem(v, 1, delta);
      it.add(newItem);
      item = newItem;
    }

    bufferCount = 0;
  }

  /**
   * Try to remove extraneous items from the set of sampled items. This checks
   * if an item is unnecessary based on the desired error bounds, and merges it
   * with the adjacent item if it is.
   */
  private void compress() {
    if (samples.size() < 2) {
      return;
    }

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem prev = null;
    SampleItem next = it.next();

    while (it.hasNext()) {
      prev = next;
      next = it.next();
      if (prev.g + next.g + next.delta <= allowableError(it.previousIndex())) {
        next.g += prev.g;
        // Remove prev. it.remove() kills the last thing returned.
        it.previous();
        it.previous();
        it.remove();
        // it.next() is now equal to next, skip it back forward again
        it.next();
      }
    }
  }

  /**
   * Get the estimated value at the specified quantile.
   * 
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  private long query(double quantile) {
    Preconditions.checkState(!samples.isEmpty(), "no data in estimator");

    int rankMin = 0;
    int desired = (int) (quantile * count);

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem prev = null;
    SampleItem cur = it.next();
    for (int i = 1; i < samples.size(); i++) {
      prev = cur;
      cur = it.next();

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + (allowableError(i) / 2)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return samples.get(samples.size() - 1).value;
  }

  /**
   * Get a snapshot of the current values of all the tracked quantiles.
   * 
   * @return snapshot of the tracked quantiles. If no items are added
   * to the estimator, returns null.
   */
  synchronized public Map<Quantile, Long> snapshot() {
    // flush the buffer first for best results
    insertBatch();
    
    if (samples.isEmpty()) {
      return null;
    }
    
    Map<Quantile, Long> values = new TreeMap<Quantile, Long>();
    for (int i = 0; i < quantiles.length; i++) {
      values.put(quantiles[i], query(quantiles[i].quantile));
    }

    return values;
  }

  /**
   * Returns the number of items that the estimator has processed
   * 
   * @return count total number of items processed
   */
  synchronized public long getCount() {
    return count;
  }

  /**
   * Returns the number of samples kept by the estimator
   * 
   * @return count current number of samples
   */
  @VisibleForTesting
  synchronized public int getSampleCount() {
    return samples.size();
  }

  /**
   * Resets the estimator, clearing out all previously inserted items
   */
  synchronized public void clear() {
    count = 0;
    bufferCount = 0;
    samples.clear();
  }
  
  @Override
  synchronized public String toString() {
    Map<Quantile, Long> data = snapshot();
    if (data == null) {
      return "[no samples]";
    } else {
      return Joiner.on("\n").withKeyValueSeparator(": ").join(data);
    }
  }

  /**
   * Describes a measured value passed to the estimator, tracking additional
   * metadata required by the CKMS algorithm.
   */
  private static class SampleItem {
    
    /**
     * Value of the sampled item (e.g. a measured latency value)
     */
    public final long value;
    
    /**
     * Difference between the lowest possible rank of the previous item, and 
     * the lowest possible rank of this item.
     * 
     * The sum of the g of all previous items yields this item's lower bound. 
     */
    public int g;
    
    /**
     * Difference between the item's greatest possible rank and lowest possible
     * rank.
     */
    public final int delta;

    public SampleItem(long value, int lowerDelta, int delta) {
      this.value = value;
      this.g = lowerDelta;
      this.delta = delta;
    }

    @Override
    public String toString() {
      return String.format("%d, %d, %d", value, g, delta);
    }
  }
}
