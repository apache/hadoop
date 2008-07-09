/** * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.mapred.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A RecordReader that can effect joins of RecordReaders sharing a common key
 * type and partitioning.
 */
public abstract class CompositeRecordReader<
    K extends WritableComparable, // key type
    V extends Writable,           // accepts RecordReader<K,V> as children
    X extends Writable>           // emits Writables of this type
    implements Configurable {


  private int id;
  private Configuration conf;
  private final ResetableIterator<X> EMPTY = new ResetableIterator.EMPTY<X>();

  private WritableComparator cmp;
  private Class<? extends WritableComparable> keyclass;
  private PriorityQueue<ComposableRecordReader<K,?>> q;

  protected final JoinCollector jc;
  protected final ComposableRecordReader<K,? extends V>[] kids;

  protected abstract boolean combine(Object[] srcs, TupleWritable value);

  /**
   * Create a RecordReader with <tt>capacity</tt> children to position
   * <tt>id</tt> in the parent reader.
   * The id of a root CompositeRecordReader is -1 by convention, but relying
   * on this is not recommended.
   */
  @SuppressWarnings("unchecked") // Generic array assignment
  public CompositeRecordReader(int id, int capacity,
      Class<? extends WritableComparator> cmpcl)
      throws IOException {
    assert capacity > 0 : "Invalid capacity";
    this.id = id;
    if (null != cmpcl) {
      cmp = (WritableComparator)ReflectionUtils.newInstance(cmpcl, null);
      q = new PriorityQueue<ComposableRecordReader<K,?>>(3,
          new Comparator<ComposableRecordReader<K,?>>() {
            public int compare(ComposableRecordReader<K,?> o1,
                               ComposableRecordReader<K,?> o2) {
              return cmp.compare(o1.key(), o2.key());
            }
          });
    }
    jc = new JoinCollector(capacity);
    kids = new ComposableRecordReader[capacity];
  }

  /**
   * Return the position in the collector this class occupies.
   */
  public int id() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * {@inheritDoc}
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * Return sorted list of RecordReaders for this composite.
   */
  protected PriorityQueue<ComposableRecordReader<K,?>> getRecordReaderQueue() {
    return q;
  }

  /**
   * Return comparator defining the ordering for RecordReaders in this
   * composite.
   */
  protected WritableComparator getComparator() {
    return cmp;
  }

  /**
   * Add a RecordReader to this collection.
   * The id() of a RecordReader determines where in the Tuple its
   * entry will appear. Adding RecordReaders with the same id has
   * undefined behavior.
   */
  public void add(ComposableRecordReader<K,? extends V> rr) throws IOException {
    kids[rr.id()] = rr;
    if (null == q) {
      cmp = WritableComparator.get(rr.createKey().getClass());
      q = new PriorityQueue<ComposableRecordReader<K,?>>(3,
          new Comparator<ComposableRecordReader<K,?>>() {
            public int compare(ComposableRecordReader<K,?> o1,
                               ComposableRecordReader<K,?> o2) {
              return cmp.compare(o1.key(), o2.key());
            }
          });
    }
    if (rr.hasNext()) {
      q.add(rr);
    }
  }

  /**
   * Collector for join values.
   * This accumulates values for a given key from the child RecordReaders. If
   * one or more child RR contain duplicate keys, this will emit the cross
   * product of the associated values until exhausted.
   */
  class JoinCollector {
    private K key;
    private ResetableIterator<X>[] iters;
    private long partial = 0L;
    private long replaymask = 0L;
    private int start = 0;
    private int pos = -1;
    private int iterpos = -1;
    private boolean first = true;

    /**
     * Construct a collector capable of handling the specified number of
     * children.
     */
    @SuppressWarnings("unchecked") // Generic array assignment
    public JoinCollector(int card) {
      iters = new ResetableIterator[card];
      for (int i = 0; i < iters.length; ++i) {
        iters[i] = EMPTY;
      }
    }

    /**
     * Register a given iterator at position id.
     */
    public void add(int id, ResetableIterator<X> i)
        throws IOException {
      iters[id] = i;
    }

    /**
     * Return the key associated with this collection.
     */
    public K key() {
      return key;
    }

    /**
     * Codify the contents of the collector to be iterated over.
     * When this is called, all RecordReaders registered for this
     * key should have added ResetableIterators.
     */
    public void reset(K key) {
      this.key = key;
      start = 0;
      pos = 0;
      first = true;
      partial = 0L;
      for (int i = 0; i < iters.length; ++i) {
        iters[i].reset();
      }
    }

    /**
     * Clear all state information.
     */
    public void clear() {
      key = null;
      pos = -1;
      first = true;
      for (int i = 0; i < iters.length; ++i) {
        iters[i].clear();
        iters[i] = EMPTY;
      }
      partial = 0L;
    }

    /**
     * Returns false if exhausted or if reset(K) has not been called.
     */
    protected boolean hasNext() {
      return !(pos < 0);
    }

    /**
     * Populate Tuple from iterators.
     * It should be the case that, given iterators i_1...i_n over values from
     * sources s_1...s_n sharing key k, repeated calls to next should yield
     * I x I.
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    protected boolean next(TupleWritable val) throws IOException {
      if (pos < 0) {
        clear();
        return false;
      }
      int i = start;
      if (first) { // Find first iterator with elements
        for (; i < iters.length && !iters[i].hasNext(); ++i);
        if (iters.length <= i) { // no children had key
          clear();
          return false;
        }
        start = i;
        for (int j = i; j < iters.length; ++j) {
          if (iters[j].hasNext()) {
            partial |= 1 << j;
          }
        }
        iterpos = pos = iters.length - 1;
        first = false;
      } else { // Copy all elements in partial into tuple
        for (; i < iterpos; ++i) {
          if ((partial & (1 << i)) != 0) {
            iters[i].replay((X)val.get(i));
            val.setWritten(i);
          }
        }
      }
      long partialwritten = val.mask();
      if (iters[i].next((X)val.get(i))) {
        val.setWritten(i);
      }
      for (++i; i < iters.length; ++i) {
        iters[i].reset();
        if (iters[i].hasNext() && iters[i].next((X)val.get(i))) {
          val.setWritten(i);
        }
      }
      iterpos = iters.length - 1;
      for (; iterpos > pos && !iters[iterpos].hasNext(); --iterpos);
      if (!iters[iterpos].hasNext()) {
        for (; !(pos < 0 || iters[pos].hasNext()); --pos);
        iterpos = pos;
      }
      replaymask = val.mask();
      if ((replaymask ^ partialwritten) == 0L) {
        return next(val);
      }
      return true;
    }

    /**
     * Replay the last Tuple emitted.
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    public void replay(TupleWritable val) throws IOException {
      // The last emitted tuple might have drawn on an empty source;
      // it can't be cleared prematurely, b/c there may be more duplicate
      // keys in iterator positions < pos
      if (first) {
        throw new IllegalStateException();
      }
      for (int i = 0; i < iters.length; ++i) {
        if ((replaymask & (1 << i)) != 0) {
          iters[i].replay((X)val.get(i));
        }
      }
    }

    /**
     * Close all child iterators.
     */
    public void close() throws IOException {
      for (int i = 0; i < iters.length; ++i) {
        iters[i].close();
      }
    }

    /**
     * Write the next value into key, value as accepted by the operation
     * associated with this set of RecordReaders.
     */
    public boolean flush(TupleWritable value) throws IOException {
      while (hasNext()) {
        value.clearWritten();
        if (next(value) && combine(kids, value)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Return the key for the current join or the value at the top of the
   * RecordReader heap.
   */
  public K key() {
    if (jc.hasNext()) {
      return jc.key();
    }
    if (!q.isEmpty()) {
      return q.peek().key();
    }
    return null;
  }

  /**
   * Clone the key at the top of this RR into the given object.
   */
  public void key(K key) throws IOException {
    WritableUtils.cloneInto(key, key());
  }

  /**
   * Return true if it is possible that this could emit more values.
   */
  public boolean hasNext() {
    return jc.hasNext() || !q.isEmpty();
  }

  /**
   * Pass skip key to child RRs.
   */
  public void skip(K key) throws IOException {
    ArrayList<ComposableRecordReader<K,?>> tmp =
      new ArrayList<ComposableRecordReader<K,?>>();
    while (!q.isEmpty() && cmp.compare(q.peek().key(), key) <= 0) {
      tmp.add(q.poll());
    }
    for (ComposableRecordReader<K,?> rr : tmp) {
      rr.skip(key);
      if (rr.hasNext()) {
        q.add(rr);
      }
    }
  }

  /**
   * Obtain an iterator over the child RRs apropos of the value type
   * ultimately emitted from this join.
   */
  protected abstract ResetableIterator<X> getDelegate();

  /**
   * If key provided matches that of this Composite, give JoinCollector
   * iterator over values it may emit.
   */
  @SuppressWarnings("unchecked") // No values from static EMPTY class
  public void accept(CompositeRecordReader.JoinCollector jc, K key)
      throws IOException {
    if (hasNext() && 0 == cmp.compare(key, key())) {
      fillJoinCollector(createKey());
      jc.add(id, getDelegate());
      return;
    }
    jc.add(id, EMPTY);
  }

  /**
   * For all child RRs offering the key provided, obtain an iterator
   * at that position in the JoinCollector.
   */
  protected void fillJoinCollector(K iterkey) throws IOException {
    if (!q.isEmpty()) {
      q.peek().key(iterkey);
      while (0 == cmp.compare(q.peek().key(), iterkey)) {
        ComposableRecordReader<K,?> t = q.poll();
        t.accept(jc, iterkey);
        if (t.hasNext()) {
          q.add(t);
        } else if (q.isEmpty()) {
          return;
        }
      }
    }
  }

  /**
   * Implement Comparable contract (compare key of join or head of heap
   * with that of another).
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * Create a new key value common to all child RRs.
   * @throws ClassCastException if key classes differ.
   */
  @SuppressWarnings("unchecked") // Explicit check for key class agreement
  public K createKey() {
    if (null == keyclass) {
      final Class<?> cls = kids[0].createKey().getClass();
      for (RecordReader<K,? extends Writable> rr : kids) {
        if (!cls.equals(rr.createKey().getClass())) {
          throw new ClassCastException("Child key classes fail to agree");
        }
      }
      keyclass = cls.asSubclass(WritableComparable.class);
    }
    return (K) ReflectionUtils.newInstance(keyclass, getConf());
  }

  /**
   * Create a value to be used internally for joins.
   */
  protected TupleWritable createInternalValue() {
    Writable[] vals = new Writable[kids.length];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = kids[i].createValue();
    }
    return new TupleWritable(vals);
  }

  /**
   * Unsupported (returns zero in all cases).
   */
  public long getPos() throws IOException {
    return 0;
  }

  /**
   * Close all child RRs.
   */
  public void close() throws IOException {
    if (kids != null) {
      for (RecordReader<K,? extends Writable> rr : kids) {
        rr.close();
      }
    }
    if (jc != null) {
      jc.close();
    }
  }

  /**
   * Report progress as the minimum of all child RR progress.
   */
  public float getProgress() throws IOException {
    float ret = 1.0f;
    for (RecordReader<K,? extends Writable> rr : kids) {
      ret = Math.min(ret, rr.getProgress());
    }
    return ret;
  }
}
