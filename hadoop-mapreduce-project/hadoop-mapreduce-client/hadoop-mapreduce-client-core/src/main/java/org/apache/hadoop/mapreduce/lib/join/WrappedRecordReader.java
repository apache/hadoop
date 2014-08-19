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

package org.apache.hadoop.mapreduce.lib.join;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Proxy class for a RecordReader participating in the join framework.
 * 
 * This class keeps track of the &quot;head&quot; key-value pair for the
 * provided RecordReader and keeps a store of values matching a key when
 * this source is participating in a join.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class WrappedRecordReader<K extends WritableComparable<?>,
    U extends Writable> extends ComposableRecordReader<K,U> {

  protected boolean empty = false;
  private RecordReader<K,U> rr;
  private int id;  // index at which values will be inserted in collector

  protected WritableComparator cmp = null;
  private K key; // key at the top of this RR
  private U value; // value assoc with key
  private ResetableIterator<U> vjoin;
  private Configuration conf = new Configuration();
  @SuppressWarnings("unchecked")
  private Class<? extends WritableComparable> keyclass = null; 
  private Class<? extends Writable> valueclass = null; 
  
  protected WrappedRecordReader(int id) {
    this.id = id;
    vjoin = new StreamBackedIterator<U>();
  }
  
  /**
   * For a given RecordReader rr, occupy position id in collector.
   */
  WrappedRecordReader(int id, RecordReader<K,U> rr,
      Class<? extends WritableComparator> cmpcl) 
  throws IOException, InterruptedException {
    this.id = id;
    this.rr = rr;
    if (cmpcl != null) {
      try {
        this.cmp = cmpcl.newInstance();
      } catch (InstantiationException e) {
        throw new IOException(e);
      } catch (IllegalAccessException e) {
        throw new IOException(e);
      }
    }
    vjoin = new StreamBackedIterator<U>();
  }

  public void initialize(InputSplit split,
                         TaskAttemptContext context)
  throws IOException, InterruptedException {
    rr.initialize(split, context);
    conf = context.getConfiguration();
    nextKeyValue();
    if (!empty) {
      keyclass = key.getClass().asSubclass(WritableComparable.class);
      valueclass = value.getClass();
      if (cmp == null) {
        cmp = WritableComparator.get(keyclass, conf);
      }
    }
  }

  /**
   * Request new key from proxied RR.
   */
  @SuppressWarnings("unchecked")
  public K createKey() {
    if (keyclass != null) {
      return (K) ReflectionUtils.newInstance(keyclass, conf);
    }
    return (K) NullWritable.get();
  }
  
  @SuppressWarnings("unchecked")
  public U createValue() {
    if (valueclass != null) {
      return (U) ReflectionUtils.newInstance(valueclass, conf);
    }
    return (U) NullWritable.get();
  }
  
  /** {@inheritDoc} */
  public int id() {
    return id;
  }

  /**
   * Return the key at the head of this RR.
   */
  public K key() {
    return key;
  }

  /**
   * Clone the key at the head of this RR into the object supplied.
   */
  public void key(K qkey) throws IOException {
    ReflectionUtils.copy(conf, key, qkey);
  }

  /**
   * Return true if the RR- including the k,v pair stored in this object-
   * is exhausted.
   */
  public boolean hasNext() {
    return !empty;
  }

  /**
   * Skip key-value pairs with keys less than or equal to the key provided.
   */
  public void skip(K key) throws IOException, InterruptedException {
    if (hasNext()) {
      while (cmp.compare(key(), key) <= 0 && next());
    }
  }

  /**
   * Add an iterator to the collector at the position occupied by this
   * RecordReader over the values in this stream paired with the key
   * provided (ie register a stream of values from this source matching K
   * with a collector).
   */
  @SuppressWarnings("unchecked")
  public void accept(CompositeRecordReader.JoinCollector i, K key)
      throws IOException, InterruptedException {
    vjoin.clear();
    if (key() != null && 0 == cmp.compare(key, key())) {
      do {
        vjoin.add(value);
      } while (next() && 0 == cmp.compare(key, key()));
    }
    i.add(id, vjoin);
  }

  /**
   * Read the next k,v pair into the head of this object; return true iff
   * the RR and this are exhausted.
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (hasNext()) {
      next();
      return true;
    }
    return false;
  }

  /**
   * Read the next k,v pair into the head of this object; return true iff
   * the RR and this are exhausted.
   */
  private boolean next() throws IOException, InterruptedException {
    empty = !rr.nextKeyValue();
    key = rr.getCurrentKey();
    value = rr.getCurrentValue();
    return !empty;
  }

  /**
   * Get current key 
   */
  public K getCurrentKey() throws IOException, InterruptedException {
    return rr.getCurrentKey();
  }

  /**
   * Get current value
   */
  public U getCurrentValue() throws IOException, InterruptedException {
    return rr.getCurrentValue();
  }

  /**
   * Request progress from proxied RR.
   */
  public float getProgress() throws IOException, InterruptedException {
    return rr.getProgress();
  }

  /**
   * Forward close request to proxied RR.
   */
  public void close() throws IOException {
    rr.close();
  }

  /**
   * Implement Comparable contract (compare key at head of proxied RR
   * with that of another).
   */
  public int compareTo(ComposableRecordReader<K,?> other) {
    return cmp.compare(key(), other.key());
  }

  /**
   * Return true iff compareTo(other) retn true.
   */
  @SuppressWarnings("unchecked") // Explicit type check prior to cast
  public boolean equals(Object other) {
    return other instanceof ComposableRecordReader
        && 0 == compareTo((ComposableRecordReader)other);
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return 42;
  }
}
