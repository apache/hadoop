/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.io.*;
import java.util.*;

/** A Comparator for {@link WritableComparable}s.
 *
 * <p>This base implemenation uses the natural ordering.  To define alternate
 * orderings, override {@link #compare(WritableComparable,WritableComparable)}.
 *
 * <p>One may optimize compare-intensive operations by overriding
 * {@link #compare(byte[],int,int,byte[],int,int)}.  Static utility methods are
 * provided to assist in optimized implementations of this method.
 */
public class WritableComparator implements Comparator {

  private static HashMap comparators = new HashMap(); // registry

  /** Get a comparator for a {@link WritableComparable} implementation. */
  public static synchronized WritableComparator get(Class c) {
    WritableComparator comparator = (WritableComparator)comparators.get(c);
    if (comparator == null)
      comparator = new WritableComparator(c);
    return comparator;
  }

  /** Register an optimized comparator for a {@link WritableComparable}
   * implementation. */
  public static synchronized void define(Class c,
                                         WritableComparator comparator) {
    comparators.put(c, comparator);
  }


  private DataInputBuffer buffer = new DataInputBuffer();

  private Class keyClass;
  private WritableComparable key1;
  private WritableComparable key2;

  /** Construct for a {@link WritableComparable} implementation. */
  protected WritableComparator(Class keyClass) {
    this.keyClass = keyClass;
    this.key1 = newKey();
    this.key2 = newKey();
  }

  /** Returns the WritableComparable implementation class. */
  public Class getKeyClass() { return keyClass; }

  /** Construct a new {@link WritableComparable} instance. */
  public WritableComparable newKey() {
    try {
      return (WritableComparable)keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  /** Optimization hook.  Override this to make SequenceFile.Sorter's scream.
   *
   * <p>The default implementation reads the data into two {@link
   * WritableComparable}s (using {@link
   * Writable#readFields(DataInput)}, then calls {@link
   * #compare(WritableComparable,WritableComparable)}.
   */
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      buffer.reset(b1, s1, l1);                   // parse key1
      key1.readFields(buffer);
      
      buffer.reset(b2, s2, l2);                   // parse key2
      key2.readFields(buffer);
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    return compare(key1, key2);                   // compare them
  }

  /** Compare two WritableComparables.
   *
   * <p> The default implementation uses the natural ordering, calling {@link
   * Comparable#compareTo(Object)}. */
  public int compare(WritableComparable a, WritableComparable b) {
    return a.compareTo(b);
  }

  public int compare(Object a, Object b) {
    return compare((WritableComparable)a, (WritableComparable)b);
  }

  /** Lexicographic order of binary data. */
  public static int compareBytes(byte[] b1, int s1, int l1,
                                 byte[] b2, int s2, int l2) {
    int end1 = s1 + l1;
    int end2 = s2 + l2;
    for (int i = s1, j = s2; i < end1 && j < end2; i++, j++) {
      int a = (b1[i] & 0xff);
      int b = (b2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return l1 - l2;
  }

  /** Compute hash for binary data. */
  public static int hashBytes(byte[] bytes, int length) {
    int hash = 1;
    for (int i = 0; i < length; i++)
      hash = (31 * hash) + (int)bytes[i];
    return hash;
  }

  /** Parse an unsigned short from a byte array. */
  public static int readUnsignedShort(byte[] bytes, int start) {
    return (((bytes[start]   & 0xff) <<  8) +
            ((bytes[start+1] & 0xff)));
  }

  /** Parse an integer from a byte array. */
  public static int readInt(byte[] bytes, int start) {
    return (((bytes[start  ] & 0xff) << 24) +
            ((bytes[start+1] & 0xff) << 16) +
            ((bytes[start+2] & 0xff) <<  8) +
            ((bytes[start+3] & 0xff)));

  }

  /** Parse a float from a byte array. */
  public static float readFloat(byte[] bytes, int start) {
    return Float.intBitsToFloat(readInt(bytes, start));
  }

  /** Parse a long from a byte array. */
  public static long readLong(byte[] bytes, int start) {
    return ((long)(readInt(bytes, start)) << 32) +
      (readInt(bytes, start+4) & 0xFFFFFFFFL);
  }

}
