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

package org.apache.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.AbstractCollection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/** A Writable wrapper for EnumSet. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EnumSetWritable<E extends Enum<E>> extends AbstractCollection<E>
  implements Writable, Configurable  {

  private EnumSet<E> value;

  private transient Class<E> elementType;

  private transient Configuration conf;
  
  EnumSetWritable() {
  }

  @Override
  public Iterator<E> iterator() { return value.iterator(); }
  @Override
  public int size() { return value.size(); }
  @Override
  public boolean add(E e) {
    if (value == null) {
      value = EnumSet.of(e);
      set(value, null);
    }
    return value.add(e);
  }

  /**
   * Construct a new EnumSetWritable. If the <tt>value</tt> argument is null or
   * its size is zero, the <tt>elementType</tt> argument must not be null. If
   * the argument <tt>value</tt>'s size is bigger than zero, the argument
   * <tt>elementType</tt> is not be used.
   * 
   * @param value
   * @param elementType
   */
  public EnumSetWritable(EnumSet<E> value, Class<E> elementType) {
    set(value, elementType);
  }

  /**
   * Construct a new EnumSetWritable. Argument <tt>value</tt> should not be null
   * or empty.
   * 
   * @param value
   */
  public EnumSetWritable(EnumSet<E> value) {
    this(value, null);
  }

  /**
   * reset the EnumSetWritable with specified
   * <tt>value</tt> and <tt>elementType</tt>. If the <tt>value</tt> argument
   * is null or its size is zero, the <tt>elementType</tt> argument must not be
   * null. If the argument <tt>value</tt>'s size is bigger than zero, the
   * argument <tt>elementType</tt> is not be used.
   * 
   * @param value
   * @param elementType
   */
  public void set(EnumSet<E> value, Class<E> elementType) {
    if ((value == null || value.size() == 0)
        && (this.elementType == null && elementType == null)) {
      throw new IllegalArgumentException(
          "The EnumSet argument is null, or is an empty set but with no elementType provided.");
    }
    this.value = value;
    if (value != null && value.size() > 0) {
      Iterator<E> iterator = value.iterator();
      this.elementType = iterator.next().getDeclaringClass();
    } else if (elementType != null) {
      this.elementType = elementType;
    }
  }

  /** Return the value of this EnumSetWritable. */
  public EnumSet<E> get() {
    return value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1)
      this.value = null;
    else if (length == 0) {
      this.elementType = (Class<E>) ObjectWritable.loadClass(conf,
          WritableUtils.readString(in));
      this.value = EnumSet.noneOf(this.elementType);
    } else {
      E first = (E) ObjectWritable.readObject(in, conf);
      this.value = (EnumSet<E>) EnumSet.of(first);
      for (int i = 1; i < length; i++)
        this.value.add((E) ObjectWritable.readObject(in, conf));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (this.value == null) {
      out.writeInt(-1);
      WritableUtils.writeString(out, this.elementType.getName());
    } else {
      Object[] array = this.value.toArray();
      int length = array.length;
      out.writeInt(length);
      if (length == 0) {
        if (this.elementType == null)
          throw new UnsupportedOperationException(
              "Unable to serialize empty EnumSet with no element type provided.");
        WritableUtils.writeString(out, this.elementType.getName());
      }
      for (int i = 0; i < length; i++) {
        ObjectWritable.writeObject(out, array[i], array[i].getClass(), conf);
      }
    }
  }

  /**
   * Returns true if <code>o</code> is an EnumSetWritable with the same value,
   * or both are null.
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      throw new IllegalArgumentException("null argument passed in equal().");
    }

    if (!(o instanceof EnumSetWritable))
      return false;

    EnumSetWritable<?> other = (EnumSetWritable<?>) o;

    if (this == o || (this.value == other.value))
      return true;
    if (this.value == null) // other.value must not be null if we reach here
      return false;

    return this.value.equals(other.value);
  }

  /**
   * Returns the class of all the elements of the underlying EnumSetWriable. It
   * may return null.
   * 
   * @return the element class
   */
  public Class<E> getElementType() {
    return elementType;
  }

  @Override
  public int hashCode() {
    if (value == null)
      return 0;
    return (int) value.hashCode();
  }

  @Override
  public String toString() {
    if (value == null)
      return "(null)";
    return value.toString();
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  static {
    WritableFactories.setFactory(EnumSetWritable.class, new WritableFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public Writable newInstance() {
        return new EnumSetWritable();
      }
    });
  }
}
