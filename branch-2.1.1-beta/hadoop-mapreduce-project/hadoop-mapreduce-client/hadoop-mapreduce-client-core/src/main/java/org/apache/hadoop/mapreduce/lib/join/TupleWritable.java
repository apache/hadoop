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

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Writable type storing multiple {@link org.apache.hadoop.io.Writable}s.
 *
 * This is *not* a general-purpose tuple type. In almost all cases, users are
 * encouraged to implement their own serializable types, which can perform
 * better validation and provide more efficient encodings than this class is
 * capable. TupleWritable relies on the join framework for type safety and
 * assumes its instances will rarely be persisted, assumptions not only
 * incompatible with, but contrary to the general case.
 *
 * @see org.apache.hadoop.io.Writable
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TupleWritable implements Writable, Iterable<Writable> {

  protected BitSet written;
  private Writable[] values;

  /**
   * Create an empty tuple with no allocated storage for writables.
   */
  public TupleWritable() {
    written = new BitSet(0);
  }

  /**
   * Initialize tuple with storage; unknown whether any of them contain
   * &quot;written&quot; values.
   */
  public TupleWritable(Writable[] vals) {
    written = new BitSet(vals.length);
    values = vals;
  }

  /**
   * Return true if tuple has an element at the position provided.
   */
  public boolean has(int i) {
    return written.get(i);
  }

  /**
   * Get ith Writable from Tuple.
   */
  public Writable get(int i) {
    return values[i];
  }

  /**
   * The number of children in this Tuple.
   */
  public int size() {
    return values.length;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object other) {
    if (other instanceof TupleWritable) {
      TupleWritable that = (TupleWritable)other;
      if (!this.written.equals(that.written)) {
        return false;
      }
      for (int i = 0; i < values.length; ++i) {
        if (!has(i)) continue;
        if (!values[i].equals(that.get(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  public int hashCode() {
    assert false : "hashCode not designed";
    return written.hashCode();
  }

  /**
   * Return an iterator over the elements in this tuple.
   * Note that this doesn't flatten the tuple; one may receive tuples
   * from this iterator.
   */
  public Iterator<Writable> iterator() {
    final TupleWritable t = this;
    return new Iterator<Writable>() {
      int bitIndex = written.nextSetBit(0);
      public boolean hasNext() {
        return bitIndex >= 0;
      }
      public Writable next() {
        int returnIndex = bitIndex;
        if (returnIndex < 0)
          throw new NoSuchElementException();
        bitIndex = written.nextSetBit(bitIndex+1);
        return t.get(returnIndex);
      }
      public void remove() {
        if (!written.get(bitIndex)) {
          throw new IllegalStateException(
            "Attempt to remove non-existent val");
        }
        written.clear(bitIndex);
      }
    };
  }

  /**
   * Convert Tuple to String as in the following.
   * <tt>[<child1>,<child2>,...,<childn>]</tt>
   */
  public String toString() {
    StringBuffer buf = new StringBuffer("[");
    for (int i = 0; i < values.length; ++i) {
      buf.append(has(i) ? values[i].toString() : "");
      buf.append(",");
    }
    if (values.length != 0)
      buf.setCharAt(buf.length() - 1, ']');
    else
      buf.append(']');
    return buf.toString();
  }

  // Writable

  /** Writes each Writable to <code>out</code>.
   * TupleWritable format:
   * {@code
   *  <count><type1><type2>...<typen><obj1><obj2>...<objn>
   * }
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, values.length);
    writeBitSet(out, values.length, written);
    for (int i = 0; i < values.length; ++i) {
      Text.writeString(out, values[i].getClass().getName());
    }
    for (int i = 0; i < values.length; ++i) {
      if (has(i)) {
        values[i].write(out);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked") // No static typeinfo on Tuples
  public void readFields(DataInput in) throws IOException {
    int card = WritableUtils.readVInt(in);
    values = new Writable[card];
    readBitSet(in, card, written);
    Class<? extends Writable>[] cls = new Class[card];
    try {
      for (int i = 0; i < card; ++i) {
        cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
      }
      for (int i = 0; i < card; ++i) {
        if (cls[i].equals(NullWritable.class)) {
          values[i] = NullWritable.get();
        } else {
          values[i] = cls[i].newInstance();
        }
        if (has(i)) {
          values[i].readFields(in);
        }
      }
    } catch (ClassNotFoundException e) {
      throw new IOException("Failed tuple init", e);
    } catch (IllegalAccessException e) {
      throw new IOException("Failed tuple init", e);
    } catch (InstantiationException e) {
      throw new IOException("Failed tuple init", e);
    }
  }

  /**
   * Record that the tuple contains an element at the position provided.
   */
  void setWritten(int i) {
    written.set(i);
  }

  /**
   * Record that the tuple does not contain an element at the position
   * provided.
   */
  void clearWritten(int i) {
    written.clear(i);
  }

  /**
   * Clear any record of which writables have been written to, without
   * releasing storage.
   */
  void clearWritten() {
    written.clear();
  }

  /**
   * Writes the bit set to the stream. The first 64 bit-positions of the bit
   * set are written as a VLong for backwards-compatibility with older 
   * versions of TupleWritable. All bit-positions >= 64 are encoded as a byte
   * for every 8 bit-positions.
   */
  private static final void writeBitSet(DataOutput stream, int nbits,
      BitSet bitSet) throws IOException {
    long bits = 0L;
        
    int bitSetIndex = bitSet.nextSetBit(0);
    for (;bitSetIndex >= 0 && bitSetIndex < Long.SIZE;
            bitSetIndex=bitSet.nextSetBit(bitSetIndex+1)) {
      bits |= 1L << bitSetIndex;
    }
    WritableUtils.writeVLong(stream,bits);
    
    if (nbits > Long.SIZE) {
      bits = 0L;
      for (int lastWordWritten = 0; bitSetIndex >= 0 && bitSetIndex < nbits; 
              bitSetIndex = bitSet.nextSetBit(bitSetIndex+1)) {
        int bitsIndex = bitSetIndex % Byte.SIZE;
        int word = (bitSetIndex-Long.SIZE) / Byte.SIZE;
        if (word > lastWordWritten) {
          stream.writeByte((byte)bits);
          bits = 0L;
          for (lastWordWritten++;lastWordWritten<word;lastWordWritten++) {
            stream.writeByte((byte)bits);
          }
        }
        bits |= 1L << bitsIndex;
      }
      stream.writeByte((byte)bits);
    }
  }

  /**
   * Reads a bitset from the stream that has been written with
   * {@link #writeBitSet(DataOutput, int, BitSet)}.
   */
  private static final void readBitSet(DataInput stream, int nbits, 
      BitSet bitSet) throws IOException {
    bitSet.clear();
    long initialBits = WritableUtils.readVLong(stream);
    long last = 0L;
    while (0L != initialBits) {
      last = Long.lowestOneBit(initialBits);
      initialBits ^= last;
      bitSet.set(Long.numberOfTrailingZeros(last));
    }
    
    for (int offset=Long.SIZE; offset < nbits; offset+=Byte.SIZE) {
      byte bits = stream.readByte();
      while (0 != bits) {
        last = Long.lowestOneBit(bits);
        bits ^= last;
        bitSet.set(Long.numberOfTrailingZeros(last) + offset);
      }
    }
  }
}
