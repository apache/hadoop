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
import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * A Writable for arrays containing instances of a class. The elements of this
 * writable must all be instances of the same class. If this writable will be
 * the input for a Reducer, you will need to create a subclass that sets the
 * value to be of the proper type.
 *
 * For example:
 * <code>
 * public class IntArrayWritable extends ArrayWritable {
 *   public IntArrayWritable() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ArrayWritable implements Writable {
  private final Class<? extends Writable> valueClass;
  private Writable[] values;

  public ArrayWritable(Class<? extends Writable> valueClass) {
    if (valueClass == null) { 
      throw new IllegalArgumentException("null valueClass"); 
    }    
    this.valueClass = valueClass;
  }

  public ArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
    this(valueClass);
    this.values = values;
  }

  public ArrayWritable(String[] strings) {
    this(Text.class, new Writable[strings.length]);
    for (int i = 0; i < strings.length; i++) {
      values[i] = new Text(strings[i]);
    }
  }

  public Class<? extends Writable> getValueClass() {
    return valueClass;
  }

  public String[] toStrings() {
    String[] strings = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      strings[i] = values[i].toString();
    }
    return strings;
  }

  public Object toArray() {
    return Arrays.copyOf(values, values.length);
  }

  public void set(Writable[] values) {
    this.values = values;
  }

  public Writable[] get() {
    return values;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    values = new Writable[in.readInt()];          // construct values
    for (int i = 0; i < values.length; i++) {
      Writable value = WritableFactories.newInstance(valueClass);
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      values[i].write(out);
    }
  }

  @Override
  public String toString() {
    return "ArrayWritable [valueClass=" + valueClass + ", values="
        + Arrays.toString(values) + "]";
  }

}

