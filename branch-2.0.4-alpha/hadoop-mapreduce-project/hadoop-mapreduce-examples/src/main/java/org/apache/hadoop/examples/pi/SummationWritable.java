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
package org.apache.hadoop.examples.pi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.pi.math.ArithmeticProgression;
import org.apache.hadoop.examples.pi.math.Summation;
import org.apache.hadoop.io.WritableComparable;

/** A Writable class for Summation */
public final class SummationWritable implements WritableComparable<SummationWritable>, Container<Summation> {
  private Summation sigma;

  public SummationWritable() {}
  
  SummationWritable(Summation sigma) {this.sigma = sigma;}

  /** {@inheritDoc} */
  @Override
  public String toString() {return getClass().getSimpleName() + sigma;}

  /** {@inheritDoc} */
  @Override
  public Summation getElement() {return sigma;}

  /** Read sigma from conf */
  public static Summation read(Class<?> clazz, Configuration conf) {
    return Summation.valueOf(conf.get(clazz.getSimpleName() + ".sigma")); 
  }

  /** Write sigma to conf */
  public static void write(Summation sigma, Class<?> clazz, Configuration conf) {
    conf.set(clazz.getSimpleName() + ".sigma", sigma.toString());
  }

  /** Read Summation from DataInput */
  static Summation read(DataInput in) throws IOException {
    final SummationWritable s = new SummationWritable();
    s.readFields(in);
    return s.getElement();
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    final ArithmeticProgression N = ArithmeticProgressionWritable.read(in);
    final ArithmeticProgression E = ArithmeticProgressionWritable.read(in);
    sigma = new Summation(N, E); 

    if (in.readBoolean()) {
      sigma.setValue(in.readDouble());
    }
  }

  /** Write sigma to DataOutput */
  public static void write(Summation sigma, DataOutput out) throws IOException {
    ArithmeticProgressionWritable.write(sigma.N, out);
    ArithmeticProgressionWritable.write(sigma.E, out);

    final Double v = sigma.getValue();
    if (v == null)
      out.writeBoolean(false);
    else {
      out.writeBoolean(true);
      out.writeDouble(v);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    write(sigma, out);
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(SummationWritable that) {
    return this.sigma.compareTo(that.sigma);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    else if (obj != null && obj instanceof SummationWritable) {
      final SummationWritable that = (SummationWritable)obj;
      return this.compareTo(that) == 0;
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** A writable class for ArithmeticProgression */
  private static class ArithmeticProgressionWritable {
    /** Read ArithmeticProgression from DataInput */
    private static ArithmeticProgression read(DataInput in) throws IOException {
      return new ArithmeticProgression(in.readChar(),
          in.readLong(), in.readLong(), in.readLong());
    }

    /** Write ArithmeticProgression to DataOutput */
    private static void write(ArithmeticProgression ap, DataOutput out
        ) throws IOException {
      out.writeChar(ap.symbol);
      out.writeLong(ap.value);
      out.writeLong(ap.delta);
      out.writeLong(ap.limit);
    }
  }
}