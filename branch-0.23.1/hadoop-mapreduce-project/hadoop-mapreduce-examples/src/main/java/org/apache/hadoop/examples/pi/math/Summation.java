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
package org.apache.hadoop.examples.pi.math;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.examples.pi.Combinable;
import org.apache.hadoop.examples.pi.Container;
import org.apache.hadoop.examples.pi.Util;

/** Represent the summation \sum \frac{2^e \mod n}{n}. */
public class Summation implements Container<Summation>, Combinable<Summation> {
  /** Variable n in the summation. */
  public final ArithmeticProgression N;
  /** Variable e in the summation. */
  public final ArithmeticProgression E;
  private Double value = null;

  /** Constructor */
  public Summation(ArithmeticProgression N, ArithmeticProgression E) {
    if (N.getSteps() != E.getSteps()) {
      throw new IllegalArgumentException("N.getSteps() != E.getSteps(),"
          + "\n  N.getSteps()=" + N.getSteps() + ", N=" + N
          + "\n  E.getSteps()=" + E.getSteps() + ", E=" + E);
    }
    this.N = N;
    this.E = E;
  }

  /** Constructor */
  Summation(long valueN, long deltaN, 
            long valueE, long deltaE, long limitE) {
    this(valueN, deltaN, valueN - deltaN*((valueE - limitE)/deltaE),
         valueE, deltaE, limitE);
  }

  /** Constructor */
  Summation(long valueN, long deltaN, long limitN, 
            long valueE, long deltaE, long limitE) {
    this(new ArithmeticProgression('n', valueN, deltaN, limitN),
         new ArithmeticProgression('e', valueE, deltaE, limitE));
  }

  /** {@inheritDoc} */
  @Override
  public Summation getElement() {return this;}

  /** Return the number of steps of this summation */
  long getSteps() {return E.getSteps();}
  
  /** Return the value of this summation */
  public Double getValue() {return value;}
  /** Set the value of this summation */
  public void setValue(double v) {this.value = v;}

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "[" + N + "; " + E + (value == null? "]": "]value=" + Double.doubleToLongBits(value)); 
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (obj != null && obj instanceof Summation) {
      final Summation that = (Summation)obj;
      return this.N.equals(that.N) && this.E.equals(that.E);
    }
    throw new IllegalArgumentException(obj == null? "obj == null":
      "obj.getClass()=" + obj.getClass());
  }

  /** Not supported */
  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }

  /** Covert a String to a Summation. */
  public static Summation valueOf(final String s) {
    int i = 1;
    int j = s.indexOf("; ", i);
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
    final ArithmeticProgression N = ArithmeticProgression.valueOf(s.substring(i, j));

    i = j + 2;
    j = s.indexOf("]", i);
    if (j < 0)
      throw new IllegalArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
    final ArithmeticProgression E = ArithmeticProgression.valueOf(s.substring(i, j));

    final Summation sigma = new Summation(N, E);
    i = j + 1;
    if (s.length() > i) {
      final String value = Util.parseStringVariable("value", s.substring(i));
      sigma.setValue(value.indexOf('.') < 0?
          Double.longBitsToDouble(Long.parseLong(value)):
          Double.parseDouble(value));
    }
    return sigma;
  }

  /** Compute the value of the summation. */
  public double compute() {
    if (value == null)
      value = N.limit <= MAX_MODULAR? compute_modular(): compute_montgomery();
    return value;
  }

  private static final long MAX_MODULAR = 1L << 32;
  /** Compute the value using {@link Modular#mod(long, long)}. */
  double compute_modular() {
    long e = E.value;
    long n = N.value;
    double s = 0;
    for(; e > E.limit; e += E.delta) {
      s = Modular.addMod(s, Modular.mod(e, n)/(double)n);
      n += N.delta;
    }
    return s;
  }

  final Montgomery montgomery = new Montgomery();
  /** Compute the value using {@link Montgomery#mod(long)}. */
  double compute_montgomery() {
    long e = E.value;
    long n = N.value;
    double s = 0;
    for(; e > E.limit; e += E.delta) {
      s = Modular.addMod(s, montgomery.set(n).mod(e)/(double)n);
      n += N.delta;
    }
    return s;
  }

  /** {@inheritDoc} */
  @Override
  public int compareTo(Summation that) {
    final int de = this.E.compareTo(that.E);
    if (de != 0) return de;
    return this.N.compareTo(that.N);
  }

  /** {@inheritDoc} */
  @Override
  public Summation combine(Summation that) {
    if (this.N.delta != that.N.delta || this.E.delta != that.E.delta) 
      throw new IllegalArgumentException(
          "this.N.delta != that.N.delta || this.E.delta != that.E.delta"
          + ",\n  this=" + this
          + ",\n  that=" + that);
    if (this.E.limit == that.E.value && this.N.limit == that.N.value) {
      final double v = Modular.addMod(this.value, that.value);
      final Summation s = new Summation(
          new ArithmeticProgression(N.symbol, N.value, N.delta, that.N.limit),
          new ArithmeticProgression(E.symbol, E.value, E.delta, that.E.limit));
      s.setValue(v);
      return s;
    }
    return null;
  }

  /** Find the remaining terms. */
  public <T extends Container<Summation>> List<Summation> remainingTerms(List<T> sorted) {
    final List<Summation> results = new ArrayList<Summation>();
    Summation remaining = this;

    if (sorted != null)
      for(Container<Summation> c : sorted) {
        final Summation sigma = c.getElement();
        if (!remaining.contains(sigma))
          throw new IllegalArgumentException("!remaining.contains(s),"
              + "\n  remaining = " + remaining
              + "\n  s         = " + sigma          
              + "\n  this      = " + this
              + "\n  sorted    = " + sorted);

        final Summation s = new Summation(sigma.N.limit, N.delta, remaining.N.limit,
                                          sigma.E.limit, E.delta, remaining.E.limit);
        if (s.getSteps() > 0)
          results.add(s);
        remaining = new Summation(remaining.N.value, N.delta, sigma.N.value,
                                  remaining.E.value, E.delta, sigma.E.value);
      }

    if (remaining.getSteps() > 0)
      results.add(remaining);
  
    return results;
  }

  /** Does this contains that? */
  public boolean contains(Summation that) {
    return this.N.contains(that.N) && this.E.contains(that.E);    
  }

  /** Partition the summation. */
  public Summation[] partition(final int nParts) {
    final Summation[] parts = new Summation[nParts];
    final long steps = (E.limit - E.value)/E.delta + 1;

    long prevN = N.value;
    long prevE = E.value;

    for(int i = 1; i < parts.length; i++) {
      final long k = (i * steps)/parts.length;

      final long currN = N.skip(k);
      final long currE = E.skip(k);
      parts[i - 1] = new Summation(
          new ArithmeticProgression(N.symbol, prevN, N.delta, currN),
          new ArithmeticProgression(E.symbol, prevE, E.delta, currE));

      prevN = currN;
      prevE = currE;
    }

    parts[parts.length - 1] = new Summation(
        new ArithmeticProgression(N.symbol, prevN, N.delta, N.limit),
        new ArithmeticProgression(E.symbol, prevE, E.delta, E.limit));
    return parts;
  }
}